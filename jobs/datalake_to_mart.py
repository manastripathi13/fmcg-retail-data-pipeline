"""PySpark job: datalake -> mart (Hive external tables on GCS, partitioned by ISO year / ISO week).

For every ISO week touching the date range, rebuild each mart table for that week (Monday .. Sunday,
or week-to-date when the range ends mid-week):

  fact_sales                      line-level sales enriched with store, customer and promotion attributes
  store_weekly_performance        per store: net sales, baskets, basket value, sales/sqft, WoW and YoY growth, regional rank
  category_brand_weekly           per category/brand: units, net sales, share of category, WoW and YoY growth
  promotion_weekly_effectiveness  per promotion: units and sales on promo, discount cost, uplift vs baseline, ROI
  customer_tier_weekly            per loyalty tier (plus walk-ins): active customers, spend, frequency, share of sales
  company_weekly_kpis             one row per week: headline KPIs with WoW and YoY change (used by the QA thresholds)

WoW compares the same weekdays of the previous week; YoY compares the same ISO week and weekdays
one year earlier. Totals are reconciled before a week is reported as done; re-running rewrites the weeks.

    python jobs/datalake_to_mart.py --date 2026-11-13
    python jobs/datalake_to_mart.py --start 2023-11-14 --end 2026-11-13
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import DATALAKE_DB, MART_BUCKET, MART_DB, gcs_client, get_spark  # noqa: E402
from common.hive_ddl import MART_TABLES, column_names, ensure_hive_objects  # noqa: E402
from common.hive_io import write_partition  # noqa: E402
from common.run_range import add_range_args, iso_weeks, resolve, same_days_last_year  # noqa: E402

D2 = "decimal(16,2)"


def NET() -> F.Column:
    """Net line value = quantity x unit price - discount (a function: Spark columns need an active session)."""
    return (F.col("quantity") * F.col("unit_price")) - F.col("discount_amount")


def partition_date() -> F.Column:
    return F.make_date("year", "month", "day")


def transactions_between(spark, start: date, end: date) -> DataFrame:
    """Datalake transactions for [start, end], with re-sent POS lines removed (one row per transaction line)."""
    tx = spark.table(f"{DATALAKE_DB}.transactions").where(partition_date().between(F.lit(start), F.lit(end)))
    latest = Window.partitionBy("transaction_id", "line_no").orderBy(F.col("modified_dttm").desc())
    return tx.withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").drop("_rn")


def latest_version(dim: DataFrame, key: str, as_of: date) -> DataFrame:
    """Most recent record per key from all drops up to and including as_of (customers arrive as deltas)."""
    w = Window.partitionBy(key).orderBy(F.col("updated_at").desc(), F.col("_drop").desc())
    return dim.where(F.col("_drop") <= F.lit(as_of)).withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def net_by(df: DataFrame, keys: list[str], name: str) -> DataFrame:
    return df.groupBy(*keys).agg(F.sum(NET()).cast(D2).alias(name))


def pct_change(cur: str, prev: str) -> F.Column:
    return F.round(100 * (F.col(cur) / F.col(prev) - 1), 2)


def build_fact_sales(tx: DataFrame, stores: DataFrame, customers: DataFrame, promos: DataFrame) -> DataFrame:
    s = stores.select("store_id", "store_name", "store_format", "city", "state", "region")
    c = customers.select("customer_id", "loyalty_tier", "age_band")
    p = promos.select("promotion_id", "promo_name", "promo_type")
    gross = (F.col("quantity") * F.col("unit_price")).cast("decimal(14,2)")
    return (tx.join(s, "store_id", "left").join(c, "customer_id", "left").join(p, "promotion_id", "left")
            .select(
                "transaction_id", "line_no",
                F.col("transaction_ts").alias("sale_ts"),
                F.to_date("transaction_ts").alias("sale_date"),
                F.date_format("transaction_ts", "EEEE").alias("day_of_week"),
                F.dayofweek("transaction_ts").isin(1, 7).alias("is_weekend"),
                "store_id", "store_name", "store_format", "city", "state", "region", "customer_id",
                F.when(F.col("customer_id").isNull(), "walk-in").otherwise("loyalty member").alias("customer_type"),
                F.coalesce("loyalty_tier", F.lit("Walk-in")).alias("loyalty_tier"),
                "age_band", "sku", "product_name", "brand", "category", "sub_category", "quantity", "unit_price",
                gross.alias("gross_amount"),
                F.col("discount_amount").cast("decimal(14,2)").alias("discount_amount"),
                (gross - F.col("discount_amount")).cast("decimal(14,2)").alias("net_amount"),
                (F.col("quantity") < 0).alias("is_return"),
                "promotion_id", "promo_name", "promo_type", "payment_mode", "channel"))


def weekly_store(fact, prev, ly, stores) -> DataFrame:
    agg = fact.groupBy("store_id").agg(
        F.countDistinct("sale_date").alias("days_in_week"),
        F.countDistinct("transaction_id").alias("baskets"),
        F.countDistinct("customer_id").alias("customers"),
        F.sum("quantity").alias("units"),
        F.sum("gross_amount").cast(D2).alias("gross_sales"),
        F.sum("discount_amount").cast(D2).alias("discount"),
        F.sum("net_amount").cast(D2).alias("net_sales"),
        F.sum(F.when(F.col("is_return"), -F.col("net_amount")).otherwise(0)).cast(D2).alias("returns_value"),
        F.sum(F.when(F.col("promotion_id").isNotNull(), F.col("net_amount")).otherwise(0)).alias("_promo_sales"))
    st = stores.select("store_id", "store_name", "store_format", "city", "region", "area_sqft")
    return (agg.join(st, "store_id", "left")
            .join(net_by(prev, ["store_id"], "prev_week_net_sales"), "store_id", "left")
            .join(net_by(ly, ["store_id"], "prev_year_net_sales"), "store_id", "left")
            .withColumn("avg_basket_value", (F.col("net_sales") / F.col("baskets")).cast("decimal(12,2)"))
            .withColumn("promo_sales_share_pct", F.round(100 * F.col("_promo_sales") / F.col("net_sales"), 2))
            .withColumn("sales_per_sqft", (F.col("net_sales") / F.col("area_sqft")).cast("decimal(12,2)"))
            .withColumn("wow_growth_pct", pct_change("net_sales", "prev_week_net_sales"))
            .withColumn("yoy_growth_pct", pct_change("net_sales", "prev_year_net_sales"))
            .withColumn("rank_in_region", F.rank().over(Window.partitionBy("region").orderBy(F.col("net_sales").desc()))))


def weekly_category(fact, prev, ly) -> DataFrame:
    keys = ["category", "sub_category", "brand"]
    agg = fact.groupBy(*keys).agg(F.sum("quantity").alias("units"), F.sum("net_amount").cast(D2).alias("net_sales"))
    return (agg.join(net_by(prev, keys, "prev_week_net_sales"), keys, "left")
            .join(net_by(ly, keys, "prev_year_net_sales"), keys, "left")
            .withColumn("share_of_category_pct", F.round(100 * F.col("net_sales") / F.sum("net_sales").over(Window.partitionBy("category")), 2))
            .withColumn("wow_growth_pct", pct_change("net_sales", "prev_week_net_sales"))
            .withColumn("yoy_growth_pct", pct_change("net_sales", "prev_year_net_sales")))


def weekly_promotions(fact, history, promos, week_start: date, as_of: date) -> DataFrame:
    """Uplift = units per day while on promo vs units per day for the same brand/category on non-promo days."""
    live = promos.where((F.col("start_date") <= F.lit(as_of)) & (F.col("end_date") >= F.lit(week_start)))
    on_promo = (fact.where(F.col("promotion_id").isNotNull()).groupBy("promotion_id").agg(
        F.countDistinct("sale_date").alias("promo_days_in_week"),
        F.sum("quantity").alias("promo_units"),
        F.sum("net_amount").cast(D2).alias("promo_net_sales"),
        F.sum("discount_amount").cast(D2).alias("discount_cost"),
        (F.sum("net_amount") / F.sum("quantity")).alias("_avg_net_price")))
    base = (history.where(F.col("promotion_id").isNull())
            .groupBy("brand", "category", F.to_date("transaction_ts").alias("d")).agg(F.sum("quantity").alias("u"))
            .groupBy("brand", "category").agg(F.avg("u").alias("baseline_daily_units")))
    return (live.select("promotion_id", "promo_name", "promo_type", "discount_pct", "category", "brand")
            .join(on_promo, "promotion_id", "inner").join(base, ["brand", "category"], "left")
            .withColumn("promo_daily_units", F.col("promo_units") / F.col("promo_days_in_week"))
            .withColumn("unit_uplift_pct", F.round(100 * (F.col("promo_daily_units") / F.col("baseline_daily_units") - 1), 1))
            .withColumn("incremental_net_sales",
                        ((F.col("promo_units") - F.col("baseline_daily_units") * F.col("promo_days_in_week")) * F.col("_avg_net_price")).cast(D2))
            .withColumn("roi", F.round(F.col("incremental_net_sales") / F.col("discount_cost"), 2)))


def weekly_tiers(fact) -> DataFrame:
    agg = fact.groupBy("customer_type", "loyalty_tier").agg(
        F.countDistinct("customer_id").alias("active_customers"), F.countDistinct("transaction_id").alias("baskets"),
        F.sum("quantity").alias("units"), F.sum("net_amount").cast(D2).alias("net_sales"))
    return (agg
            .withColumn("avg_spend_per_customer", F.when(F.col("active_customers") > 0, F.col("net_sales") / F.col("active_customers")).cast("decimal(12,2)"))
            .withColumn("avg_baskets_per_customer", F.when(F.col("active_customers") > 0, F.round(F.col("baskets") / F.col("active_customers"), 2)))
            .withColumn("share_of_sales_pct", F.round(100 * F.col("net_sales") / F.sum("net_sales").over(Window.partitionBy()), 2)))


def weekly_company(fact, prev, ly, week_start: date, last_day: date) -> DataFrame:
    agg = fact.agg(
        F.countDistinct("sale_date").alias("days_in_week"), F.countDistinct("store_id").alias("stores_trading"),
        F.countDistinct("transaction_id").alias("baskets"), F.countDistinct("customer_id").alias("customers"),
        F.sum("quantity").alias("units"), F.sum("gross_amount").cast(D2).alias("gross_sales"),
        F.sum("discount_amount").cast(D2).alias("discount"), F.sum("net_amount").cast(D2).alias("net_sales"),
        F.round(100 * F.sum(F.when(F.col("channel") == "online", F.col("net_amount")).otherwise(0)) / F.sum("net_amount"), 2).alias("online_share_pct"))
    return (agg.crossJoin(prev.agg(F.sum(NET()).cast(D2).alias("prev_week_net_sales")))
            .crossJoin(ly.agg(F.sum(NET()).cast(D2).alias("prev_year_net_sales")))
            .withColumn("week_start", F.lit(week_start)).withColumn("week_end", F.lit(last_day))
            .withColumn("avg_basket_value", (F.col("net_sales") / F.col("baskets")).cast("decimal(12,2)"))
            .withColumn("wow_growth_pct", pct_change("net_sales", "prev_week_net_sales"))
            .withColumn("yoy_growth_pct", pct_change("net_sales", "prev_year_net_sales")))


def build_week(spark, dims: dict, iso_year: int, iso_week: int, week_start: date, last_day: date) -> dict:
    n_days = (last_day - week_start).days + 1
    tx = transactions_between(spark, week_start, last_day).cache()
    raw_lines = spark.table(f"{DATALAKE_DB}.transactions").where(partition_date().between(F.lit(week_start), F.lit(last_day))).count()
    prev = transactions_between(spark, week_start - timedelta(days=7), last_day - timedelta(days=7))
    ly_start, ly_end = same_days_last_year(iso_year, iso_week, n_days)
    ly = transactions_between(spark, ly_start, ly_end)
    history = transactions_between(spark, last_day - timedelta(days=28), last_day)
    stores = latest_version(dims["stores"], "store_id", last_day)
    customers = latest_version(dims["customers"], "customer_id", last_day)
    promos = latest_version(dims["promotions"], "promotion_id", last_day)

    fact = build_fact_sales(tx, stores, customers, promos).cache()
    outputs = {
        "fact_sales": fact,
        "store_weekly_performance": weekly_store(fact, prev, ly, stores),
        "category_brand_weekly": weekly_category(fact, prev, ly),
        "promotion_weekly_effectiveness": weekly_promotions(fact, history, promos, week_start, last_day),
        "customer_tier_weekly": weekly_tiers(fact),
        "company_weekly_kpis": weekly_company(fact, prev, ly, week_start, last_day),
    }
    spec, totals, counts = {"year": iso_year, "week": iso_week}, {}, {}
    for table, df in outputs.items():
        df = df.select(*column_names(MART_TABLES[table])).cache()
        counts[table] = df.count()
        col = "net_amount" if table == "fact_sales" else "net_sales"
        if col in df.columns:
            totals[table] = float(df.agg(F.sum(col)).collect()[0][0] or 0)
        write_partition(spark, df, f"{MART_DB}.{table}", spec)
        df.unpersist()
    fact_total = totals["fact_sales"]
    checks = {
        "duplicates_removed": raw_lines - counts["fact_sales"],
        "store_total_matches_fact": abs(totals["store_weekly_performance"] - fact_total) < 0.01,
        "category_total_matches_fact": abs(totals["category_brand_weekly"] - fact_total) < 0.01,
        "tier_total_matches_fact": abs(totals["customer_tier_weekly"] - fact_total) < 0.01,
        "company_total_matches_fact": abs(totals["company_weekly_kpis"] - fact_total) < 0.01,
        "every_line_has_store": fact.where(F.col("store_name").isNull()).count() == 0,
    }
    fact.unpersist()
    tx.unpersist()
    return {"iso_year": iso_year, "iso_week": iso_week, "week_start": str(week_start), "last_day": str(last_day),
            "row_counts": counts, "net_sales": round(fact_total, 2), "checks": checks}


def main() -> int:
    ap = argparse.ArgumentParser()
    add_range_args(ap)
    start, end = resolve(ap.parse_args())
    spark = get_spark(f"datalake_to_mart_{start}_{end}")
    ensure_hive_objects(spark)

    # dimensions are small: load every drop once, pick the right version per week
    dims = {t: spark.table(f"{DATALAKE_DB}.{t}").withColumn("_drop", partition_date()).where(F.col("_drop") <= F.lit(end)).cache()
            for t in ["stores", "customers", "promotions"]}
    weeks, failed = iso_weeks(start, end), []
    for i, (y, w, monday, last_day) in enumerate(weeks, 1):
        m = build_week(spark, dims, y, w, monday, last_day)
        m["written_at"] = datetime.now(timezone.utc).isoformat()
        blob = gcs_client().bucket(MART_BUCKET).blob(f"_manifests/{y}/W{w:02d}/manifest_{last_day:%Y%m%d}.json")
        blob.upload_from_string(json.dumps(m, indent=2), content_type="application/json")
        bad = [k for k, v in m["checks"].items() if v is False]
        failed += [f"{y}-W{w:02d}: {b}" for b in bad]
        print(f"[mart] {i:>3}/{len(weeks)} {y}-W{w:02d} ({monday}..{last_day}) net INR {m['net_sales']:>14,.2f} "
              f"lines {m['row_counts']['fact_sales']:>6} dupes removed {m['checks']['duplicates_removed']:>3} "
              f"{'OK' if not bad else 'RECONCILIATION FAILED ' + str(bad)}")
    spark.stop()
    if failed:
        print("[mart] RECONCILIATION FAILED:", failed)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
