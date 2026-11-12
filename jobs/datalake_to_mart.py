"""PySpark job: datalake -> mart (Hive external tables on GCS, partitioned by ISO year / ISO week).

For the ISO week that contains --date, rebuild every mart table week-to-date (Monday .. --date):

  fact_sales                      line-level sales enriched with store, customer and promotion attributes
  store_weekly_performance        per store: net sales, baskets, basket value, sales/sqft, WoW growth, regional rank
  category_brand_weekly           per category/brand: units, net sales, share of category, WoW growth
  promotion_weekly_effectiveness  per promotion: units and sales on promo, discount cost, uplift vs baseline, ROI
  customer_tier_weekly            per loyalty tier (plus walk-ins): active customers, spend, frequency, share of sales

Transformations: de-duplicate re-sent POS lines, pick each dimension's latest version as of --date,
compute gross / discount / net amounts, flag returns and weekends, and reconcile totals across tables
before anything is reported as done. Running it again for the same date rewrites the same week.

    python jobs/datalake_to_mart.py --date 2026-11-08
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

D2 = "decimal(16,2)"


def partition_date() -> F.Column:
    return F.make_date("year", "month", "day")


def transactions_between(spark, start: date, end: date) -> DataFrame:
    """Datalake transactions for [start, end], with re-sent POS lines removed (one row per transaction line)."""
    tx = (spark.table(f"{DATALAKE_DB}.transactions")
          .where(partition_date().between(F.lit(start), F.lit(end))))
    latest = Window.partitionBy("transaction_id", "line_no").orderBy(F.col("_ingested_at").desc())
    return tx.withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").drop("_rn")


def latest_version(spark, table: str, key: str, as_of: date) -> DataFrame:
    """Most recent record per key from all drops up to and including as_of (customers arrive as deltas)."""
    df = spark.table(f"{DATALAKE_DB}.{table}").where(partition_date() <= F.lit(as_of))
    w = Window.partitionBy(key).orderBy(F.col("updated_at").desc(), partition_date().desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


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
                "store_id", "store_name", "store_format", "city", "state", "region",
                "customer_id",
                F.when(F.col("customer_id").isNull(), "walk-in").otherwise("loyalty member").alias("customer_type"),
                F.coalesce("loyalty_tier", F.lit("Walk-in")).alias("loyalty_tier"),
                "age_band", "sku", "product_name", "brand", "category", "sub_category", "quantity", "unit_price",
                gross.alias("gross_amount"),
                F.col("discount_amount").cast("decimal(14,2)").alias("discount_amount"),
                (gross - F.col("discount_amount")).cast("decimal(14,2)").alias("net_amount"),
                (F.col("quantity") < 0).alias("is_return"),
                "promotion_id", "promo_name", "promo_type", "payment_mode", "channel"))


def weekly_store(fact: DataFrame, prev: DataFrame, stores: DataFrame) -> DataFrame:
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
    prev_agg = prev.groupBy("store_id").agg(
        F.sum((F.col("quantity") * F.col("unit_price")) - F.col("discount_amount")).cast(D2).alias("prev_week_net_sales"))
    st = stores.select("store_id", "store_name", "store_format", "city", "region", "area_sqft")
    out = (agg.join(st, "store_id", "left").join(prev_agg, "store_id", "left")
           .withColumn("avg_basket_value", (F.col("net_sales") / F.col("baskets")).cast("decimal(12,2)"))
           .withColumn("promo_sales_share_pct", F.round(100 * F.col("_promo_sales") / F.col("net_sales"), 2))
           .withColumn("sales_per_sqft", (F.col("net_sales") / F.col("area_sqft")).cast("decimal(12,2)"))
           .withColumn("wow_growth_pct", F.round(100 * (F.col("net_sales") / F.col("prev_week_net_sales") - 1), 2))
           .withColumn("rank_in_region", F.rank().over(Window.partitionBy("region").orderBy(F.col("net_sales").desc()))))
    return out


def weekly_category(fact: DataFrame, prev: DataFrame) -> DataFrame:
    keys = ["category", "sub_category", "brand"]
    agg = fact.groupBy(*keys).agg(F.sum("quantity").alias("units"), F.sum("net_amount").cast(D2).alias("net_sales"))
    prev_agg = prev.groupBy(*keys).agg(
        F.sum((F.col("quantity") * F.col("unit_price")) - F.col("discount_amount")).cast(D2).alias("prev_week_net_sales"))
    cat_total = F.sum("net_sales").over(Window.partitionBy("category"))
    return (agg.join(prev_agg, keys, "left")
            .withColumn("share_of_category_pct", F.round(100 * F.col("net_sales") / cat_total, 2))
            .withColumn("wow_growth_pct", F.round(100 * (F.col("net_sales") / F.col("prev_week_net_sales") - 1), 2)))


def weekly_promotions(fact: DataFrame, history: DataFrame, promos: DataFrame, week_start: date, as_of: date) -> DataFrame:
    """Uplift = units per day while on promo vs units per day for the same brand/category on non-promo days."""
    live = promos.where((F.col("start_date") <= F.lit(as_of)) & (F.col("end_date") >= F.lit(week_start)))
    on_promo = (fact.where(F.col("promotion_id").isNotNull())
                .groupBy("promotion_id").agg(
                    F.countDistinct("sale_date").alias("promo_days_in_week"),
                    F.sum("quantity").alias("promo_units"),
                    F.sum("net_amount").cast(D2).alias("promo_net_sales"),
                    F.sum("discount_amount").cast(D2).alias("discount_cost"),
                    (F.sum("net_amount") / F.sum("quantity")).alias("_avg_net_price")))
    base = (history.where(F.col("promotion_id").isNull())
            .groupBy("brand", "category", F.to_date("transaction_ts").alias("d")).agg(F.sum("quantity").alias("u"))
            .groupBy("brand", "category").agg(F.avg("u").alias("baseline_daily_units")))
    return (live.select("promotion_id", "promo_name", "promo_type", "discount_pct", "category", "brand")
            .join(on_promo, "promotion_id", "inner")
            .join(base, ["brand", "category"], "left")
            .withColumn("promo_daily_units", F.col("promo_units") / F.col("promo_days_in_week"))
            .withColumn("unit_uplift_pct", F.round(100 * (F.col("promo_daily_units") / F.col("baseline_daily_units") - 1), 1))
            .withColumn("incremental_net_sales",
                        ((F.col("promo_units") - F.col("baseline_daily_units") * F.col("promo_days_in_week"))
                         * F.col("_avg_net_price")).cast(D2))
            .withColumn("roi", F.round(F.col("incremental_net_sales") / F.col("discount_cost"), 2)))


def weekly_tiers(fact: DataFrame) -> DataFrame:
    agg = fact.groupBy("customer_type", "loyalty_tier").agg(
        F.countDistinct("customer_id").alias("active_customers"),
        F.countDistinct("transaction_id").alias("baskets"),
        F.sum("quantity").alias("units"),
        F.sum("net_amount").cast(D2).alias("net_sales"))
    return (agg
            .withColumn("avg_spend_per_customer", F.when(F.col("active_customers") > 0, F.col("net_sales") / F.col("active_customers")).cast("decimal(12,2)"))
            .withColumn("avg_baskets_per_customer", F.when(F.col("active_customers") > 0, F.round(F.col("baskets") / F.col("active_customers"), 2)))
            .withColumn("share_of_sales_pct", F.round(100 * F.col("net_sales") / F.sum("net_sales").over(Window.partitionBy()), 2)))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, type=date.fromisoformat)
    d = ap.parse_args().date
    iso_year, iso_week, _ = d.isocalendar()
    week_start = d - timedelta(days=d.weekday())               # Monday of this ISO week
    prev_start, prev_end = week_start - timedelta(days=7), week_start - timedelta(days=7) + (d - week_start)

    spark = get_spark(f"datalake_to_mart_{iso_year}_W{iso_week:02d}")
    ensure_hive_objects(spark)

    tx = transactions_between(spark, week_start, d).cache()
    raw_lines = (spark.table(f"{DATALAKE_DB}.transactions")
                 .where(partition_date().between(F.lit(week_start), F.lit(d))).count())
    prev = transactions_between(spark, prev_start, prev_end)            # same days of last week, for like-for-like WoW
    history = transactions_between(spark, d - timedelta(days=28), d)   # baseline for promo uplift
    stores = latest_version(spark, "stores", "store_id", d)
    customers = latest_version(spark, "customers", "customer_id", d)
    promos = latest_version(spark, "promotions", "promotion_id", d)

    fact = build_fact_sales(tx, stores, customers, promos).cache()
    outputs = {
        "fact_sales": fact,
        "store_weekly_performance": weekly_store(fact, prev, stores),
        "category_brand_weekly": weekly_category(fact, prev),
        "promotion_weekly_effectiveness": weekly_promotions(fact, history, promos, week_start, d),
        "customer_tier_weekly": weekly_tiers(fact),
    }
    spec = {"year": iso_year, "week": iso_week}
    totals, counts = {}, {}
    for table, df in outputs.items():
        df = df.select(*column_names(MART_TABLES[table])).cache()
        counts[table] = df.count()
        if "net_sales" in df.columns or table == "fact_sales":
            col = "net_amount" if table == "fact_sales" else "net_sales"
            totals[table] = float(df.agg(F.sum(col)).collect()[0][0] or 0)
        path = write_partition(spark, df, f"{MART_DB}.{table}", spec)
        print(f"[mart] {table:<31} {counts[table]:>6} rows  -> {path}")

    # Reconciliation: every aggregate must add up to the line-level fact table.
    fact_total = totals["fact_sales"]
    checks = {
        "duplicates_removed": raw_lines - counts["fact_sales"],
        "store_total_matches_fact": abs(totals["store_weekly_performance"] - fact_total) < 0.01,
        "category_total_matches_fact": abs(totals["category_brand_weekly"] - fact_total) < 0.01,
        "tier_total_matches_fact": abs(totals["customer_tier_weekly"] - fact_total) < 0.01,
        "every_line_has_store": fact.where(F.col("store_name").isNull()).count() == 0,
    }
    manifest = {"run_date": d.isoformat(), "iso_year": iso_year, "iso_week": iso_week,
                "week_start": week_start.isoformat(), "written_at": datetime.now(timezone.utc).isoformat(),
                "row_counts": counts, "net_sales": round(fact_total, 2), "checks": checks}
    blob = gcs_client().bucket(MART_BUCKET).blob(f"_manifests/{iso_year}/W{iso_week:02d}/manifest_{d:%Y%m%d}.json")
    blob.upload_from_string(json.dumps(manifest, indent=2), content_type="application/json")
    print(f"[mart] week {iso_year}-W{iso_week:02d} ({week_start}..{d}) net sales INR {fact_total:,.2f}; checks {checks}")
    print(f"[mart] manifest gs://{MART_BUCKET}/{blob.name}")
    spark.stop()
    failed = [k for k, v in checks.items() if v is False]
    if failed:
        print(f"[mart] RECONCILIATION FAILED: {failed}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
