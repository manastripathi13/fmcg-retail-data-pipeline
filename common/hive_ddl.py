"""Hive databases and external tables for the two layers.

  datalake  (gs://<prefix>-datalake)  raw data as Parquet, one table per source file,
                                      partitioned by year / month / day of the drop
  mart      (gs://<prefix>-mart)      business-ready tables, partitioned by ISO year / ISO week

External tables: Hive stores only the definitions (in the metastore); the data files live in
GCS and survive a DROP TABLE.
"""

from __future__ import annotations

from common.config import DATALAKE_BUCKET, DATALAKE_DB, MART_BUCKET, MART_DB

LOAD_META = "_source_file string, _ingested_at timestamp"
DAY_PART = "partitioned by (year int, month int, day int)"
WEEK_PART = "partitioned by (year int, week int)"

DATALAKE_TABLES = {
    "transactions": f"""
        transaction_id string, line_no int, transaction_ts timestamp, store_id string, customer_id string,
        sku string, product_name string, brand string, category string, sub_category string, pack_size string,
        quantity int, unit_price decimal(12,2), discount_amount decimal(12,2), promotion_id string,
        payment_mode string, channel string, {LOAD_META}""",
    "customers": f"""
        customer_id string, customer_name string, gender string, age_band string, city string, state string,
        home_store_id string, loyalty_tier string, signup_date date, updated_at timestamp, {LOAD_META}""",
    "stores": f"""
        store_id string, store_name string, store_format string, city string, state string, region string,
        opened_date date, area_sqft int, updated_at timestamp, {LOAD_META}""",
    "promotions": f"""
        promotion_id string, promo_name string, promo_type string, discount_pct int, category string, brand string,
        start_date date, end_date date, updated_at timestamp, {LOAD_META}""",
}

MART_TABLES = {
    "fact_sales": """
        transaction_id string, line_no int, sale_ts timestamp, sale_date date, day_of_week string, is_weekend boolean,
        store_id string, store_name string, store_format string, city string, state string, region string,
        customer_id string, customer_type string, loyalty_tier string, age_band string,
        sku string, product_name string, brand string, category string, sub_category string,
        quantity int, unit_price decimal(12,2), gross_amount decimal(14,2), discount_amount decimal(14,2),
        net_amount decimal(14,2), is_return boolean, promotion_id string, promo_name string, promo_type string,
        payment_mode string, channel string""",
    "store_weekly_performance": """
        store_id string, store_name string, store_format string, city string, region string, area_sqft int,
        days_in_week int, baskets bigint, customers bigint, units bigint, gross_sales decimal(16,2),
        discount decimal(16,2), net_sales decimal(16,2), returns_value decimal(16,2), avg_basket_value decimal(12,2),
        promo_sales_share_pct double, sales_per_sqft decimal(12,2), prev_week_net_sales decimal(16,2),
        wow_growth_pct double, rank_in_region int""",
    "category_brand_weekly": """
        category string, sub_category string, brand string, units bigint, net_sales decimal(16,2),
        share_of_category_pct double, prev_week_net_sales decimal(16,2), wow_growth_pct double""",
    "promotion_weekly_effectiveness": """
        promotion_id string, promo_name string, promo_type string, discount_pct int, category string, brand string,
        promo_days_in_week int, promo_units bigint, promo_net_sales decimal(16,2), discount_cost decimal(16,2),
        baseline_daily_units double, promo_daily_units double, unit_uplift_pct double,
        incremental_net_sales decimal(16,2), roi double""",
    "customer_tier_weekly": """
        customer_type string, loyalty_tier string, active_customers bigint, baskets bigint, units bigint,
        net_sales decimal(16,2), avg_spend_per_customer decimal(12,2), avg_baskets_per_customer double,
        share_of_sales_pct double""",
}


def ensure_hive_objects(spark) -> None:
    spark.sql(f"create database if not exists {DATALAKE_DB} comment 'Raw FMCG data as Parquet, partitioned by drop date' "
              f"location 'gs://{DATALAKE_BUCKET}/'")
    spark.sql(f"create database if not exists {MART_DB} comment 'Business-ready FMCG marts, partitioned by ISO year/week' "
              f"location 'gs://{MART_BUCKET}/'")
    for t, cols in DATALAKE_TABLES.items():
        spark.sql(f"""create external table if not exists {DATALAKE_DB}.{t} ({cols}) {DAY_PART}
                      stored as parquet location 'gs://{DATALAKE_BUCKET}/{t}'
                      tblproperties ('layer'='datalake', 'source'='landing/{t}/*.csv.gz', 'parquet.compression'='SNAPPY')""")
    for t, cols in MART_TABLES.items():
        spark.sql(f"""create external table if not exists {MART_DB}.{t} ({cols}) {WEEK_PART}
                      stored as parquet location 'gs://{MART_BUCKET}/{t}'
                      tblproperties ('layer'='mart', 'parquet.compression'='SNAPPY')""")


def column_types(ddl_cols: str) -> dict[str, str]:
    """{column: type} in DDL order. Splits on commas that are not inside a type like decimal(12,2)."""
    import re

    parts = [c.strip().split() for c in re.split(r",(?![^()]*\))", ddl_cols.replace("\n", " ")) if c.strip()]
    return {p[0]: p[1] for p in parts}


def column_names(ddl_cols: str) -> list[str]:
    return list(column_types(ddl_cols))
