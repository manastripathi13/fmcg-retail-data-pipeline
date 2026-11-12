"""PySpark job: landing (.csv.gz) -> datalake (Parquet, Hive external tables, year/month/day).

For one business date:
  1. read the four gzipped CSV drops from the landing bucket with an explicit schema
  2. cast to proper types (timestamps, dates, decimals) - no business logic, the data stays "raw"
  3. add load metadata (_source_file, _ingested_at)
  4. overwrite that day's partition of datalake.<table> and register it in the Hive metastore
  5. write a manifest (row counts, null keys) to gs://<datalake>/_manifests/YYYY/MM/DD/manifest.json

Re-running a date replaces exactly that date's partitions (idempotent).

    python jobs/raw_to_datalake.py --date 2026-10-26
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import DATALAKE_BUCKET, DATALAKE_DB, LANDING_BUCKET, TABLES, gcs_client, get_spark  # noqa: E402
from common.hive_ddl import DATALAKE_TABLES, column_types, ensure_hive_objects  # noqa: E402
from common.hive_io import write_partition  # noqa: E402

KEYS = {"transactions": ["transaction_id", "store_id", "sku"], "customers": ["customer_id"],
        "stores": ["store_id"], "promotions": ["promotion_id"]}


def landing_path(table: str, d: date) -> str:
    return f"gs://{LANDING_BUCKET}/{table}/{d:%Y/%m/%d}/{table}_{d:%Y%m%d}.csv.gz"


def typed(df, table: str):
    """Cast the CSV strings to the datalake column types declared in the Hive DDL."""
    types = column_types(DATALAKE_TABLES[table])
    cols = []
    for name in types:
        if name == "_source_file":
            cols.append(F.input_file_name().alias(name))
        elif name == "_ingested_at":
            cols.append(F.current_timestamp().alias(name))
        else:
            c = F.col(name)
            if types[name] in ("string",):
                c = F.when(F.trim(c) == "", None).otherwise(c)   # empty CSV field -> NULL
            cols.append(c.cast(types[name]).alias(name))
    return df.select(*cols)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, type=date.fromisoformat)
    d = ap.parse_args().date

    spark = get_spark(f"raw_to_datalake_{d}")
    ensure_hive_objects(spark)
    manifest = {"run_date": d.isoformat(), "written_at": datetime.now(timezone.utc).isoformat(), "tables": {}}
    for table in TABLES:
        src = landing_path(table, d)
        raw = spark.read.option("header", True).option("inferSchema", False).csv(src)   # gzip is read transparently
        df = typed(raw, table).cache()
        null_keys = {k: df.where(F.col(k).isNull()).count() for k in KEYS[table]}
        rows = df.count()
        path = write_partition(spark, df, f"{DATALAKE_DB}.{table}", {"year": d.year, "month": d.month, "day": d.day})
        manifest["tables"][table] = {"source": src, "target": path, "rows": rows, "null_keys": null_keys}
        print(f"[datalake] {table:<13} {rows:>6} rows  {src}  ->  {path}")
        df.unpersist()

    blob = gcs_client().bucket(DATALAKE_BUCKET).blob(f"_manifests/{d:%Y/%m/%d}/manifest.json")
    blob.upload_from_string(json.dumps(manifest, indent=2), content_type="application/json")
    print(f"[datalake] manifest gs://{DATALAKE_BUCKET}/{blob.name}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
