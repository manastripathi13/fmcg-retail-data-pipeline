"""PySpark job: landing (.csv.gz) -> datalake (Parquet, Hive external tables, year/month/day).

For one business date or a whole range (backfill):
  1. read the gzipped CSV drops of all four tables from the landing bucket
  2. cast to proper types (timestamps, dates, decimals) - no business logic, the data stays "raw"
  3. add load metadata: filename (e.g. transactions_20261108.csv.gz) and modified_dttm (when the row was loaded);
     the partition comes from the date in the file name
  4. overwrite those days' partitions of datalake.<table> and register them in the Hive metastore
  5. write a run manifest with rows per table per day to gs://<datalake>/_manifests/

Re-running a date or range replaces exactly those partitions (idempotent).

    python jobs/raw_to_datalake.py --date 2026-11-13
    python jobs/raw_to_datalake.py --start 2023-11-14 --end 2026-11-13
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import DATALAKE_BUCKET, DATALAKE_DB, LANDING_BUCKET, TABLES, gcs_client, get_spark  # noqa: E402
from common.hive_ddl import DATALAKE_TABLES, column_types, ensure_hive_objects  # noqa: E402
from common.hive_io import write_partitions  # noqa: E402
from common.run_range import add_range_args, days, resolve  # noqa: E402


def landing_paths(table: str, start, end) -> list[str]:
    return [f"gs://{LANDING_BUCKET}/{table}/{d:%Y/%m/%d}/{table}_{d:%Y%m%d}.csv.gz" for d in days(start, end)]


def typed(df, table: str):
    """Cast the CSV strings to the datalake column types declared in the Hive DDL, add the drop-date partition."""
    types = column_types(DATALAKE_TABLES[table])
    cols = []
    for name, typ in types.items():
        if name == "filename":
            cols.append(F.regexp_extract("_file", r"([^/]+)$", 1).alias(name))   # just <table>_YYYYMMDD.csv.gz
        elif name == "modified_dttm":
            cols.append(F.current_timestamp().alias(name))
        else:
            c = F.col(name)
            if typ == "string":
                c = F.when(F.trim(c) == "", None).otherwise(c)   # empty CSV field -> NULL
            cols.append(c.cast(typ).alias(name))
    drop_date = F.to_date(F.regexp_extract("_file", r"_(\d{8})\.csv\.gz$", 1), "yyyyMMdd")
    return df.select(*cols, F.year(drop_date).alias("year"), F.month(drop_date).alias("month"),
                     F.dayofmonth(drop_date).alias("day"))


def main() -> int:
    ap = argparse.ArgumentParser()
    add_range_args(ap)
    start, end = resolve(ap.parse_args())

    spark = get_spark(f"raw_to_datalake_{start}_{end}")
    ensure_hive_objects(spark)
    run_id = f"{start}_{end}"
    manifest = {"run_id": run_id, "start": str(start), "end": str(end),
                "written_at": datetime.now(timezone.utc).isoformat(), "rows": {}}
    for table in TABLES:
        raw = (spark.read.option("header", True).option("inferSchema", False)
               .csv(landing_paths(table, start, end))                      # gzip is read transparently
               .withColumn("_file", F.input_file_name()))
        df = typed(raw, table).cache()
        per_day = {f"{r.year:04d}-{r.month:02d}-{r.day:02d}": r["count"]
                   for r in df.groupBy("year", "month", "day").count().collect()}
        specs = write_partitions(spark, df, f"{DATALAKE_DB}.{table}", ["year", "month", "day"])
        manifest["rows"][table] = per_day
        print(f"[datalake] {table:<13} {sum(per_day.values()):>9,} rows into {len(specs):>4} daily partitions "
              f"({start} .. {end})")
        df.unpersist()

    blob = gcs_client().bucket(DATALAKE_BUCKET).blob(f"_manifests/runs/{run_id}.json")
    blob.upload_from_string(json.dumps(manifest, indent=1), content_type="application/json")
    print(f"[datalake] manifest gs://{DATALAKE_BUCKET}/{blob.name}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
