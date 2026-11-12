"""Datalake QA: checks every daily partition of the datalake for a date or range.

  --check row_counts      rows in each landing .csv.gz == rows in the matching datalake partition
  --check pk_uniqueness   primary keys unique within each daily partition (config/qa_rules.yaml)
  --check null_keys       business keys never NULL
  --check all             all of the above

Every evaluation is saved to the Hive table qa.datalake_checks (partition run_id). Exit code 1
when any check FAILs, which fails the Airflow task and stops the mart from being built.

    python jobs/qa_datalake.py --start 2023-11-14 --end 2026-11-13 --check pk_uniqueness
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import DATALAKE_DB, TABLES, get_spark  # noqa: E402
from common.hive_ddl import QA_DB, QA_TABLES, column_names, ensure_hive_objects  # noqa: E402
from common.qa import RULES, Results  # noqa: E402
from common.run_range import add_range_args, resolve  # noqa: E402
from jobs.raw_to_datalake import landing_paths  # noqa: E402

R = RULES["datalake"]


def partition_date():
    return F.make_date("year", "month", "day")


def datalake(spark, table, start, end):
    return spark.table(f"{DATALAKE_DB}.{table}").where(partition_date().between(F.lit(start), F.lit(end)))


def check_row_counts(spark, start, end, res: Results) -> None:
    for t in TABLES:
        landing = (spark.read.option("header", True).csv(landing_paths(t, start, end))
                   .withColumn("d", F.to_date(F.regexp_extract(F.input_file_name(), r"_(\d{8})\.csv\.gz$", 1), "yyyyMMdd"))
                   .groupBy("d").count().withColumnRenamed("count", "landing"))
        lake = datalake(spark, t, start, end).groupBy(partition_date().alias("d")).count().withColumnRenamed("count", "datalake")
        for r in landing.join(lake, "d", "full").collect():
            ok = (r.landing or 0) == (r.datalake or 0)
            res.add(check_name="row_counts", table_name=t, check_date=r.d, severity="FAIL", status="PASS" if ok else "FAIL",
                    observed=float(r.datalake or 0), threshold=float(r.landing or 0),
                    detail=f"landing={r.landing} datalake={r.datalake}")


def check_pk_uniqueness(spark, start, end, res: Results) -> None:
    for t, keys in R["primary_keys"].items():
        df = datalake(spark, t, start, end).withColumn("d", partition_date())
        stats = (df.groupBy("d", *keys).count()
                 .groupBy("d").agg(F.sum("count").alias("rows"), F.count("*").alias("distinct_keys")))
        for r in stats.collect():
            dupes = r.rows - r.distinct_keys
            pct = 100.0 * dupes / r.rows if r.rows else 0.0
            if t == "transactions":
                limit = R["transactions_max_duplicate_pct"]
                status = "PASS" if dupes == 0 else ("WARN" if pct <= limit else "FAIL")
            else:
                limit, status = 0.0, "PASS" if dupes == 0 else "FAIL"
            res.add(check_name="pk_uniqueness", table_name=t, check_date=r.d, severity="FAIL", status=status,
                    observed=round(pct, 3), threshold=limit,
                    detail=f"key=({', '.join(keys)}) rows={r.rows} duplicates={dupes} ({pct:.2f}%)")


def check_null_keys(spark, start, end, res: Results) -> None:
    for t, keys in R["not_null_keys"].items():
        df = datalake(spark, t, start, end).withColumn("d", partition_date())
        agg = df.groupBy("d").agg(*[F.sum(F.col(k).isNull().cast("int")).alias(k) for k in keys])
        for r in agg.collect():
            nulls = {k: r[k] for k in keys if r[k]}
            res.add(check_name="null_keys", table_name=t, check_date=r.d, severity="FAIL",
                    status="FAIL" if nulls else "PASS", observed=float(sum(nulls.values())), threshold=0.0,
                    detail=f"NULLs: {nulls}" if nulls else f"no NULLs in {keys}")


CHECKS = {"row_counts": check_row_counts, "pk_uniqueness": check_pk_uniqueness, "null_keys": check_null_keys}


def main() -> int:
    ap = argparse.ArgumentParser()
    add_range_args(ap)
    ap.add_argument("--check", choices=[*CHECKS, "all"], default="all")
    a = ap.parse_args()
    start, end = resolve(a)
    spark = get_spark(f"qa_datalake_{a.check}_{start}_{end}")
    ensure_hive_objects(spark)
    res = Results()
    for name, fn in CHECKS.items():
        if a.check in (name, "all"):
            fn(spark, start, end, res)
    run_id = f"{start}_{end}__{a.check}"
    res.save(spark, f"{QA_DB}.datalake_checks", column_names(QA_TABLES["datalake_checks"]), run_id)

    print(f"[qa-datalake] {a.check} {start}..{end}: {res.counts()}  (saved to {QA_DB}.datalake_checks run_id={run_id})")
    for r in res.warnings()[:5]:
        print(f"  WARN  {r['check_name']} {r['table_name']} {r['check_date']}: {r['detail']}")
    if len(res.warnings()) > 5:
        print(f"  ... {len(res.warnings()) - 5} more warnings")
    for r in res.failures()[:20]:
        print(f"  FAIL  {r['check_name']} {r['table_name']} {r['check_date']}: {r['detail']}")
    spark.stop()
    return 1 if res.failures() else 0


if __name__ == "__main__":
    sys.exit(main())
