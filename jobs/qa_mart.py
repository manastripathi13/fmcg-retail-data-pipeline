"""Mart QA: checks every weekly partition of the mart for a date or range.

  --check pk_uniqueness   primary keys unique within each weekly partition of every mart table
  --check wow_change      |week-on-week % change in net sales| > 20 (config/qa_rules.yaml) is flagged
  --check yoy_change      |year-on-year % change in net sales| > 40 is flagged
  --check all             all of the above

WoW / YoY are evaluated at three levels:
  company   company_weekly_kpis            breach = FAIL (the Airflow task goes red and the week needs review)
  region    store_weekly_performance       breach = WARN (recorded, reported)
  category  category_brand_weekly          breach = WARN
Weeks without a comparison (the first week, or the first year for YoY) are recorded as SKIPPED.

Every evaluation is saved to the Hive table qa.mart_checks (partition run_id). Exit code 1 on any FAIL.

    python jobs/qa_mart.py --start 2023-11-14 --end 2026-11-13 --check yoy_change
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import MART_DB, get_spark  # noqa: E402
from common.hive_ddl import QA_DB, QA_TABLES, column_names, ensure_hive_objects  # noqa: E402
from common.qa import RULES, Results  # noqa: E402
from common.run_range import add_range_args, iso_weeks, resolve  # noqa: E402

R = RULES["mart"]


def weeks_filter(start, end):
    """ISO weeks in the range as a partition filter: year*100+week sorts correctly across years."""
    pairs = [(y, w) for y, w, _, _ in iso_weeks(start, end)]
    lo, hi = pairs[0][0] * 100 + pairs[0][1], pairs[-1][0] * 100 + pairs[-1][1]
    return (F.col("year") * 100 + F.col("week")).between(lo, hi), pairs


def check_pk_uniqueness(spark, start, end, res: Results) -> None:
    cond, pairs = weeks_filter(start, end)
    for t, keys in R["primary_keys"].items():
        df = spark.table(f"{MART_DB}.{t}").where(cond)
        if keys:
            stats = (df.groupBy("year", "week", *keys).count()
                     .groupBy("year", "week").agg(F.sum("count").alias("rows"), F.count("*").alias("distinct_keys")))
        else:  # one row per week expected
            stats = df.groupBy("year", "week").agg(F.count("*").alias("rows"), F.lit(1).cast("long").alias("distinct_keys"))
        registered = {tuple(int(kv.split("=")[1]) for kv in r[0].split("/")) for r in spark.sql(f"show partitions {MART_DB}.{t}").collect()}
        seen = set()
        for r in stats.collect():
            seen.add((r.year, r.week))
            dupes = r.rows - r.distinct_keys
            res.add(check_name="pk_uniqueness", table_name=t, level="table", member=",".join(keys) or "<one row per week>",
                    iso_year=r.year, iso_week=r.week, severity="FAIL", status="PASS" if dupes == 0 else "FAIL",
                    observed=float(dupes), threshold=0.0, detail=f"rows={r.rows} duplicate keys={dupes}")
        for y, w in pairs:
            if (y, w) in seen:
                continue
            if (y, w) in registered:   # e.g. no promotion ran that week: the partition exists and is empty
                res.add(check_name="pk_uniqueness", table_name=t, level="table", member=",".join(keys), iso_year=y, iso_week=w,
                        severity="FAIL", status="PASS", observed=0.0, threshold=0.0, detail="partition registered, 0 rows")
            else:
                res.add(check_name="pk_uniqueness", table_name=t, level="table", member=",".join(keys), iso_year=y, iso_week=w,
                        severity="FAIL", status="FAIL", observed=None, threshold=0.0, detail="partition missing in Hive")


def _change_check(spark, start, end, res: Results, name: str, cur_prev_pct: tuple[str, str, str]) -> None:
    cfg = R[name]
    limit = float(cfg["threshold_abs_pct"])
    cur, prev, _ = cur_prev_pct
    cond, _ = weeks_filter(start, end)
    levels = {
        "company": spark.table(f"{MART_DB}.company_weekly_kpis").where(cond)
                   .select("year", "week", F.lit("company").alias("member"), cur, prev),
        "region": spark.table(f"{MART_DB}.store_weekly_performance").where(cond)
                  .groupBy("year", "week", F.col("region").alias("member")).agg(F.sum(cur).alias(cur), F.sum(prev).alias(prev)),
        "category": spark.table(f"{MART_DB}.category_brand_weekly").where(cond)
                    .groupBy("year", "week", F.col("category").alias("member")).agg(F.sum(cur).alias(cur), F.sum(prev).alias(prev)),
    }
    for level, df in levels.items():
        table = {"company": "company_weekly_kpis", "region": "store_weekly_performance", "category": "category_brand_weekly"}[level]
        breach_status = "FAIL" if level in cfg["fail_levels"] else "WARN"
        for r in df.collect():
            c, p = r[cur], r[prev]
            if p is None or float(p) == 0:
                res.add(check_name=name, table_name=table, level=level, member=r.member, iso_year=r.year, iso_week=r.week,
                        severity=breach_status, status="SKIPPED", observed=None, threshold=limit,
                        detail="no comparison period in the data")
                continue
            pct = 100.0 * (float(c) / float(p) - 1)
            res.add(check_name=name, table_name=table, level=level, member=r.member, iso_year=r.year, iso_week=r.week,
                    severity=breach_status, status=breach_status if abs(pct) > limit else "PASS",
                    observed=round(pct, 2), threshold=limit,
                    detail=f"{cur}={float(c):,.0f} {prev}={float(p):,.0f} change={pct:+.1f}% (limit ±{limit:.0f}%)")


def check_wow(spark, start, end, res):
    _change_check(spark, start, end, res, "wow_change", ("net_sales", "prev_week_net_sales", "wow_growth_pct"))


def check_yoy(spark, start, end, res):
    _change_check(spark, start, end, res, "yoy_change", ("net_sales", "prev_year_net_sales", "yoy_growth_pct"))


CHECKS = {"pk_uniqueness": check_pk_uniqueness, "wow_change": check_wow, "yoy_change": check_yoy}


def main() -> int:
    ap = argparse.ArgumentParser()
    add_range_args(ap)
    ap.add_argument("--check", choices=[*CHECKS, "all"], default="all")
    a = ap.parse_args()
    start, end = resolve(a)
    spark = get_spark(f"qa_mart_{a.check}_{start}_{end}")
    ensure_hive_objects(spark)
    res = Results()
    for name, fn in CHECKS.items():
        if a.check in (name, "all"):
            fn(spark, start, end, res)
    run_id = f"{start}_{end}__{a.check}"
    res.save(spark, f"{QA_DB}.mart_checks", column_names(QA_TABLES["mart_checks"]), run_id)

    print(f"[qa-mart] {a.check} {start}..{end}: {res.counts()}  (saved to {QA_DB}.mart_checks run_id={run_id})")
    for r in sorted(res.failures(), key=lambda r: (r["iso_year"], r["iso_week"]))[:40]:
        print(f"  FAIL  {r['iso_year']}-W{r['iso_week']:02d} {r['check_name']} {r['level']}:{r['member']}  {r['detail']}")
    if len(res.failures()) > 40:
        print(f"  ... {len(res.failures()) - 40} more failures (query {QA_DB}.mart_checks)")
    print(f"  WARN  {len(res.warnings())} region/category breaches recorded in {QA_DB}.mart_checks")
    spark.stop()
    return 1 if res.failures() else 0


if __name__ == "__main__":
    sys.exit(main())
