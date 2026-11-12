"""DAG 4 of 4 - mart QA.  Triggered by fmcg_3_mart.

    check_pk_uniqueness ──┐
    check_wow_change ─────┼─► publish_qa_report (always runs)
    check_yoy_change ─────┘

check_pk_uniqueness   primary keys unique in every weekly partition of every mart table
check_wow_change      |week-on-week % change in net sales| > 20  -> company level FAIL, region/category WARN
check_yoy_change      |year-on-year % change in net sales| > 40  -> company level FAIL, region/category WARN
publish_qa_report     docs/qa/QA_REPORT_<start>_<end>.md from qa.mart_checks and qa.datalake_checks

A red check means a week needs review: either a business explanation (festival timing, new stores)
or a data fix. Thresholds live in config/qa_rules.yaml.
"""

from __future__ import annotations

from datetime import datetime

import sys  # noqa: E402
from pathlib import Path  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))   # shared helpers live next to the DAGs
from _fmcg_common import DEFAULT_ARGS, PARAMS, RANGE, sh, spark_job
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="fmcg_4_mart_qa",
    description="4/4 FMCG: mart QA (PK uniqueness, WoW ±20%, YoY ±40%)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    params=PARAMS,
    default_args=DEFAULT_ARGS,
    tags=["project-4", "fmcg", "4-mart-qa", "qa"],
    doc_md=__doc__,
) as dag:
    checks = [BashOperator(task_id=f"check_{c}", bash_command=spark_job("jobs/qa_mart.py", f"--check {c}"))
              for c in ["pk_uniqueness", "wow_change", "yoy_change"]]
    report = BashOperator(task_id="publish_qa_report", trigger_rule="all_done", bash_command=sh(
        f"uv run python scripts/qa_report.py {RANGE} 2>&1 | grep -vE 'WARN|NativeCodeLoader|log4j|setLogLevel|incubator'; exit ${{PIPESTATUS[0]}}"))
    checks >> report
    report.as_teardown()   # always runs, but the run's colour comes from the checks
