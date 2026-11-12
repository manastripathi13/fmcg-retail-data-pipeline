"""DAG 3 of 4 - datalake -> mart.  Triggered by fmcg_2_datalake_qa; triggers fmcg_4_mart_qa.

    build_mart_weeks ─► latest_complete_week ─► weekly_insights
                    └─► trigger_mart_qa

build_mart_weeks       PySpark: rebuild every ISO week touching the range in mart.* (partition year/week),
                       with WoW (same days last week) and YoY (same ISO week last year), reconciled totals
latest_complete_week   short-circuit: the last Sunday inside the range (skips the report if there is none)
weekly_insights        PySpark: business report for that week (docs/insights/ and gs://<mart>/_reports/)
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import sys  # noqa: E402
from pathlib import Path  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))   # shared helpers live next to the DAGs
from _fmcg_common import CONF, DEFAULT_ARGS, PARAMS, sh, spark_job
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG


def last_sunday_in_range(**ctx) -> str | bool:
    p = ctx["params"]
    fallback = (ctx["data_interval_start"] - timedelta(days=1)).date() if ctx.get("data_interval_start") else None
    start = date.fromisoformat(p["start_date"]) if p.get("start_date") else fallback
    end = date.fromisoformat(p["end_date"]) if p.get("end_date") else start
    sunday = end - timedelta(days=(end.weekday() + 1) % 7)
    if sunday < start:
        print(f"no complete ISO week between {start} and {end}: skip the weekly report")
        return False
    print(f"weekly report for the ISO week ending {sunday}")
    return sunday.isoformat()


with DAG(
    dag_id="fmcg_3_mart",
    description="3/4 FMCG: datalake -> mart (ISO year/week) + weekly insights",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params=PARAMS,
    default_args=DEFAULT_ARGS,
    tags=["project-4", "fmcg", "3-mart"],
    doc_md=__doc__,
) as dag:
    build = BashOperator(task_id="build_mart_weeks", bash_command=spark_job("jobs/datalake_to_mart.py"))
    week = ShortCircuitOperator(task_id="latest_complete_week", python_callable=last_sunday_in_range,
                                ignore_downstream_trigger_rules=False)
    report = BashOperator(task_id="weekly_insights", bash_command=sh(
        "uv run python jobs/weekly_insights.py --date {{ ti.xcom_pull(task_ids='latest_complete_week') }} 2>&1 "
        "| grep -vE 'WARN|NativeCodeLoader|log4j|setLogLevel|incubator|^\\s*$'; exit ${PIPESTATUS[0]}"))
    nxt = TriggerDagRunOperator(task_id="trigger_mart_qa", trigger_dag_id="fmcg_4_mart_qa", conf=CONF)
    build >> week >> report
    build >> nxt
