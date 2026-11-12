"""Airflow DAG: daily FMCG pipeline  landing (.csv.gz) -> datalake (Parquet, y/m/d) -> mart (y/week) -> insights.

    ensure_infrastructure ─► simulate_source_drop ─► wait_for_landing_files ─► raw_to_datalake ─► validate_datalake
        ─► datalake_to_mart ─► validate_mart ─► is_end_of_week ─► weekly_insights

ensure_infrastructure   buckets, Hive metastore schema, connector jars (idempotent)
simulate_source_drop    stands in for the retailer's system dropping 4 gzipped CSVs into the landing bucket
wait_for_landing_files  sensor: proceeds only when all 4 files for the day are in GCS
raw_to_datalake         PySpark: CSV.gz -> typed Parquet in datalake.<table>, partition year/month/day
validate_datalake       landing row counts == datalake row counts, no NULL business keys
datalake_to_mart        PySpark: rebuild the ISO week (week-to-date) in mart.*, partition year/week, with reconciliation
validate_mart           every reconciliation check in the mart manifest passed
is_end_of_week          short-circuit: continue only on Sundays (end of ISO week)
weekly_insights         PySpark: write the weekly business report (docs/insights/ and gs://<mart>/_reports/)

Schedule: @daily from 2026-10-26 to 2026-11-08 with catch-up, so unpausing the DAG replays the
two simulated weeks (ISO weeks 44 and 45, ending on Diwali) one day at a time.
Set GCS_MODE=real (and BUCKET_PREFIX, GCP_PROJECT) in the Airflow environment to use real GCS.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.providers.standard.sensors.bash import BashSensor
from airflow.sdk import DAG, Param

REPO = Path(__file__).resolve().parents[2]
RUN_DATE = "{{ params.run_date or data_interval_start.strftime('%Y-%m-%d') }}"


def sh(cmd: str) -> str:
    # each project has its own uv environment; trailing space stops Airflow treating "*.sh" as a template file
    return f"set -euo pipefail; cd {REPO}; unset VIRTUAL_ENV; {cmd} "


def spark_job(script: str) -> str:
    noise = "WARN|NativeCodeLoader|log4j|setLogLevel|incubator|Ivy|^::|^\\s*$"
    return sh(f"uv run python {script} --date {RUN_DATE} 2>&1 | grep -vE '{noise}'; exit ${{PIPESTATUS[0]}}")


def end_of_week(**ctx) -> bool:
    run_date = ctx["params"].get("run_date") or ctx["data_interval_start"].strftime("%Y-%m-%d")
    is_sunday = datetime.strptime(run_date, "%Y-%m-%d").weekday() == 6
    print(f"{run_date} is {'Sunday: write the weekly report' if is_sunday else 'not Sunday: skip the weekly report'}")
    return is_sunday


with DAG(
    dag_id="fmcg_daily_pipeline",
    description="FMCG: landing .csv.gz -> datalake Parquet (y/m/d) -> mart (year/week) -> weekly insights",
    start_date=datetime(2026, 10, 26),
    end_date=datetime(2026, 11, 8),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    params={"run_date": Param(None, type=["null", "string"], description="YYYY-MM-DD; empty = the run's date")},
    default_args={"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["project-4", "fmcg", "gcs", "hive", "pyspark"],
    doc_md=__doc__,
) as dag:
    infra = BashOperator(task_id="ensure_infrastructure", bash_command=sh("./scripts/ensure_infrastructure.sh"))
    drop = BashOperator(task_id="simulate_source_drop",
                        bash_command=sh(f"uv run python generator/generate_fmcg_drop.py --date {RUN_DATE}"))
    wait = BashSensor(task_id="wait_for_landing_files",
                      bash_command=sh(f"uv run python scripts/check_landing.py --date {RUN_DATE}"),
                      poke_interval=30, timeout=60 * 60, mode="reschedule")
    raw = BashOperator(task_id="raw_to_datalake", bash_command=spark_job("jobs/raw_to_datalake.py"))
    v_dl = BashOperator(task_id="validate_datalake",
                        bash_command=sh(f"uv run python scripts/validate_layer.py --layer datalake --date {RUN_DATE}"))
    mart = BashOperator(task_id="datalake_to_mart", bash_command=spark_job("jobs/datalake_to_mart.py"))
    v_mart = BashOperator(task_id="validate_mart",
                          bash_command=sh(f"uv run python scripts/validate_layer.py --layer mart --date {RUN_DATE}"))
    eow = ShortCircuitOperator(task_id="is_end_of_week", python_callable=end_of_week)
    report = BashOperator(task_id="weekly_insights", bash_command=spark_job("jobs/weekly_insights.py"))

    infra >> drop >> wait >> raw >> v_dl >> mart >> v_mart >> eow >> report
