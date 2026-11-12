"""DAG 1 of 4 - landing -> datalake.  Triggers fmcg_2_datalake_qa.

    ensure_infrastructure ─► land_source_files ─► wait_for_landing_files ─► raw_to_datalake ─► trigger_datalake_qa

ensure_infrastructure   buckets, Hive metastore schema, connector jars (idempotent)
land_source_files       stands in for the retailer's systems dropping 4 .csv.gz files per day into the landing bucket
wait_for_landing_files  sensor: proceeds when every file of the range is in GCS
raw_to_datalake         PySpark: .csv.gz -> typed Parquet in datalake.<table>, partition year/month/day
trigger_datalake_qa     starts fmcg_2_datalake_qa for the same date range

Scheduled daily (processes yesterday). For the 3-year backfill trigger it with
{"start_date": "2023-11-14", "end_date": "2026-11-13"}.
"""

from __future__ import annotations

from datetime import datetime

import sys  # noqa: E402
from pathlib import Path  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))   # shared helpers live next to the DAGs
from _fmcg_common import CONF, DEFAULT_ARGS, PARAMS, RANGE, sh, spark_job
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.bash import BashSensor
from airflow.sdk import DAG

with DAG(
    dag_id="fmcg_1_datalake",
    description="1/4 FMCG: landing .csv.gz -> datalake Parquet (year/month/day)",
    start_date=datetime(2026, 11, 14),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    params=PARAMS,
    default_args=DEFAULT_ARGS,
    tags=["project-4", "fmcg", "1-datalake"],
    doc_md=__doc__,
) as dag:
    infra = BashOperator(task_id="ensure_infrastructure", bash_command=sh("./scripts/ensure_infrastructure.sh"))
    land = BashOperator(task_id="land_source_files", bash_command=sh(f"uv run python generator/generate_fmcg_drop.py {RANGE}"))
    wait = BashSensor(task_id="wait_for_landing_files", bash_command=sh(f"uv run python scripts/check_landing.py {RANGE}"),
                      poke_interval=30, timeout=60 * 60, mode="reschedule")
    load = BashOperator(task_id="raw_to_datalake", bash_command=spark_job("jobs/raw_to_datalake.py"))
    nxt = TriggerDagRunOperator(task_id="trigger_datalake_qa", trigger_dag_id="fmcg_2_datalake_qa", conf=CONF)
    infra >> land >> wait >> load >> nxt
