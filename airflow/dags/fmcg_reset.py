"""Airflow DAG (manual): wipe the local emulator buckets and Hive metastore for a fresh replay.

Only for GCS_MODE=emulator. Removes the Docker volumes behind fake-gcs-server and Postgres.
Afterwards trigger fmcg_1_datalake with a start_date / end_date to reload.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

REPO = Path(__file__).resolve().parents[2]

with DAG(dag_id="fmcg_reset", start_date=datetime(2026, 1, 1), schedule=None, catchup=False,
         tags=["project-4", "fmcg", "maintenance"], doc_md=__doc__) as dag:
    BashOperator(task_id="wipe_local_buckets_and_metastore",
                 bash_command=f"cd {REPO} && docker compose down -v && rm -rf docs/insights docs/qa && echo 'emulator buckets and metastore wiped' ")
