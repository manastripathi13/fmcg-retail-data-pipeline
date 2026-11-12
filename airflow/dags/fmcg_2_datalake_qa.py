"""DAG 2 of 4 - datalake QA.  Triggered by fmcg_1_datalake; triggers fmcg_3_mart only if every check passes.

    check_row_counts ──┐
    check_pk_uniqueness ├─► trigger_mart
    check_null_keys ───┘

check_row_counts      rows in each landing .csv.gz == rows in its datalake partition
check_pk_uniqueness   primary keys unique per daily partition (raw POS re-sends: WARN up to 1%, FAIL above)
check_null_keys       business keys never NULL
Results: Hive table qa.datalake_checks. Rules: config/qa_rules.yaml.
"""

from __future__ import annotations

from datetime import datetime

import sys  # noqa: E402
from pathlib import Path  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))   # shared helpers live next to the DAGs
from _fmcg_common import CONF, DEFAULT_ARGS, PARAMS, spark_job
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG

with DAG(
    dag_id="fmcg_2_datalake_qa",
    description="2/4 FMCG: datalake QA (row counts, PK uniqueness, NULL keys)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,          # one Spark job at a time on a laptop
    params=PARAMS,
    default_args=DEFAULT_ARGS,
    tags=["project-4", "fmcg", "2-datalake-qa", "qa"],
    doc_md=__doc__,
) as dag:
    checks = [BashOperator(task_id=f"check_{c}", bash_command=spark_job("jobs/qa_datalake.py", f"--check {c}"))
              for c in ["row_counts", "pk_uniqueness", "null_keys"]]
    nxt = TriggerDagRunOperator(task_id="trigger_mart", trigger_dag_id="fmcg_3_mart", conf=CONF)   # all_success
    checks >> nxt
