"""Shared settings for the four FMCG DAGs (not a DAG itself; files starting with _ are skipped by Airflow)."""

from __future__ import annotations

from pathlib import Path

from airflow.sdk import Param

REPO = Path(__file__).resolve().parents[2]

# Every DAG works on a date range. A scheduled run of fmcg_1_datalake processes the previous day;
# a manual trigger can pass start_date / end_date (e.g. the 3-year backfill) and the range is handed
# down the chain: datalake -> datalake QA -> mart -> mart QA.
PARAMS = {
    "start_date": Param(None, type=["null", "string"], description="YYYY-MM-DD; empty = yesterday"),
    "end_date": Param(None, type=["null", "string"], description="YYYY-MM-DD; empty = start_date"),
}
_YESTERDAY = "(data_interval_start - macros.timedelta(days=1)).strftime('%Y-%m-%d')"
START = "{{ params.start_date or " + _YESTERDAY + " }}"
END = "{{ params.end_date or params.start_date or " + _YESTERDAY + " }}"
RANGE = f"--start {START} --end {END}"
CONF = {"start_date": START, "end_date": END}
NOISE = "WARN|NativeCodeLoader|log4j|setLogLevel|incubator|Ivy|^::|^\\s*$"


def sh(cmd: str) -> str:
    # each project has its own uv environment; trailing space stops Airflow treating "*.sh" as a template file
    return f"set -euo pipefail; cd {REPO}; unset VIRTUAL_ENV; {cmd} "


def spark_job(script: str, extra: str = "") -> str:
    return sh(f"uv run python {script} {RANGE} {extra} 2>&1 | grep -vE '{NOISE}'; exit ${{PIPESTATUS[0]}}")


DEFAULT_ARGS = {"owner": "data-engineering", "retries": 0}
