#!/usr/bin/env bash
# Interactive SQL over the GCS data through the Hive metastore (Spark SQL CLI).
#
#   ./scripts/sql_shell.sh                                   # interactive prompt: spark-sql>
#   ./scripts/sql_shell.sh -e "select * from mart.store_weekly_performance limit 5"
#   ./scripts/sql_shell.sh -f my_queries.sql
#
# Uses exactly the same settings as the PySpark jobs (common/config.py), so it sees the
# `datalake` and `mart` databases and reads the Parquet files from the gs:// buckets.
set -euo pipefail
cd "$(dirname "$0")/.."
unset VIRTUAL_ENV
CONF_ARGS=()
while IFS= read -r line; do CONF_ARGS+=(--conf "$line"); done < <(uv run python -c "
from common.config import spark_conf
for k, v in spark_conf().items(): print(f'{k}={v}')")
export SPARK_HOME="$PWD/.venv/lib/python3.12/site-packages/pyspark"
exec "$SPARK_HOME/bin/spark-sql" --master local[4] "${CONF_ARGS[@]}" \
  --conf spark.sql.cli.print.header=true --hiveconf hive.cli.print.header=true "$@" 2> >(grep -vE "WARN|NativeCodeLoader|log4j|incubator|setLogLevel|Spark Web UI|Spark master|Time taken" >&2)
