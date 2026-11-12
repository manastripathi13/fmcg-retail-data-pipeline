#!/usr/bin/env bash
# Idempotent setup used by the first Airflow task: services, jars, metastore schema, buckets.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "${GCS_MODE:-emulator}" = "emulator" ]; then
  docker compose up -d >/dev/null
  until curl -sf http://localhost:4443/storage/v1/b >/dev/null; do sleep 1; done
else
  docker compose up -d metastore-db >/dev/null
fi
./scripts/download_jars.sh >/dev/null
./scripts/init_metastore.sh
uv run python scripts/create_buckets.py
