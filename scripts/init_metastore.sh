#!/usr/bin/env bash
# Create the Hive metastore schema in Postgres once (what Hive's `schematool -initSchema` does).
# Skips if the schema already exists.
set -euo pipefail
cd "$(dirname "$0")/.."
for i in $(seq 1 30); do docker compose exec -T metastore-db pg_isready -U hive -d metastore >/dev/null 2>&1 && break; sleep 1; done
if docker compose exec -T metastore-db psql -U hive -d metastore -tAc "select 1 from information_schema.tables where table_name='VERSION'" | grep -q 1; then
  echo "metastore schema already present: $(docker compose exec -T metastore-db psql -U hive -d metastore -tAc 'select "SCHEMA_VERSION" from "VERSION"')"
else
  docker compose exec -T metastore-db psql -q -U hive -d metastore -v ON_ERROR_STOP=1 < hive/hive-schema-2.3.0.postgres.sql >/dev/null
  echo "metastore schema created: $(docker compose exec -T metastore-db psql -U hive -d metastore -tAc 'select "SCHEMA_VERSION" from "VERSION"')"
fi
