#!/usr/bin/env bash
# Jars Spark needs on the driver classpath:
#   gcs-connector (shaded)  - lets Spark read/write gs:// paths
#   postgresql              - JDBC driver for the Hive metastore database
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p jars
fetch() { [ -f "jars/$2" ] || curl -sfL -o "jars/$2" "$1"; echo "jars/$2"; }
fetch https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.28/gcs-connector-hadoop3-2.2.28-shaded.jar gcs-connector-hadoop3-2.2.28-shaded.jar
fetch https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar postgresql-42.7.8.jar
