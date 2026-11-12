"""Single place for bucket names, Hive databases and the Spark session.

GCS_MODE decides where the buckets live:
  emulator (default)  local fake-gcs-server on http://localhost:4443, no GCP account needed
  real                real Google Cloud Storage, using your Application Default Credentials
                      (`gcloud auth application-default login`) and GCP_PROJECT
The pipeline code is identical in both modes; only the connector settings change.
"""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

GCS_MODE = os.getenv("GCS_MODE", "emulator")
GCP_PROJECT = os.getenv("GCP_PROJECT", "fmcg-transactional-pipeline")
EMULATOR_URL = os.getenv("GCS_EMULATOR_URL", "http://localhost:4443")
BUCKET_PREFIX = os.getenv("BUCKET_PREFIX", "fmcg")  # real bucket names are global, so make this unique on GCP

LANDING_BUCKET = f"{BUCKET_PREFIX}-landing"
DATALAKE_BUCKET = f"{BUCKET_PREFIX}-datalake"
MART_BUCKET = f"{BUCKET_PREFIX}-mart"

DATALAKE_DB = "datalake"
MART_DB = "mart"

TABLES = ["transactions", "customers", "stores", "promotions"]

METASTORE_JDBC = os.getenv("METASTORE_JDBC", "jdbc:postgresql://localhost:5433/metastore")
JARS = [ROOT / "jars" / os.getenv("GCS_CONNECTOR_JAR", "gcs-connector-hadoop3-2.2.28-shaded.jar"), ROOT / "jars" / "postgresql-42.7.8.jar"]


def gcs_client():
    """google-cloud-storage client for the emulator or real GCS."""
    from google.cloud import storage

    if GCS_MODE == "emulator":
        os.environ.setdefault("STORAGE_EMULATOR_HOST", EMULATOR_URL)
        from google.auth.credentials import AnonymousCredentials

        return storage.Client(project=GCP_PROJECT, credentials=AnonymousCredentials())
    return storage.Client(project=GCP_PROJECT)


def get_spark(app: str):
    """SparkSession with the GCS connector and a Hive metastore (Postgres-backed)."""
    from pyspark.sql import SparkSession

    missing = [str(j) for j in JARS if not j.exists()]
    if missing:
        raise SystemExit(f"missing jars {missing}: run ./scripts/download_jars.sh")
    jars = ",".join(str(j) for j in JARS)
    b = (
        SparkSession.builder.appName(app)
        .master(os.getenv("SPARK_MASTER", "local[4]"))
        .config("spark.jars", jars)
        .config("spark.driver.extraClassPath", ":".join(str(j) for j in JARS))
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .config("spark.ui.showConsoleProgress", "false")
        # ---- Hive metastore (table definitions live in Postgres, data lives in GCS) ----
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", METASTORE_JDBC)
        .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
        .config("spark.hadoop.javax.jdo.option.ConnectionUserName", os.getenv("METASTORE_USER", "hive"))
        .config("spark.hadoop.javax.jdo.option.ConnectionPassword", os.getenv("METASTORE_PASSWORD", "hive"))
        .config("spark.hadoop.datanucleus.schema.autoCreateAll", "false")  # schema created once by scripts/init_metastore.sh
        .config("spark.hadoop.hive.metastore.schema.verification", "false")
        .config("spark.sql.warehouse.dir", f"gs://{DATALAKE_BUCKET}/_warehouse")
        .config("spark.sql.hive.convertMetastoreParquet", "true")
        # ---- GCS connector ----
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.fs.gs.project.id", GCP_PROJECT)
    )
    if GCS_MODE == "emulator":
        b = (b.config("spark.hadoop.fs.gs.storage.root.url", EMULATOR_URL)
              .config("spark.hadoop.fs.gs.auth.service.account.enable", "false")
              .config("spark.hadoop.fs.gs.auth.null.enable", "true"))
    # real mode: connector 2.2 picks up Application Default Credentials automatically
    spark = b.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
