"""Runs automatically when the "FMCG PySpark" Jupyter kernel starts.

Creates `spark`: a SparkSession with the Google Cloud Storage connector and the Hive
metastore, the same configuration the pipeline jobs use. Every notebook can then call
spark.table("mart.fact_sales") or spark.sql("select ...").show(10, 0) straight away.
"""

import os
import sys
from pathlib import Path

REPO = Path(os.environ.get("FMCG_REPO", Path.cwd())).resolve()
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from common.config import DATALAKE_BUCKET, GCS_MODE, LANDING_BUCKET, MART_BUCKET, get_spark  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402,F401  (available in every notebook)

spark = get_spark("jupyter-fmcg")
sc = spark.sparkContext

print(f"Spark {spark.version} ready ({GCS_MODE} GCS) | UI: {sc.uiWebUrl}")
print(f"buckets: gs://{LANDING_BUCKET}  gs://{DATALAKE_BUCKET}  gs://{MART_BUCKET}")
print("databases:", ", ".join(r.namespace for r in spark.sql("show databases").collect()))
print('try: spark.table("mart.store_weekly_performance").show(10, 0)   or   spark.sql("show tables in mart").show(10, 0)')
