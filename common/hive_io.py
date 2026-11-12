"""Write partitions of Hive external tables on GCS, idempotently.

Pattern: delete exactly the partition folders being (re)written, write the new Parquet files,
then register those partitions in the Hive metastore. Re-running the same day / week / range
replaces those partitions and nothing else.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _literal(v) -> str:
    return f"'{v}'" if isinstance(v, str) else str(v)


def partition_path(location: str, spec: dict) -> str:
    return location.rstrip("/") + "/" + "/".join(f"{k}={v}" for k, v in spec.items())


def table_location(spark: SparkSession, table: str) -> str:
    return spark.sql(f"describe formatted {table}").where("col_name = 'Location'").collect()[0].data_type


def _conform(spark: SparkSession, df: DataFrame, table: str, keep_partition_cols: bool) -> DataFrame:
    """Same columns, order and types as the Hive table, so every file is readable with the declared schema."""
    t = spark.table(table)
    part_cols = [c.name for c in spark.catalog.listColumns(table) if c.isPartition]
    fields = [f for f in t.schema.fields if keep_partition_cols or f.name not in part_cols]
    return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in fields])


def _delete_prefixes(location: str, specs: list[dict]) -> int:
    """Remove existing files of the partitions about to be rewritten (gs:// locations only)."""
    from common.config import gcs_client

    bucket_name, _, base = location.removeprefix("gs://").partition("/")
    bucket = gcs_client().bucket(bucket_name)
    wanted = {partition_path(base, s).strip("/") + "/" for s in specs}
    deleted = 0
    for blob in bucket.list_blobs(prefix=base.strip("/") + "/" if base else None):
        if any(blob.name.startswith(w) for w in wanted):
            blob.delete()
            deleted += 1
    return deleted


def _register(spark: SparkSession, table: str, location: str, specs: list[dict], chunk: int = 200) -> None:
    for i in range(0, len(specs), chunk):
        part = specs[i:i + chunk]
        drops = ", ".join("partition (" + ", ".join(f"{k}={_literal(v)}" for k, v in s.items()) + ")" for s in part)
        spark.sql(f"alter table {table} drop if exists {drops}")
        adds = " ".join("partition (" + ", ".join(f"{k}={_literal(v)}" for k, v in s.items()) + f") location '{partition_path(location, s)}'"
                        for s in part)
        spark.sql(f"alter table {table} add if not exists {adds}")


def write_partition(spark: SparkSession, df: DataFrame, table: str, spec: dict) -> str:
    """Overwrite one partition of `table` with `df` (df must NOT contain the partition columns)."""
    location = table_location(spark, table)
    path = partition_path(location, spec)
    _conform(spark, df, table, keep_partition_cols=False).write.mode("overwrite").parquet(path)
    _register(spark, table, location, [spec])
    return path


def write_partitions(spark: SparkSession, df: DataFrame, table: str, part_cols: list[str]) -> list[dict]:
    """Overwrite every partition present in `df` (df contains the partition columns) in one Spark job.

    Used for backfills: one Parquet file per partition, then all partitions registered in Hive.
    """
    location = table_location(spark, table)
    df = _conform(spark, df, table, keep_partition_cols=True).cache()
    specs = [r.asDict() for r in df.select(*part_cols).distinct().orderBy(*part_cols).collect()]
    if not specs:
        return []
    _delete_prefixes(location, specs)
    df.repartition(*part_cols).write.mode("append").partitionBy(*part_cols).parquet(location)
    _register(spark, table, location, specs)
    df.unpersist()
    return specs
