"""Write one partition of a Hive external table on GCS, idempotently.

Pattern: overwrite exactly one partition folder, then register it in the Hive metastore.
Re-running the same day/week replaces that partition and nothing else.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def partition_path(location: str, spec: dict[str, int]) -> str:
    return location.rstrip("/") + "/" + "/".join(f"{k}={v}" for k, v in spec.items())


def write_partition(spark: SparkSession, df: DataFrame, table: str, spec: dict[str, int]) -> str:
    """Overwrite one partition of `table` with `df` (df must NOT contain the partition columns)."""
    location = spark.sql(f"describe formatted {table}").where("col_name = 'Location'").collect()[0].data_type
    path = partition_path(location, spec)
    # Enforce the Hive schema: same columns, same order, same types as the table definition,
    # so every Parquet file in the table is readable with the declared schema.
    from pyspark.sql import functions as F

    target = [f for f in spark.table(table).schema.fields if f.name not in spec]
    df = df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in target])
    df.write.mode("overwrite").parquet(path)
    cond = ", ".join(f"{k}={v}" for k, v in spec.items())
    spark.sql(f"alter table {table} drop if exists partition ({cond})")
    spark.sql(f"alter table {table} add partition ({cond}) location '{path}'")
    return path
