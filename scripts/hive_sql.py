"""Run HiveQL against the datalake/mart metastore, e.g.

    uv run python scripts/hive_sql.py "show partitions mart.fact_sales"
    uv run python scripts/hive_sql.py "select region, sum(net_sales) from mart.store_weekly_performance where year=2026 and week=45 group by 1"
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import get_spark  # noqa: E402

spark = get_spark("hive-sql")
for stmt in [s for s in " ".join(sys.argv[1:]).split(";") if s.strip()]:
    spark.sql(stmt).show(50, truncate=False)
