"""Unit tests that need neither Spark nor GCS."""
import gzip
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.hive_ddl import DATALAKE_TABLES, MART_TABLES, column_types  # noqa: E402
from common.hive_io import partition_path  # noqa: E402
from generator import generate_fmcg_drop as g  # noqa: E402


def test_decimal_types_are_not_split():
    types = column_types(DATALAKE_TABLES["transactions"])
    assert types["unit_price"] == "decimal(12,2)" and types["discount_amount"] == "decimal(12,2)"
    assert list(types)[-2:] == ["_source_file", "_ingested_at"]


def test_every_mart_table_has_columns():
    for name, ddl in MART_TABLES.items():
        assert len(column_types(ddl)) >= 5, name


def test_partition_paths():
    assert partition_path("gs://b/transactions", {"year": 2026, "month": 10, "day": 26}) == "gs://b/transactions/year=2026/month=10/day=26"
    assert partition_path("gs://b/fact_sales/", {"year": 2026, "week": 45}) == "gs://b/fact_sales/year=2026/week=45"


def test_iso_weeks_of_the_simulation():
    assert g.SIM_START.isocalendar()[:2] == (2026, 44) and g.SIM_START.weekday() == 0   # Monday
    assert g.SIM_END.isocalendar()[:2] == (2026, 45) and g.SIM_END.weekday() == 6       # Sunday (Diwali)


def test_demand_peaks_before_diwali():
    assert g.demand_multiplier(date(2026, 11, 7)) > g.demand_multiplier(date(2026, 10, 27))


def test_transactions_are_reproducible_and_valid():
    import random
    base = random.Random(g.SEED)
    stores = g.build_stores(base)
    customers = g.build_customers(base, stores)
    prods, promos = g.products(), g.build_promotions()
    a = g.transactions_for(date(2026, 11, 2), random.Random(1), stores, customers, prods, promos)
    b = g.transactions_for(date(2026, 11, 2), random.Random(1), stores, customers, prods, promos)
    assert a == b
    assert all(r["store_id"] and r["sku"] for r in a)
    assert any(r["quantity"] < 0 for r in a)                       # returns exist
    keys = [(r["transaction_id"], r["line_no"]) for r in a]
    assert len(keys) > len(set(keys))                             # re-sent duplicates exist
    assert all(r["promotion_id"] == "" or r["discount_amount"] != 0 for r in a)


def test_csv_gz_roundtrip():
    data = g.to_csv_gz([{"a": 1, "b": "x"}])
    assert gzip.decompress(data).decode().splitlines() == ["a,b", "1,x"]
