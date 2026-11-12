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
    assert list(types)[-2:] == ["filename", "modified_dttm"]


def test_every_mart_table_has_columns():
    for name, ddl in MART_TABLES.items():
        assert len(column_types(ddl)) >= 5, name


def test_partition_paths():
    assert partition_path("gs://b/transactions", {"year": 2026, "month": 10, "day": 26}) == "gs://b/transactions/year=2026/month=10/day=26"
    assert partition_path("gs://b/fact_sales/", {"year": 2026, "week": 45}) == "gs://b/fact_sales/year=2026/week=45"


def test_history_window_is_three_years():
    assert (g.HISTORY_END - g.HISTORY_START).days + 1 == 1096
    assert g.HISTORY_END == date(2026, 11, 13)


def test_iso_week_helpers():
    from common.run_range import iso_weeks, same_days_last_year
    weeks = iso_weeks(date(2026, 11, 2), date(2026, 11, 13))
    assert [(y, w) for y, w, _, _ in weeks] == [(2026, 45), (2026, 46)]
    assert weeks[-1][3] == date(2026, 11, 13)                       # second week is week-to-date
    assert same_days_last_year(2026, 45, 7) == (date(2025, 11, 3), date(2025, 11, 9))
    assert same_days_last_year(2021, 53, 3)[0] == date(2020, 12, 28)  # 2020 had a week 53 as well


def test_demand_has_trend_and_festive_peaks():
    assert g.demand_multiplier(date(2026, 11, 7)) > g.demand_multiplier(date(2026, 10, 20))        # Diwali build-up
    assert g.demand_multiplier(date(2026, 6, 10)) > g.demand_multiplier(date(2024, 6, 12))          # year-on-year growth


def test_day_files_are_reproducible_and_valid():
    a, b = g.files_for_day(date(2025, 3, 10)), g.files_for_day(date(2025, 3, 10))
    assert a == b
    tx = a["transactions"]
    assert all(r["store_id"] and r["sku"] for r in tx)
    assert any(r["quantity"] < 0 for r in tx)                       # returns exist
    keys = [(r["transaction_id"], r["line_no"]) for r in tx]
    assert len(keys) > len(set(keys))                              # re-sent duplicates exist
    assert all(r["promotion_id"] == "" or r["discount_amount"] != 0 for r in tx)
    assert len({s["store_id"] for s in a["stores"]}) == len(a["stores"])


def test_stores_open_over_time():
    early, late = g.files_for_day(g.HISTORY_START)["stores"], g.files_for_day(g.HISTORY_END)["stores"]
    assert len(late) > len(early)


def test_csv_gz_roundtrip():
    data = g.to_csv_gz([{"a": 1, "b": "x"}])
    assert gzip.decompress(data).decode().splitlines() == ["a,b", "1,x"]
