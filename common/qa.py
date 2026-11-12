"""Shared helpers for the QA jobs: load rules, collect results, save them to Hive, decide pass/fail."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
RULES = yaml.safe_load((ROOT / "config" / "qa_rules.yaml").read_text())


class Results:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    def add(self, **row) -> None:
        row.setdefault("checked_at", datetime.now(timezone.utc).replace(tzinfo=None))
        self.rows.append(row)

    def counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for r in self.rows:
            out[r["status"]] = out.get(r["status"], 0) + 1
        return out

    def failures(self) -> list[dict]:
        return [r for r in self.rows if r["status"] == "FAIL"]

    def warnings(self) -> list[dict]:
        return [r for r in self.rows if r["status"] == "WARN"]

    def save(self, spark, table: str, columns: list[str], run_id: str) -> None:
        from common.hive_io import write_partition

        if not self.rows:
            return
        df = spark.createDataFrame([[r.get(c) for c in columns] for r in self.rows],
                                   schema=spark.table(table).drop("run_id").schema)
        write_partition(spark, df, table, {"run_id": run_id})
