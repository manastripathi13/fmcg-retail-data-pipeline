"""Lightweight checks between pipeline steps (no Spark needed).

  --layer datalake  rows in each landing .csv.gz == rows written to the datalake partition,
                    and no NULL business keys (transaction_id, store_id, sku, customer_id, ...)
  --layer mart      the mart manifest exists and every reconciliation check passed
Exit code 1 fails the Airflow task.
"""

import argparse
import csv
import gzip
import io
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import DATALAKE_BUCKET, LANDING_BUCKET, MART_BUCKET, TABLES, gcs_client  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--layer", choices=["datalake", "mart"], required=True)
ap.add_argument("--date", required=True, type=date.fromisoformat)
a = ap.parse_args()
d, client, problems = a.date, gcs_client(), []

if a.layer == "datalake":
    manifest = json.loads(client.bucket(DATALAKE_BUCKET).blob(f"_manifests/{d:%Y/%m/%d}/manifest.json").download_as_bytes())
    for t in TABLES:
        raw = client.bucket(LANDING_BUCKET).blob(f"{t}/{d:%Y/%m/%d}/{t}_{d:%Y%m%d}.csv.gz").download_as_bytes()
        landing_rows = sum(1 for _ in csv.reader(io.StringIO(gzip.decompress(raw).decode()))) - 1
        m = manifest["tables"][t]
        ok = landing_rows == m["rows"]
        print(f"{'PASS' if ok else 'FAIL'}  {t:<13} landing={landing_rows:>6}  datalake={m['rows']:>6}  null_keys={m['null_keys']}")
        if not ok:
            problems.append(f"{t}: row count mismatch")
        problems += [f"{t}: {k} has {n} NULLs" for k, n in m["null_keys"].items() if n]
else:
    y, w, _ = d.isocalendar()
    manifest = json.loads(client.bucket(MART_BUCKET).blob(f"_manifests/{y}/W{w:02d}/manifest_{d:%Y%m%d}.json").download_as_bytes())
    print(f"week {y}-W{w:02d}: net sales INR {manifest['net_sales']:,.2f}, rows {manifest['row_counts']}")
    for k, v in manifest["checks"].items():
        print(f"{'FAIL' if v is False else 'PASS'}  {k} = {v}")
        if v is False:
            problems.append(k)
    if manifest["row_counts"]["fact_sales"] == 0:
        problems.append("fact_sales is empty")

for p in problems:
    print("PROBLEM", p)
sys.exit(1 if problems else 0)
