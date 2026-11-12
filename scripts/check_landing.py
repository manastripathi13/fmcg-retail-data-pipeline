"""Exit 0 when all four files for every day of --date / --start..--end are in the landing bucket, else 1 (Airflow sensor)."""
import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import LANDING_BUCKET, TABLES, gcs_client  # noqa: E402
from common.run_range import add_range_args, days, resolve  # noqa: E402

ap = argparse.ArgumentParser()
add_range_args(ap)
start, end = resolve(ap.parse_args())
expected = {f"{t}/{d:%Y/%m/%d}/{t}_{d:%Y%m%d}.csv.gz" for t in TABLES for d in days(start, end)}
present = {b.name for t in TABLES for b in gcs_client().list_blobs(LANDING_BUCKET, prefix=f"{t}/")}
missing = sorted(expected - present)
print(f"all {len(expected)} landing files present for {start}..{end}" if not missing
      else f"waiting for {len(missing)} file(s), e.g. {missing[:3]}")
sys.exit(1 if missing else 0)
