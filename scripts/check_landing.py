"""Exit 0 when all four files for --date are in the landing bucket, else 1 (used by the Airflow sensor)."""
import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import LANDING_BUCKET, TABLES, gcs_client  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--date", required=True, type=date.fromisoformat)
d = ap.parse_args().date
bucket = gcs_client().bucket(LANDING_BUCKET)
missing = [t for t in TABLES if not bucket.blob(f"{t}/{d:%Y/%m/%d}/{t}_{d:%Y%m%d}.csv.gz").exists()]
print("all landing files present" if not missing else f"waiting for: {missing}")
sys.exit(1 if missing else 0)
