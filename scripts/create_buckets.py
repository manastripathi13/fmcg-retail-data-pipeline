"""Create the three buckets (landing, datalake, mart). Idempotent. Works on the emulator and on real GCS."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import DATALAKE_BUCKET, GCS_MODE, LANDING_BUCKET, MART_BUCKET, gcs_client  # noqa: E402

client = gcs_client()
existing = {b.name for b in client.list_buckets()}
for name in [LANDING_BUCKET, DATALAKE_BUCKET, MART_BUCKET]:
    if name in existing:
        print(f"exists   gs://{name}")
    else:
        b = client.bucket(name)
        b.storage_class = "STANDARD"
        client.create_bucket(b, location="asia-south1")   # Mumbai
        print(f"created  gs://{name}  ({GCS_MODE})")
