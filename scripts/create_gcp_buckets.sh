#!/usr/bin/env bash
# Create the real GCP project resources (run once billing is enabled on your account).
#
#   GCP_PROJECT=fmcg-transactional-pipeline BUCKET_PREFIX=fmcg-<your-suffix> ./scripts/create_gcp_buckets.sh
#
# Bucket names are global across all of GCP, so pick a unique BUCKET_PREFIX.
set -euo pipefail
: "${GCP_PROJECT:?set GCP_PROJECT}"; : "${BUCKET_PREFIX:?set BUCKET_PREFIX (must be globally unique)}"
REGION="${REGION:-asia-south1}"   # Mumbai

gcloud projects describe "$GCP_PROJECT" >/dev/null 2>&1 || gcloud projects create "$GCP_PROJECT" --name="FMCGTransactionalPipeline"
gcloud services enable storage.googleapis.com --project "$GCP_PROJECT"
for layer in landing datalake mart; do
  gcloud storage buckets create "gs://${BUCKET_PREFIX}-${layer}" --project "$GCP_PROJECT" \
    --location "$REGION" --uniform-bucket-level-access --public-access-prevention 2>/dev/null \
    || echo "gs://${BUCKET_PREFIX}-${layer} already exists"
done
# raw drops are only needed for 30 days once they are in the datalake
cat > /tmp/landing-lifecycle.json <<JSON
{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 30}}]}
JSON
gcloud storage buckets update "gs://${BUCKET_PREFIX}-landing" --lifecycle-file=/tmp/landing-lifecycle.json
gcloud storage ls --project "$GCP_PROJECT"
echo "Now run the pipeline with: GCS_MODE=real GCP_PROJECT=$GCP_PROJECT BUCKET_PREFIX=$BUCKET_PREFIX"
