#!/usr/bin/env bash
# Point the Google Cloud CLI (gcloud storage / gsutil) at the local GCS emulator WITHOUT touching
# your default gcloud setup: it creates a separate named configuration, "fmcg-emulator", that is
# only used where CLOUDSDK_ACTIVE_CONFIG_NAME=fmcg-emulator is set (the Jupyter kernel sets it).
#
#   CLOUDSDK_ACTIVE_CONFIG_NAME=fmcg-emulator BOTO_CONFIG=$PWD/.gcloud/boto.cfg gsutil ls -lh gs://fmcg-datalake/
set -euo pipefail
cd "$(dirname "$0")/.."
command -v gcloud >/dev/null || { echo "install the Google Cloud CLI first: https://cloud.google.com/sdk/docs/install"; exit 1; }
gcloud config configurations describe fmcg-emulator >/dev/null 2>&1 || gcloud config configurations create fmcg-emulator --no-activate >/dev/null
export CLOUDSDK_ACTIVE_CONFIG_NAME=fmcg-emulator
gcloud config set project fmcg-transactional-pipeline >/dev/null 2>&1
gcloud config set api_endpoint_overrides/storage "${GCS_EMULATOR_URL:-http://localhost:4443}/storage/v1/" >/dev/null 2>&1
gcloud config set auth/disable_credentials true >/dev/null 2>&1      # the emulator needs no login
# gsutil itself cannot use a custom endpoint; this makes it run through `gcloud storage`, which can
mkdir -p .gcloud
printf '[GSUtil]\nuse_gcloud_storage = True\n' > .gcloud/boto.cfg
echo "gcloud configuration 'fmcg-emulator' -> ${GCS_EMULATOR_URL:-http://localhost:4443} (your default configuration is unchanged)"
gcloud storage ls
