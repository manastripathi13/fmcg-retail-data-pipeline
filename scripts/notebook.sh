#!/usr/bin/env bash
# Start JupyterLab on http://localhost:8888 (local only, no token) in the notebooks/ folder.
# Pick the "FMCG PySpark (Spark 4.0.1, GCS + Hive)" kernel: `spark`, `F` and gsutil are preconfigured.
set -euo pipefail
cd "$(dirname "$0")/.."
unset VIRTUAL_ENV
[ -f "$HOME/Library/Jupyter/kernels/fmcg-pyspark/kernel.json" ] || [ -f "$HOME/.local/share/jupyter/kernels/fmcg-pyspark/kernel.json" ] || ./scripts/install_kernel.sh
docker compose up -d >/dev/null   # GCS emulator + Hive metastore must be running
exec uv run --group notebook jupyter lab --notebook-dir=notebooks --ip=127.0.0.1 --port=8888 --no-browser \
  --IdentityProvider.token='' --ServerApp.password='' --MultiKernelManager.default_kernel_name=fmcg-pyspark
