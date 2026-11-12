#!/usr/bin/env bash
# Start JupyterLab on http://localhost:8888 (local only, no token) in the notebooks/ folder
# and open it in your default web browser (Chrome, Safari, ...).
# Pick the "FMCG PySpark (Spark 4.0.1, GCS + Hive)" kernel: `spark`, `F` and gsutil are preconfigured.
set -euo pipefail
cd "$(dirname "$0")/.."
unset VIRTUAL_ENV
[ -f "$HOME/Library/Jupyter/kernels/fmcg-pyspark/kernel.json" ] || [ -f "$HOME/.local/share/jupyter/kernels/fmcg-pyspark/kernel.json" ] || ./scripts/install_kernel.sh
docker compose up -d >/dev/null   # GCS emulator + Hive metastore must be running
# reuse a JupyterLab that is already running instead of starting a second one
for port in 8888 8889 8890; do
  if curl -sf "http://127.0.0.1:$port/api/status" >/dev/null 2>&1; then
    echo "JupyterLab is already running: http://127.0.0.1:$port/lab (opening it)"
    open "http://127.0.0.1:$port/lab/tree/fmcg_lakehouse_queries.ipynb" 2>/dev/null || true
    exit 0
  fi
done
exec uv run --group notebook jupyter lab --notebook-dir=notebooks --ip=127.0.0.1 --port=8888 \
  --IdentityProvider.token='' --ServerApp.password='' --MultiKernelManager.default_kernel_name=fmcg-pyspark \
  --ServerApp.default_url=/lab/tree/fmcg_lakehouse_queries.ipynb
