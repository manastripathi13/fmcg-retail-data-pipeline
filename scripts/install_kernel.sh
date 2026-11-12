#!/usr/bin/env bash
# Register the "FMCG PySpark" Jupyter kernel for this user (visible in JupyterLab and VS Code).
# The kernel uses this project's uv environment and runs notebooks/spark_init.py at startup,
# so `spark` (GCS + Hive) and `F` (pyspark.sql.functions) exist in every notebook.
set -euo pipefail
cd "$(dirname "$0")/.."
REPO="$PWD"
unset VIRTUAL_ENV
uv sync --group notebook >/dev/null
./scripts/download_jars.sh >/dev/null
./scripts/setup_gcloud_emulator.sh >/dev/null
uv run python -m ipykernel install --user --name fmcg-pyspark --display-name "FMCG PySpark (Spark 4.0.1, GCS + Hive)" >/dev/null
KDIR="$(uv run jupyter kernelspec list --json | uv run python -c "import sys,json; print(json.load(sys.stdin)['kernelspecs']['fmcg-pyspark']['resource_dir'])")"
uv run python - "$KDIR/kernel.json" "$REPO" <<'PY'
import json, sys
path, repo = sys.argv[1], sys.argv[2]
k = json.load(open(path))
k["argv"] = [f"{repo}/.venv/bin/python", "-m", "ipykernel_launcher", "-f", "{connection_file}",
             f"--IPKernelApp.exec_files={repo}/notebooks/spark_init.py"]
k["env"] = {
    "FMCG_REPO": repo,
    "PYTHONPATH": repo,
    "SPARK_HOME": f"{repo}/.venv/lib/python3.12/site-packages/pyspark",
    "PYSPARK_PYTHON": f"{repo}/.venv/bin/python",
    "PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin",
    "GCS_MODE": "emulator",
    # `!gsutil ...` and `!gcloud storage ...` in notebook cells talk to the GCS emulator,
    # through the separate gcloud configuration created by scripts/setup_gcloud_emulator.sh
    "CLOUDSDK_ACTIVE_CONFIG_NAME": "fmcg-emulator",
    "BOTO_CONFIG": f"{repo}/.gcloud/boto.cfg",
    "CLOUDSDK_COMPONENT_MANAGER_DISABLE_UPDATE_CHECK": "1",
}
k["metadata"] = {"debugger": True}
json.dump(k, open(path, "w"), indent=2)
print(json.dumps(k, indent=2))
PY
echo "kernel installed at $KDIR"
