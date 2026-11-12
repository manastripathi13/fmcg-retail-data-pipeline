.PHONY: setup infra day mart insights run-all sql sql-shell notebook test reset
setup:      ## python deps + Spark jars
	uv sync && ./scripts/download_jars.sh
infra:      ## GCS emulator, Hive metastore schema, buckets
	./scripts/ensure_infrastructure.sh
day:        ## one day without Airflow: make day DATE=2026-10-26
	uv run python generator/generate_fmcg_drop.py --date $(DATE)
	uv run python jobs/raw_to_datalake.py --date $(DATE)
	uv run python scripts/validate_layer.py --layer datalake --date $(DATE)
	uv run python jobs/datalake_to_mart.py --date $(DATE)
	uv run python scripts/validate_layer.py --layer mart --date $(DATE)
insights:   ## weekly report: make insights DATE=2026-11-08
	uv run python jobs/weekly_insights.py --date $(DATE)
sql:        ## make sql Q="show partitions mart.fact_sales"
	uv run python scripts/hive_sql.py "$(Q)"
sql-shell:  ## interactive SQL prompt over the datalake and mart Hive databases
	./scripts/sql_shell.sh
notebook:   ## JupyterLab with the "FMCG PySpark" kernel (spark + gsutil preconfigured)
	./scripts/notebook.sh
test:
	uv run pytest -q tests
reset:      ## wipe emulator buckets + metastore
	docker compose down -v
