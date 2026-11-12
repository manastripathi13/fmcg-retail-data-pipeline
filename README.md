# FMCG Retail Data Pipeline on GCP: GCS, PySpark, Hive, Airflow

An FMCG retailer sends its daily transactional data as gzipped CSV files. This pipeline lands them in Google Cloud Storage, keeps a raw Parquet **datalake** partitioned by day, builds a business-ready **mart** partitioned by ISO week, and publishes a weekly insights report. Both layers are **Hive databases** of external tables whose data lives in GCS. Airflow runs every step.

**Stack:** Python · PySpark · Google Cloud Storage · Hive metastore · Apache Airflow

```
 retailer systems                 GCS buckets                                  Hive metastore (Postgres)
 ─────────────────                ───────────────────────────────────────      ─────────────────────────
 transactions  ┐                  gs://fmcg-landing/<table>/YYYY/MM/DD/
 customers     ├─ daily .csv.gz ─►   <table>_YYYYMMDD.csv.gz
 stores        │                         │  PySpark raw_to_datalake
 promotions    ┘                         ▼
                                  gs://fmcg-datalake/<table>/year=/month=/day=   ◄─ database `datalake` (4 tables)
                                         │  PySpark datalake_to_mart (de-dup, enrich, aggregate, reconcile)
                                         ▼
                                  gs://fmcg-mart/<table>/year=/week=             ◄─ database `mart` (5 tables)
                                         │  PySpark weekly_insights (Sundays)
                                         ▼
                                  docs/insights/<year>-W<week>.md + gs://fmcg-mart/_reports/
 Airflow: fmcg_daily_pipeline (@daily, catch-up) runs and checks every step
```

## Results from a full Airflow run (14 days, 26 Oct to 8 Nov 2026)

| What | Result |
|---|---|
| Airflow runs | 14 of 14 green; the weekly report was written on both Sundays |
| Landing | 56 `.csv.gz` files (4 tables × 14 days) |
| Datalake | 4 Hive tables, 14 daily partitions each (`year=2026/month=10/day=26` …), landing rows = datalake rows every day |
| Mart | 5 Hive tables, 2 weekly partitions (`year=2026/week=44`, `week=45`), 53,840 sales lines |
| Data quality | 263 re-sent duplicate POS lines removed; store, category and loyalty-tier totals reconcile to the line-level fact table every day |
| Week 44 | Net sales ₹85.9 L |
| Week 45 (Diwali week) | Net sales ₹1.21 Cr, **+41.1%** vs the same days of week 44; Dairy +61%; best day Sunday 8 Nov (₹27.6 L) |
| Promotions | "Ghee for Diwali" lifted units 97% for a 12% discount (ROI 3.6x); "Fizzup Party Pack" 6.5x |

The full reports are [`docs/insights/2026-W44.md`](docs/insights/2026-W44.md) and [`docs/insights/2026-W45.md`](docs/insights/2026-W45.md). All data is synthetic.

## The data

Each day the retailer drops four files into the landing bucket:

| File | Contents | Volume per day |
|---|---|---|
| `transactions_YYYYMMDD.csv.gz` | POS line items: transaction, store, customer (empty for walk-ins), SKU, brand, category, quantity, price, discount, promotion, payment mode, channel | 2,800 to 6,200 lines |
| `customers_YYYYMMDD.csv.gz` | Loyalty members: full extract on day 1, then only members who changed tier | 3,000, then about 25 |
| `stores_YYYYMMDD.csv.gz` | Full store master: format, city, state, region, area | 46 |
| `promotions_YYYYMMDD.csv.gz` | Promotions live or announced (type, discount, brand, dates) | 4 to 7 |

The simulated data has realistic messiness:
- demand builds towards Diwali (8 Nov) and peaks at weekends;
- promotions lift units;
- about 1% of lines are returns (negative quantity);
- about 30% of baskets are walk-ins with no customer ID;
- about 0.5% of lines are re-sent twice by the POS.

## The layers

### Landing bucket: `gs://<prefix>-landing`
Files exactly as delivered: `<table>/YYYY/MM/DD/<table>_YYYYMMDD.csv.gz`. Nothing is changed here, so any day can be reprocessed.

### Datalake: bucket `gs://<prefix>-datalake`, Hive database `datalake`
- **One table per file:** `datalake.transactions`, `datalake.customers`, `datalake.stores`, `datalake.promotions`.
- **Raw content, proper types:** the columns are exactly the source's, cast to timestamps, dates and decimals, plus `_source_file` and `_ingested_at`. No business logic is applied.
- **Storage:** Parquet with Snappy compression, partitioned by the drop date: `year=YYYY/month=M/day=D`.
- **Manifest:** each load writes `_manifests/YYYY/MM/DD/manifest.json` with row counts and NULL-key counts.

### Mart: bucket `gs://<prefix>-mart`, Hive database `mart`
All five tables are partitioned by **ISO year and ISO week** (`year=2026/week=45`). Each daily run rebuilds the current week, Monday to that day.

| Table | Grain | Built from |
|---|---|---|
| `fact_sales` | one row per sales line | De-duplicated transactions, enriched with the store, the customer's tier and age band, and the promotion. Adds gross, discount and net amounts, a returns flag, weekday and weekend flag, and customer type (loyalty member or walk-in). |
| `store_weekly_performance` | store × week | Net sales, baskets, customers, units, average basket value, promotion share of sales, returns, sales per sq ft, week-on-week change (same days last week), rank within region |
| `category_brand_weekly` | category × sub-category × brand × week | Units, net sales, share of category, week-on-week change |
| `promotion_weekly_effectiveness` | promotion × week | Units and sales on promotion, discount cost, units per day vs the brand's normal units per day, incremental sales, ROI |
| `customer_tier_weekly` | loyalty tier × week | Active customers, baskets, spend per customer, visit frequency, share of sales |

Before a week is reported as done, `datalake_to_mart` checks that the store, category and tier totals equal the `fact_sales` total, and that every line found its store. The results go to `_manifests/<year>/W<week>/manifest_<date>.json`, and the run fails if any check fails.

## Airflow: the main way to run it

`airflow/dags/fmcg_daily_pipeline.py` (`@daily`, 26 Oct to 8 Nov 2026, catch-up, one run at a time):

```
ensure_infrastructure ─► simulate_source_drop ─► wait_for_landing_files ─► raw_to_datalake ─► validate_datalake
    ─► datalake_to_mart ─► validate_mart ─► is_end_of_week ─► weekly_insights
```

| Task | What it does |
|---|---|
| `ensure_infrastructure` | Starts the services, downloads the Spark jars, creates the Hive metastore schema and the buckets (all idempotent) |
| `simulate_source_drop` | Stands in for the retailer's system writing the day's 4 files to landing |
| `wait_for_landing_files` | Sensor: waits until all 4 files exist in GCS (reschedule mode, so it doesn't hold a worker slot) |
| `raw_to_datalake` | PySpark: `.csv.gz` → typed Parquet partition in `datalake.*` |
| `validate_datalake` | Landing row count = datalake row count per table; no NULL business keys |
| `datalake_to_mart` | PySpark: rebuild the ISO week in `mart.*`, then reconcile |
| `validate_mart` | Every reconciliation check in the mart manifest passed |
| `is_end_of_week` | Short-circuit: only Sundays continue |
| `weekly_insights` | PySpark: weekly business report from the mart |

`fmcg_reset` (manual) wipes the local buckets and metastore so you can replay from scratch.

Start a local Airflow 3 pointed at this repo's DAGs (web UI on http://localhost:8081, no login):

```bash
uv venv --python 3.12 ~/airflow-venv
curl -sO https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.12.txt
uv pip install --python ~/airflow-venv/bin/python -c constraints-3.12.txt apache-airflow==3.2.2 apache-airflow-providers-standard

export AIRFLOW_HOME=~/airflow AIRFLOW__CORE__DAGS_FOLDER="$PWD/airflow/dags" AIRFLOW__CORE__LOAD_EXAMPLES=False \
       AIRFLOW__API__PORT=8081 AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True \
       AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://localhost:8081/execution/ PATH="$HOME/airflow-venv/bin:$PATH"
airflow standalone
```

Unpause `fmcg_daily_pipeline` and Airflow replays the 14 days, about 2 to 3 minutes each. Docker Desktop must be running.

## Jupyter notebook (PySpark + gsutil)

A JupyterLab environment with a ready-made kernel, **FMCG PySpark (Spark 4.0.1, GCS + Hive)**:

```bash
./scripts/notebook.sh      # JupyterLab on http://localhost:8888 (local only); registers the kernel on first run
```

When the kernel starts it runs [`notebooks/spark_init.py`](notebooks/spark_init.py), so every notebook already has:
- **`spark`**: a SparkSession with the GCS connector and the Hive metastore (the same settings as the pipeline jobs).
- **`F`**: `pyspark.sql.functions`.
- **`gsutil` and `gcloud storage`** in `!` shell cells, pointed at the project's buckets.

```python
spark.table("mart.store_weekly_performance").where("year = 2026 and week = 45").show(10, 0)
spark.sql("select category, sum(net_sales) from mart.category_brand_weekly where year = 2026 and week = 45 group by 1").show(10, 0)
!gsutil ls -lh gs://fmcg-datalake/
!gsutil du -sh gs://fmcg-landing gs://fmcg-datalake gs://fmcg-mart
```

[`notebooks/fmcg_lakehouse_queries.ipynb`](notebooks/fmcg_lakehouse_queries.ipynb) is a worked example, saved with its outputs:
- a GCS configuration cell;
- what is in each layer;
- the raw datalake through `spark.table`;
- business questions through `spark.sql(...).show(10, 0)`;
- DataFrames registered as temp views, plus a matplotlib chart;
- proof of partition pruning;
- browsing the buckets with `gsutil`.

**How `gsutil` reaches the emulator.** `scripts/setup_gcloud_emulator.sh` creates a separate gcloud configuration, `fmcg-emulator`, with the storage endpoint set to `http://localhost:4443` and no login. The notebook selects it only for its own shell commands (`CLOUDSDK_ACTIVE_CONFIG_NAME`), so your default gcloud configuration and login are unchanged. `gsutil` cannot use a custom endpoint itself, so `.gcloud/boto.cfg` makes it run through `gcloud storage`, which can. With `GCS_MODE=real`, the notebook uses your normal gcloud login and project instead.

## Query the data with SQL

The `datalake` and `mart` Hive databases can be queried with SQL. Spark SQL reads the table definitions from the Hive metastore and the Parquet files from the `gs://` buckets.

```bash
./scripts/sql_shell.sh          # interactive prompt (spark-sql>), or: make sql-shell
```

```sql
show tables in mart;
select region, sum(net_sales) as net_sales_inr
from mart.store_weekly_performance
where year = 2026 and week = 45
group by region
order by net_sales_inr desc;
```

- **One-off query:** `./scripts/sql_shell.sh -e "select ..."`.
- **Run a file:** `./scripts/sql_shell.sh -f docs/sample_queries.sql`.
- **Sample queries:** [`docs/sample_queries.sql`](docs/sample_queries.sql) has ready-made queries covering daily volumes, region and store rankings, category growth, promotion ROI, loyalty tiers, hourly sales and payment mix.
- **Filter on partition columns:** always include `year`, `month`, `day` (datalake) or `year`, `week` (mart). Spark then reads only the matching GCS folders (partition pruning).
- **Promotions with `NULL` uplift:** these ran from the first day of the data, so there are no non-promotion days for that brand to compare against yet.
- **Browsing the raw files:** the emulator's JSON API lists every object, for example `curl "http://localhost:4443/storage/v1/b/fmcg-mart/o?prefix=fact_sales/"`. On real GCP, use the Cloud Console's Storage browser or `gcloud storage ls gs://<bucket>/...`. You can also define BigQuery external tables over the same Parquet folders to query them from BigQuery.

## Run it without Airflow

```bash
make setup                    # Python deps + Spark jars (GCS connector, Postgres JDBC)
make infra                    # GCS emulator, Hive metastore, buckets
make day DATE=2026-10-26      # land -> datalake -> validate -> mart -> validate for one day
make insights DATE=2026-11-08
make sql Q="show partitions mart.fact_sales"
make test
```

## Local emulator vs real GCP

| Piece | Local (default, `GCS_MODE=emulator`) | Real GCP (`GCS_MODE=real`) |
|---|---|---|
| Buckets | `fake-gcs-server` in Docker; same `gs://` API | Real GCS buckets in `asia-south1` (Mumbai), created by `scripts/create_gcp_buckets.sh` |
| Spark → GCS | Google's Cloud Storage connector for Hadoop (2.2.28) pointed at the emulator | Same connector, Application Default Credentials |
| Hive metastore | Hive 2.3 schema in Postgres (Docker) | Same, or Dataproc Metastore |
| Spark | PySpark `local[4]` on the laptop | Same jobs on Dataproc (`gcloud dataproc jobs submit pyspark`) or Dataproc Serverless |
| Airflow | Local Airflow 3 | Cloud Composer |

To switch to real GCS, once billing is enabled on the account:

```bash
gcloud auth application-default login
GCP_PROJECT=fmcg-transactional-pipeline BUCKET_PREFIX=fmcg-<unique-suffix> ./scripts/create_gcp_buckets.sh
export GCS_MODE=real GCP_PROJECT=fmcg-transactional-pipeline BUCKET_PREFIX=fmcg-<unique-suffix>
make day DATE=2026-10-26
```

> Everything in the "Local" column has been run end to end. The real-GCP path uses the same code and the documented connector settings, but has not been run from this repo yet, because the GCP billing account used during development was closed.

---

## Explained from zero

### The business problem
A retailer selling everyday goods (soap, biscuits, tea, milk) across 46 stores wants to know every week:
- how sales are moving;
- which stores and regions lead;
- which categories and brands are growing;
- whether promotions actually pay for their discounts;
- how loyalty members behave.

The raw data arrives as four separate files a day, with duplicates, returns and anonymous customers.

### The pieces

| Piece | Plain-words explanation | Role here |
|---|---|---|
| **Google Cloud Storage (GCS)** | Cloud file storage organised in *buckets*. Cheap, durable, and readable by Spark and BigQuery. | Holds the landing files, the datalake and the mart. |
| **CSV.gz** | A spreadsheet-like text file, compressed with gzip to about a fifth of its size. | The format the retailer sends. |
| **Parquet** | A column-based file format with compression and types built in. Queries read only the columns they need. | The format of the datalake and mart. |
| **Partitioning** | Splitting a table into folders by a column, such as `year=2026/month=11/day=8`. A query for one day reads one folder instead of the whole table. | Datalake by day, mart by week. |
| **PySpark** | Python API for Apache Spark, which processes large data in parallel. | All transformations. |
| **Hive metastore** | A catalogue of table definitions: names, columns, types, partitions and the GCS folder holding the data. Spark, Hive, Trino and Presto all read it. | Makes the buckets look like the databases `datalake` and `mart`. |
| **External table** | A Hive table whose data lives outside Hive (here in GCS). Dropping the table removes only the definition, not the data. | All 9 tables. |
| **Airflow** | A scheduler that runs tasks in order, waits for files, retries failures and shows everything in a UI. | Runs the daily pipeline. |

### One business day, step by step
1. **Files land.** The day's 4 files appear in `gs://fmcg-landing/<table>/2026/11/08/`.
2. **Sensor.** Airflow waits until all 4 are present.
3. **Raw to datalake.**
   - Spark reads each `.csv.gz` with an explicit schema and casts types.
   - It writes Parquet to `gs://fmcg-datalake/<table>/year=2026/month=11/day=8/` and registers that partition in Hive.
   - Re-running the day replaces only that folder, so the step is **idempotent**.
4. **Validate.** Row counts in landing must equal the datalake, and business keys must not be NULL.
5. **Datalake to mart.**
   - For the ISO week containing the day, Spark de-duplicates POS lines.
   - It picks each customer's, store's and promotion's latest record.
   - It joins them onto each sale, computes amounts and builds the five mart tables in `gs://fmcg-mart/<table>/year=2026/week=45/`.
   - Every total must reconcile.
6. **Weekly report.** On Sundays, Spark queries the mart and writes the insights report.

### Key ideas
- **Why two layers?** The datalake keeps the data close to how it arrived, so it can be audited or reprocessed if business rules change. The mart holds the answers business users need, already cleaned and aggregated.
- **Why partition the mart by week?** Reporting is weekly, so a query for "week 45" reads one folder. Each daily run overwrites only the current week's partition.
- **ISO weeks:** weeks run Monday to Sunday, and week 1 is the week containing the year's first Thursday. Use ISO year and ISO week together, otherwise late-December days get the wrong year.
- **Week-on-week compares the same days:** on a Wednesday, the current week has 3 days of data, so it is compared with Monday to Wednesday of last week, not the full previous week.
- **Promotion uplift:** units per day while on promotion, divided by the same brand's units per day without a promotion, minus 1. ROI = extra net sales ÷ discount cost.

---

## Talking about it in an interview

**30-second version.** "An FMCG retailer drops four gzipped CSVs a day: transactions, customers, stores and promotions. I land them in a GCS bucket and run a PySpark job that writes them as typed Parquet into a datalake bucket, partitioned by year, month and day and registered as Hive external tables. A second PySpark job builds a weekly mart in its own bucket, partitioned by ISO year and week: de-duplicated, enriched sales facts plus store, category, promotion and loyalty-tier tables. Every total is reconciled before the week is marked done. Airflow runs it daily, with a sensor for the files, validation tasks between layers and a weekly insights report on Sundays. On the simulated Diwali week it showed sales up 41% and which promotions actually returned their discount."

**Likely follow-up questions**
- *Why external tables?* Data and metadata are decoupled. Several engines can read the same files, and dropping a table never deletes data.
- *How do you make reruns safe?* Each run overwrites one explicit partition folder, then drops and re-adds that partition in the metastore. Nothing else is touched.
- *Why not Spark's dynamic partition overwrite?* It stages output and renames folders. On object storage, renames are copy-and-delete and slow, and with the emulator they failed. Writing one known partition is simpler and predictable.
- *Small files?* Daily partitions are small here. In production, coalesce before writing, and compact old partitions periodically.
- *Late-arriving data?* Today each daily file lands in its own drop-date partition. For late transactions you'd partition by business date and rebuild every affected day and week, driven by the manifest.
- *How would this run on GCP?* Dataproc or Dataproc Serverless for Spark, Dataproc Metastore for Hive, Cloud Composer for Airflow (with `DataprocSubmitJobOperator`), lifecycle rules on the landing bucket, and BigQuery external tables or BigLake over the mart for analysts.
- *Data quality?* Landing vs datalake row counts, NULL-key checks, de-duplication counts and cross-table reconciliation, all written to manifests and enforced by Airflow tasks.

## Repo layout
```
generator/generate_fmcg_drop.py      synthetic daily drop of 4 .csv.gz files into the landing bucket
common/config.py                     buckets, GCS mode (emulator/real), Spark session with GCS connector + Hive
common/hive_ddl.py                   Hive databases and external tables for datalake and mart
common/hive_io.py                    idempotent single-partition writer (enforces the Hive schema)
jobs/raw_to_datalake.py              PySpark: landing -> datalake (year/month/day)
jobs/datalake_to_mart.py             PySpark: datalake -> mart (year/week) with reconciliation
jobs/weekly_insights.py              PySpark: weekly business report from the mart
scripts/validate_layer.py            checks between layers (row counts, NULL keys, reconciliation)
scripts/check_landing.py             used by the Airflow sensor
scripts/create_gcp_buckets.sh        real GCP project + buckets (needs billing)
scripts/init_metastore.sh            Hive metastore schema (Apache Hive 2.3 script, like schematool)
airflow/dags/fmcg_daily_pipeline.py  the daily DAG
airflow/dags/fmcg_reset.py           wipe local state
docs/insights/                       generated weekly reports
docs/sample_queries.sql              ready-to-run SQL over datalake and mart
notebooks/fmcg_lakehouse_queries.ipynb  PySpark notebook: spark.table / spark.sql / gsutil, with outputs
notebooks/spark_init.py              kernel startup: creates `spark` with GCS + Hive
scripts/install_kernel.sh            registers the "FMCG PySpark" Jupyter kernel
scripts/notebook.sh                  starts JupyterLab on localhost:8888
scripts/setup_gcloud_emulator.sh     gcloud/gsutil -> GCS emulator via a separate gcloud configuration
tests/                               unit tests (no Spark or GCS needed)
```
