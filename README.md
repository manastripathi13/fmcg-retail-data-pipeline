# FMCG Retail Data Pipeline on GCP: GCS, PySpark, Hive, Airflow

An FMCG retailer sends its daily transactional data as gzipped CSV files. This pipeline lands them in Google Cloud Storage and builds two layers, both **Hive databases** of external tables with their data in GCS:
- a raw Parquet **datalake**, partitioned by day;
- a business-ready **mart**, partitioned by ISO week.

It holds **three years of history (14 Nov 2023 to 13 Nov 2026)**. Four chained Airflow DAGs run it (datalake, then datalake QA, then mart, then mart QA). The QA stages check primary-key uniqueness, week-on-week changes beyond ±20% and year-on-year changes beyond ±40%.

**Stack:** Python · PySpark · Google Cloud Storage · Hive metastore · Apache Airflow · JupyterLab

```
 retailer systems                 GCS buckets                                    Hive metastore (Postgres)
 ─────────────────                ─────────────────────────────────────────      ─────────────────────────
 transactions  ┐                  gs://fmcg-landing/<table>/YYYY/MM/DD/<table>_YYYYMMDD.csv.gz
 customers     ├─ daily .csv.gz ─►       │
 stores        │                         │  DAG 1  fmcg_1_datalake      (PySpark raw_to_datalake)
 promotions    ┘                         ▼
                                  gs://fmcg-datalake/<table>/year=/month=/day=     ◄─ database `datalake` (4 tables)
                                         │  DAG 2  fmcg_2_datalake_qa   row counts · PK uniqueness · NULL keys
                                         │  DAG 3  fmcg_3_mart          (PySpark datalake_to_mart + weekly insights)
                                         ▼
                                  gs://fmcg-mart/<table>/year=/week=               ◄─ database `mart` (6 tables)
                                         │  DAG 4  fmcg_4_mart_qa       PK uniqueness · WoW ±20% · YoY ±40% · report
                                         ▼
                                  gs://fmcg-mart/_qa/  (qa.datalake_checks, qa.mart_checks) + docs/qa/LATEST.md
```

## Results from the 3-year backfill through Airflow

One manual trigger of `fmcg_1_datalake` with `{"start_date": "2023-11-14", "end_date": "2026-11-13"}` ran the whole chain on a laptop in about 1 hour 35 minutes (GCS emulator, Hive metastore on Postgres, Spark local mode).

| Stage | Result |
|---|---|
| Landing | 1,096 days x 4 files (`.csv.gz`) |
| `fmcg_1_datalake` | 4 tables, 1,096 daily partitions each (promotions: 405 days with rows, the rest header-only files); 3,555,180 raw transaction lines |
| `fmcg_2_datalake_qa` | Row counts, PK uniqueness and null keys pass on every partition. Raw POS duplicates average 0.48% of lines per day (max 0.50%), so `transactions` is WARN, under the 1% FAIL limit |
| `fmcg_3_mart` | 157 ISO weeks x 6 tables; 3,538,051 lines in `fact_sales` after de-duplication (17,129 duplicates removed); every week reconciles store, category, tier and company totals to the fact table |
| `fmcg_4_mart_qa` | 942 weekly PK checks pass with 0 duplicates. 9 company-level WoW/YoY breaches fail the run on purpose (below); 81 region/category breaches recorded as WARN |

Business view (49 stores, 4,500 loyalty members, 33 promotions, synthetic data):

| Period (ISO weeks) | Net sales | Growth |
|---|---|---|
| 20 Nov 2023 to 17 Nov 2024 | ₹40.34 Cr | |
| 18 Nov 2024 to 16 Nov 2025 | ₹46.97 Cr | +16.4% |
| 17 Nov 2025 to 13 Nov 2026 | ₹55.28 Cr | +17.7% |

All 9 company-level breaches sit around Diwali. The festival moves between ISO weeks each year (1 Nov 2024, 21 Oct 2025, 8 Nov 2026), so the same week last year is not a festival week:

- 2026-W45 (Diwali week) is the biggest week in the data: ₹1.94 Cr net, +45.7% WoW and +107.6% YoY against an ordinary week in 2025.
- 2025-W42, the week before Diwali 2025, is +64.7% YoY for the same reason.
- The weeks after Diwali drop 20% to 33% WoW (2024-W45, 2025-W44, 2026-W46).

This is the intended behaviour: the task goes red, an analyst confirms the festival shift, and the report keeps the explanation. A real data problem (a missing store drop, a double load) would show up in the same table.

Outputs committed with this run:

- [`docs/qa/LATEST.md`](docs/qa/LATEST.md): full QA report built from `qa.datalake_checks` and `qa.mart_checks`.
- [`docs/insights/2026-W45.md`](docs/insights/2026-W45.md): weekly insights for the latest complete week (Diwali 2026).
- [`notebooks/fmcg_lakehouse_queries.ipynb`](notebooks/fmcg_lakehouse_queries.ipynb): executed notebook with the 3-year KPIs, chart and QA queries.

## The data

Each day the retailer drops four files into the landing bucket:

| File | Contents | Volume per day |
|---|---|---|
| `transactions_YYYYMMDD.csv.gz` | POS line items: transaction, store, customer (empty for walk-ins), SKU, brand, category, quantity, price, discount, promotion, payment mode, channel | about 2,200 to 9,700 lines |
| `customers_YYYYMMDD.csv.gz` | Loyalty members: full extract on the first day, then new sign-ups and members who changed tier | about 1,900, then 10 to 20 |
| `stores_YYYYMMDD.csv.gz` | Snapshot of every store open that day: format, city, state, region, area | 43, growing to 49 |
| `promotions_YYYYMMDD.csv.gz` | Promotions live or announced (type, discount, brand, dates); header-only when none | 0 to 4 |

The synthetic data behaves like Indian FMCG retail over time:
- about 9% year-on-year growth, plus about 5% price inflation a year;
- new stores opening, with a ramp-up period, and loyalty members signing up throughout;
- festive peaks on the real dates of Diwali (2023-11-12, 2024-11-01, 2025-10-21, 2026-11-08) and Holi, plus New Year, summer beverages and a monsoon dip;
- a yearly promotion calendar, weekend peaks, and online and UPI shares growing each year;
- returns (negative quantity), walk-ins without a loyalty ID, and about 0.5% of POS lines re-sent twice.

## The layers

### Landing bucket: `gs://<prefix>-landing`
Files exactly as delivered: `<table>/YYYY/MM/DD/<table>_YYYYMMDD.csv.gz`. Nothing is changed here, so any day can be reprocessed.

### Datalake: bucket `gs://<prefix>-datalake`, Hive database `datalake`
- **One table per file:** `datalake.transactions`, `datalake.customers`, `datalake.stores`, `datalake.promotions`.
- **Raw content, proper types:** the columns are exactly the source's, cast to timestamps, dates and decimals. No business logic is applied.
- **Two load columns** are added to every row:
  - `filename`: the landing file the row came from, for example `transactions_20261108.csv.gz`.
  - `modified_dttm`: when the row was loaded.
- **Storage:** Parquet with Snappy compression, partitioned by the date in the file name: `year=YYYY/month=M/day=D`.
- **Run manifest:** each run writes `_manifests/runs/<start>_<end>.json` with rows per table per day.

### Mart: bucket `gs://<prefix>-mart`, Hive database `mart`
All tables are partitioned by **ISO year and ISO week** (`year=2026/week=45`). A run rebuilds every week touching its date range (week-to-date for the current week).
- **Week-on-week (WoW)** compares the same weekdays of the previous week.
- **Year-on-year (YoY)** compares the same ISO week and weekdays one year earlier.

| Table | Grain | Contents |
|---|---|---|
| `fact_sales` | one row per sales line | De-duplicated transactions, enriched with the store, the customer's tier and age band, and the promotion. Adds gross, discount and net amounts, a returns flag, weekday and weekend flag, and customer type (loyalty member or walk-in). |
| `store_weekly_performance` | store × week | Net sales, baskets, customers, units, average basket value, promotion share, returns, sales per sq ft, WoW and YoY change, rank within region |
| `category_brand_weekly` | category × sub-category × brand × week | Units, net sales, share of category, WoW and YoY change |
| `promotion_weekly_effectiveness` | promotion × week | Units and sales on promotion, discount cost, units per day vs the brand's normal units per day, incremental sales, ROI |
| `customer_tier_weekly` | loyalty tier × week | Active customers, baskets, spend per customer, visit frequency, share of sales |
| `company_weekly_kpis` | one row per week | Headline KPIs (stores trading, baskets, net sales, basket value, online share) with WoW and YoY change. Used by the QA thresholds. |

Before a week is marked done, `datalake_to_mart` reconciles the store, category, tier and company totals to the `fact_sales` total. The results go to `_manifests/<year>/W<week>/manifest_<date>.json`.

## Data quality (QA)

Two QA stages each run as their own Airflow DAG. The rules live in [`config/qa_rules.yaml`](config/qa_rules.yaml), and every evaluation is saved to the Hive database `qa`.

| Stage | Check | Rule | On failure |
|---|---|---|---|
| Datalake QA | `row_counts` | Rows in each landing `.csv.gz` = rows in its datalake partition | FAIL: the mart is not built |
| Datalake QA | `pk_uniqueness` | Primary keys unique per daily partition: `(transaction_id, line_no)`, `customer_id`, `store_id`, `promotion_id` | FAIL for customers, stores and promotions. Raw transactions contain the POS re-sends, so they are a WARN up to 1% duplicates and a FAIL above. |
| Datalake QA | `null_keys` | Business keys are never NULL | FAIL |
| Mart QA | `pk_uniqueness` | Primary keys unique in every weekly partition of every mart table; one row per week in `company_weekly_kpis` | FAIL |
| Mart QA | `wow_change` | \|week-on-week % change in net sales\| > **20** | FAIL at company level, WARN for regions and categories |
| Mart QA | `yoy_change` | \|year-on-year % change in net sales\| > **40** | FAIL at company level, WARN for regions and categories |

A failed WoW or YoY check doesn't mean the data is wrong. It means a week moved more than the threshold, and someone should confirm why: a festival, a new store, or a data problem. Weeks with nothing to compare against (the first week, or the first year for YoY) are recorded as `SKIPPED`. `publish_qa_report` writes [`docs/qa/LATEST.md`](docs/qa/LATEST.md) after every run.

## Airflow: four DAGs, triggered one after another

```
fmcg_1_datalake ──trigger──► fmcg_2_datalake_qa ──trigger (only if all checks pass)──► fmcg_3_mart ──trigger──► fmcg_4_mart_qa
```

| DAG | Tasks |
|---|---|
| `fmcg_1_datalake` (daily; processes yesterday) | `ensure_infrastructure` → `land_source_files` → `wait_for_landing_files` (sensor) → `raw_to_datalake` → `trigger_datalake_qa` |
| `fmcg_2_datalake_qa` | `check_row_counts`, `check_pk_uniqueness`, `check_null_keys` → `trigger_mart` (runs only when all three pass) |
| `fmcg_3_mart` | `build_mart_weeks` → `trigger_mart_qa`; `latest_complete_week` (short-circuit) → `weekly_insights` |
| `fmcg_4_mart_qa` | `check_pk_uniqueness`, `check_wow_change`, `check_yoy_change` → `publish_qa_report` (a teardown: it always runs, but doesn't decide the run's colour) |

**Date ranges.** Every DAG works on a date range passed down the chain as `start_date` and `end_date`. The scheduled run of `fmcg_1_datalake` processes the previous day. The three-year history was loaded by one manual trigger:

```bash
airflow dags trigger fmcg_1_datalake --conf '{"start_date": "2023-11-14", "end_date": "2026-11-13"}'
```

For a range, `raw_to_datalake` writes all daily partitions in one Spark job (one Parquet file per partition) and registers them in Hive in batches. `build_mart_weeks` then rebuilds the range week by week. `fmcg_reset` (manual) wipes the local buckets and metastore.

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

Unpause all four `fmcg_*` DAGs; triggered DAGs only run when unpaused. Docker Desktop must be running.

## Jupyter notebook (PySpark + gsutil)

A JupyterLab environment with a ready-made kernel, **FMCG PySpark (Spark 4.0.1, GCS + Hive)**:

```bash
./scripts/notebook.sh      # or: make notebook. Opens JupyterLab in your web browser at http://localhost:8888/lab
```

It runs in any browser (Chrome, Safari, Firefox) on this machine. It listens on `127.0.0.1` only, so no token is needed. The first run also registers the kernel. When the kernel starts it runs [`notebooks/spark_init.py`](notebooks/spark_init.py), so every notebook already has:
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
- what is in each layer (`datalake`, `mart`, `qa`);
- the raw datalake through `spark.table`;
- business questions through `spark.sql(...).show(10, 0)`;
- DataFrames registered as temp views, plus a matplotlib chart;
- proof of partition pruning;
- three years of weekly KPIs with WoW and YoY, plus a chart;
- the QA results;
- browsing the buckets with `gsutil`.

**How `gsutil` reaches the emulator.** `scripts/setup_gcloud_emulator.sh` creates a separate gcloud configuration, `fmcg-emulator`, with the storage endpoint set to `http://localhost:4443` and no login. The notebook selects it only for its own shell commands (`CLOUDSDK_ACTIVE_CONFIG_NAME`), so your default gcloud configuration and login are unchanged. `gsutil` cannot use a custom endpoint itself, so `.gcloud/boto.cfg` makes it run through `gcloud storage`, which can. With `GCS_MODE=real`, the notebook uses your normal gcloud login and project instead.

## Query the data with SQL

The `datalake`, `mart` and `qa` Hive databases can be queried with SQL. Spark SQL reads the table definitions from the Hive metastore and the Parquet files from the `gs://` buckets.

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
make day DATE=2026-11-13                         # one day: land -> datalake -> datalake QA -> mart -> mart QA
make range START=2023-11-14 END=2026-11-13       # the same for a range (the 3-year backfill)
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
make day DATE=2026-11-13
```

> Everything in the "Local" column has been run end to end. The real-GCP path uses the same code and the documented connector settings, but has not been run from this repo yet, because the GCP billing account used during development was closed.

---

## Explained from zero

### The business problem
A retailer selling everyday goods (soap, biscuits, tea, milk) across about 50 stores wants to know every week:
- how sales are moving, week on week and against last year;
- which stores and regions lead;
- which categories and brands are growing;
- whether promotions actually pay for their discounts;
- how loyalty members behave.

The raw data arrives as four separate files a day, with duplicates, returns and anonymous customers. The team also wants to be told automatically when a week looks wrong.

### The pieces

| Piece | Plain-words explanation | Role here |
|---|---|---|
| **Google Cloud Storage (GCS)** | Cloud file storage organised in *buckets*. Cheap, durable, and readable by Spark and BigQuery. | Holds the landing files, the datalake and the mart. |
| **CSV.gz** | A spreadsheet-like text file, compressed with gzip to about a fifth of its size. | The format the retailer sends. |
| **Parquet** | A column-based file format with compression and types built in. Queries read only the columns they need. | The format of the datalake and mart. |
| **Partitioning** | Splitting a table into folders by a column, such as `year=2026/month=11/day=8`. A query for one day reads one folder instead of the whole table. | Datalake by day, mart by week. |
| **PySpark** | Python API for Apache Spark, which processes large data in parallel. | All transformations. |
| **Hive metastore** | A catalogue of table definitions: names, columns, types, partitions and the GCS folder holding the data. Spark, Hive, Trino and Presto all read it. | Makes the buckets look like the databases `datalake` and `mart`. |
| **External table** | A Hive table whose data lives outside Hive (here in GCS). Dropping the table removes only the definition, not the data. | Every table in `datalake`, `mart` and `qa`. |
| **Airflow** | A scheduler that runs tasks in order, waits for files, retries failures and shows everything in a UI. A DAG is one pipeline. | Four DAGs that trigger each other. |
| **Primary key (PK)** | The column or columns that identify a row uniquely, such as `(transaction_id, line_no)`. | Checked for duplicates in both layers. |

### One business day, step by step
1. **Files land.** The day's 4 files appear in `gs://fmcg-landing/<table>/2026/11/08/`.
2. **Sensor.** Airflow waits until all 4 are present.
3. **Raw to datalake.**
   - Spark reads each `.csv.gz` with an explicit schema and casts types.
   - It writes Parquet to `gs://fmcg-datalake/<table>/year=2026/month=11/day=8/` and registers that partition in Hive.
   - Re-running the day replaces only that folder, so the step is **idempotent**.
4. **Datalake QA (DAG 2).** Row counts in landing must equal the datalake, primary keys must be unique, and business keys must not be NULL. Only if all three pass is DAG 3 triggered.
5. **Datalake to mart.**
   - For the ISO week containing the day, Spark de-duplicates POS lines.
   - It picks each customer's, store's and promotion's latest record.
   - It joins them onto each sale, computes amounts and builds the five mart tables in `gs://fmcg-mart/<table>/year=2026/week=45/`.
   - It adds WoW (same days last week) and YoY (same ISO week last year) figures.
   - Every total must reconcile.
6. **Weekly report.** When the range includes a Sunday, Spark writes the insights report for that week.
7. **Mart QA (DAG 4).** Mart primary keys must be unique. Company-level week-on-week moves beyond ±20% and year-on-year moves beyond ±40% fail the check and appear in the QA report.

### Key ideas
- **Why two layers?** The datalake keeps the data close to how it arrived, so it can be audited or reprocessed if business rules change. The mart holds the answers business users need, already cleaned and aggregated.
- **Why partition the mart by week?** Reporting is weekly, so a query for "week 45" reads one folder. Each daily run overwrites only the current week's partition.
- **ISO weeks:** weeks run Monday to Sunday, and week 1 is the week containing the year's first Thursday. Use ISO year and ISO week together, otherwise late-December days get the wrong year.
- **Week-on-week compares the same days:** on a Wednesday, the current week has 3 days of data, so it is compared with Monday to Wednesday of last week, not the full previous week.
- **YoY by ISO week moves with festivals:** Diwali fell in ISO week 45 in 2023, week 44 in 2024, week 43 in 2025 and week 45 in 2026. The festive weeks therefore show huge YoY swings even when the business is healthy. That is why the YoY rule flags them for review rather than treating them as errors. Comparing festival-aligned periods ("Diwali week vs last Diwali week") is the usual fix in reporting.
- **Promotion uplift:** units per day while on promotion, divided by the same brand's units per day without a promotion, minus 1. ROI = extra net sales ÷ discount cost.

---

## Talking about it in an interview

**30-second version.** "An FMCG retailer drops four gzipped CSVs a day: transactions, customers, stores and promotions. They land in a GCS bucket. A PySpark job writes them as typed Parquet into a datalake bucket, partitioned by year, month and day and registered as Hive external tables. A second job builds a weekly mart, partitioned by ISO year and week: enriched sales facts, plus store, category, promotion, loyalty-tier and company KPI tables with week-on-week and year-on-year growth. Airflow runs it as four DAGs that trigger each other: datalake, datalake QA, mart and mart QA. QA checks primary-key uniqueness in both layers and flags weeks where sales moved more than 20% week on week or 40% year on year. I loaded three years of history with one backfill trigger, and the QA report shows exactly which festive weeks breached the thresholds and why."

**Likely follow-up questions**
- *Why external tables?* Data and metadata are decoupled. Several engines can read the same files, and dropping a table never deletes data.
- *How do you make reruns safe?* Each run overwrites one explicit partition folder, then drops and re-adds that partition in the metastore. Nothing else is touched.
- *Why not Spark's dynamic partition overwrite?* It stages output and renames folders. On object storage, renames are copy-and-delete and slow, and with the emulator they failed. Writing one known partition is simpler and predictable.
- *Small files?* Daily partitions are small here. In production, coalesce before writing, and compact old partitions periodically.
- *Late-arriving data?* Today each daily file lands in its own drop-date partition. For late transactions you'd partition by business date and rebuild every affected day and week, driven by the manifest.
- *How would this run on GCP?* Dataproc or Dataproc Serverless for Spark, Dataproc Metastore for Hive, Cloud Composer for Airflow (with `DataprocSubmitJobOperator`), lifecycle rules on the landing bucket, and BigQuery external tables or BigLake over the mart for analysts.
- *Data quality?* Landing vs datalake row counts, primary-key uniqueness, NULL keys, cross-table reconciliation, and WoW/YoY thresholds. Every result goes to Hive tables (`qa.*`), and a failed check stops the next DAG.
- *Why split into four DAGs?* Each stage has its own owner, schedule and failure domain. A failed datalake QA stops the mart from being built on bad data, and QA can be re-run on its own without reloading anything. The date range travels down the chain in the trigger's `conf`.
- *How did you load three years efficiently?* One Spark job per stage for the whole range, writing all daily partitions in one pass (one file per partition) and registering partitions in Hive in batches, instead of 1,096 separate daily runs.
- *Why do the WoW and YoY checks fail on festive weeks?* Festivals move between ISO weeks from year to year, so the thresholds flag them. The fix is a festival calendar: either suppress known events or compare festival-aligned periods.

## Repo layout
```
generator/generate_fmcg_drop.py      synthetic daily drop of 4 .csv.gz files into the landing bucket
common/config.py                     buckets, GCS mode (emulator/real), Spark session with GCS connector + Hive
common/hive_ddl.py                   Hive databases and external tables for datalake and mart
common/hive_io.py                    idempotent partition writers (single partition, or every partition of a range)
common/run_range.py                  --date / --start --end, ISO weeks, same days last year
common/qa.py                         QA rules loader and results saved to Hive
config/qa_rules.yaml                 PK definitions, WoW 20% and YoY 40% thresholds
jobs/raw_to_datalake.py              PySpark: landing -> datalake (year/month/day), any date range
jobs/qa_datalake.py                  PySpark: datalake QA (row counts, PK uniqueness, NULL keys)
jobs/datalake_to_mart.py             PySpark: datalake -> mart (year/week) with WoW/YoY and reconciliation
jobs/qa_mart.py                      PySpark: mart QA (PK uniqueness, WoW ±20%, YoY ±40%)
jobs/weekly_insights.py              PySpark: weekly business report from the mart
scripts/qa_report.py                 QA report (docs/qa/) from the qa Hive tables
scripts/check_landing.py             used by the Airflow sensor
scripts/create_gcp_buckets.sh        real GCP project + buckets (needs billing)
scripts/init_metastore.sh            Hive metastore schema (Apache Hive 2.3 script, like schematool)
airflow/dags/fmcg_1_datalake.py      DAG 1: landing -> datalake (daily, or a range)
airflow/dags/fmcg_2_datalake_qa.py   DAG 2: datalake QA, triggers the mart only if all checks pass
airflow/dags/fmcg_3_mart.py          DAG 3: datalake -> mart + weekly insights
airflow/dags/fmcg_4_mart_qa.py       DAG 4: mart QA + QA report
airflow/dags/_fmcg_common.py         shared DAG settings (date-range params, helpers)
airflow/dags/fmcg_reset.py           wipe local state
docs/insights/                       generated weekly reports
docs/qa/                             generated QA reports
docs/sample_queries.sql              ready-to-run SQL over datalake and mart
notebooks/fmcg_lakehouse_queries.ipynb  PySpark notebook: spark.table / spark.sql / gsutil, with outputs
notebooks/spark_init.py              kernel startup: creates `spark` with GCS + Hive
scripts/install_kernel.sh            registers the "FMCG PySpark" Jupyter kernel
scripts/notebook.sh                  starts JupyterLab on localhost:8888
scripts/setup_gcloud_emulator.sh     gcloud/gsutil -> GCS emulator via a separate gcloud configuration
tests/                               unit tests (no Spark or GCS needed)
```
