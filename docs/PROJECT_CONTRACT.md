# Project Contract: Sanlam Insurance Data Pipeline

Reviewed for GitHub publication on 10 November 2026. This document consolidates the architecture, interfaces, execution requirements, and boundaries. The code defines current behavior; the report and retained local history provide development context.

## 1. Purpose and scope

Prepare coherent insurance reporting datasets from a synthetic Oracle source, preserve extracted changes in a local data lake, validate and transform them with Spark, and automate the workflow through Airflow. Tableau consumes local CSV files.

The project demonstrates data engineering and business intelligence. It does not reproduce a verified map of Sanlam's internal infrastructure. Records come from Faker and controlled random generation; no real customer extract is included. Geographic conventions, amounts, risk scores, and fraud flags are demonstration assumptions. No currency or predictive model is defined.

## 2. Responsibilities and deployment boundaries

| Component | Implemented responsibility | Location / interface |
|---|---|---|
| Oracle | Business tables and ingestion progress | External database; `python-oracledb` |
| Source generator | Schema creation and initial/incremental activity | `src/seed_insurance_oracle.py` |
| Python / Pandas / Boto3 | Extract changed rows; serialize and upload Parquet | Airflow worker; Oracle and MinIO APIs |
| MinIO | Raw, Silver, and Gold storage | Separate container; S3 API 9000, console 9001 |
| PySpark | Consolidation, validation, aggregation, and export selection | Airflow worker JVM with Java 17 |
| Hadoop-AWS / AWS SDK | Spark S3A access to MinIO | Java libraries, not a Hadoop cluster or AWS deployment |
| Airflow | Schedule, dependencies, task state, retries | Docker Compose; API/UI 8080 |
| PostgreSQL | Airflow metadata and Celery results | Compose service; not the business source |
| Redis | Celery broker | Compose service |
| Tableau | Worksheets and dashboards over exports | External desktop application; manual refresh |

Compose supplies the API server, scheduler, DAG processor, worker, triggerer, initialization service, and optional CLI/Flower profiles. Project files mount at `/opt/sanlam`; Airflow runtime directories use host mounts. PostgreSQL uses a named volume. MinIO uses its separate `minio-data` volume. Redis has no named volume in this configuration.

The image declares Airflow 3.3.1, PySpark 4.0.1, Hadoop-AWS 3.4.1, and AWS SDK bundle 2.24.6. The Dockerfile assumes a Python 3.13 package directory when copying JARs. These are existing declared versions, not a claim that publication rebuilt the image.

## 3. Source model

| Table | Primary key | Relationship |
|---|---|---|
| `CUSTOMERS` | `customer_id` | Parent of policies |
| `AGENTS` | `agent_id` | Optional selling agent for a policy |
| `INSURANCE_PRODUCTS` | `product_id` | Product referenced by policies |
| `POLICIES` | `policy_id` | Customer, product, and optional agent |
| `POLICY_COVERAGES` | `policy_coverage_id` | Many coverages per policy |
| `POLICY_STATUS_HISTORY` | `policy_status_history_id` | Many lifecycle events per policy |
| `PREMIUM_TRANSACTIONS` | `premium_transaction_id` | Many premium transactions per policy |
| `CLAIMS` | `claim_id` | Many claims per policy |
| `CLAIM_PAYMENTS` | `claim_payment_id` | Many payment records per claim |

Business DDL lives in the source generator: identity keys, selected unique/check constraints, foreign keys, and `LAST_UPDATED`. Its watermark-index loop covers eight tables and omits status history. `sql/SANLAM.sql` initializes the separate control table; it is initial setup/inspection SQL, not a complete schema migration.

`ETL_METADATA(TABLE_NAME VARCHAR2(100) PRIMARY KEY, LAST_WATERMARK TIMESTAMP)` must contain one row per business table. Initial timestamps of `1900-01-01 00:00:00` let the first extraction read the initial population.

## 4. Raw ingestion contract

`src/pipeline_jobs.py::ingest_table()` accepts only the nine allowlisted tables. It reads that table's watermark, binds its value in `LAST_UPDATED > :watermark`, and holds the result in Pandas. Empty results return zero without creating an object.

Non-empty batches are serialized to Parquet in memory and uploaded to:

```text
s3://<bucket>/raw/<UPPERCASE_TABLE>/load_date=YYYY-MM-DD/<TABLE>_<UTC_TIMESTAMP>.parquet
```

After upload succeeds, the maximum extracted `LAST_UPDATED` is committed to Oracle. `load_date` represents UTC extraction date, not a business event date.

| Failure | Recovery behavior |
|---|---|
| Query, serialization, or upload fails | Watermark remains unchanged |
| Upload succeeds but watermark commit fails | Raw object can remain; retry can create duplicate versions |
| One table fails after others succeed | Successful table watermarks remain advanced; Silver waits |
| Silver fails | Retained Raw batches can be reused for a downstream retry |

Oracle and MinIO have no shared transaction. The historical “atomically ingest” docstring must not be interpreted as exactly-once delivery. There is no overlap window, delete capture, composite cursor, or shared cross-table snapshot. A late-visible change with an equal or older timestamp can be missed.

## 5. Silver contract

`run_silver()` reads all accumulated Raw history for each table. Names become lowercase; configured dates and decimal amounts are cast. A window keeps one row per primary key, ordered by descending `last_updated`, extraction date, and source filename. Exact ties within the same file have no additional value-level ordering.

Checks reject null/duplicate primary keys, selected invalid enums, orphan non-null foreign keys, inconsistent policy cancellation/expiry dates, nonpositive policy/premium amounts, loss dates after reporting dates, excessive approved claim amounts, missing required approvals/closure dates, and paid premiums without transaction dates.

Missing email remains null with a derived `has_email` flag. Initial null status becomes `NEW_POLICY`. Rejected claims retain null approved amounts. Closure dates are required for `SETTLED`/`CLOSED`, rather than all rejected claims.

All Silver checks precede publication. Tables are overwritten independently as Snappy Parquet at `silver/<lowercase_table>/`. Temporary `_source_file` is dropped; `load_date` remains. `changed_at` becomes a date and loses intraday precision. Some validations allow nulls; they are not a complete schema contract for arbitrary external sources.

## 6. Gold and reporting contracts

| Dataset | Unique grain key | CSV columns | Business interpretation |
|---|---|---:|---|
| `customer_360` | `customer_id` | 16 | Profile, policy/claim counts, paid claim totals |
| `policy_performance` | `policy_id` | 32 | Policy context and preaggregated child metrics |
| `claims_performance` | `claim_id` | 24 | Claim context and paid payment summary |
| `premium_collection` | `due_month` | 11 | Monthly premium statuses and collection ratios |
| `policy_status_history` | `policy_status_history_id` | 16 | Status event, elapsed days, lapse/cancel indicator |

Payments are filtered to `PAID` and aggregated by claim before joining. Claims, payments, coverages, and premiums are reduced to the relevant policy grain before combining them. This prevents unrelated one-to-many joins from multiplying amounts. Each Gold output is checked for duplicate grain keys before its individual overwrite at `gold/<dataset>/`.

Metric meanings:

- `total_paid_amount` and `number_of_payments` in customer/policy outputs concern claim payments. Collected premiums use the separate `paid_total_amount` premium field.
- Policy premium totals sum contractual fields across frequencies; they are not normalized annual revenue or cash collected.
- Coverage totals include inactive rows in the current implementation.
- Collection count ratio is `PAID_count / (PAID_count + FAILED_count + PENDING_count + REVERSED_count)`; amount ratio uses equivalent sums. Zero denominators yield null. Ratios are fractions.
- Collection groups by due month; transaction types are not netted as a signed accounting ledger.
- Age uses the processing date, so unchanged records can produce different ages on a later rebuild.
- Status history retains event grain; `is_lapse_or_cancel` identifies `LAPSED`/`CANCELLED` transitions.

`GOLD_EXPORT_COLUMNS` in `src/pipeline_jobs.py` defines every required CSV field and its order. `export_tableau()` normalizes pivot names such as `PAID_count` to lowercase, rejects missing columns, selects by name, collects each dataset through `toPandas()`, and writes UTF-8 without an index to `tableau_exports/<dataset>.csv`. Generated files stay outside Git.

The exporter writes directly to final filenames. Silver, Gold, and CSV publication are sequential, without shared version promotion or atomic snapshots. Extraction/export require enough memory for each full batch/dataset.

## 7. Orchestration contract

```text
seed_incremental_source (--mode incremental --batch-size 400)
    -> nine independent ingest_<table> tasks
    -> transform_raw_to_silver
    -> transform_silver_to_gold
    -> export_gold_for_tableau
```

The DAG has 13 statically generated tasks, a four-day interval, a start date of 17 August 2026, `catchup=False`, `max_active_runs=1`, and one retry after five minutes. Parallel ingestion depends on worker capacity. Application imports are deferred until execution.

The seed parameter scales several kinds of activity; it does not mean exactly 400 inserted rows. Retrying/retriggering the seed can generate more changes. Jobs use current source watermarks rather than Airflow data intervals; changing a logical date does not reconstruct a historical snapshot.

## Setup

### 1. Prerequisites and configuration

Use Docker Desktop with Compose, an accessible Oracle schema with table/index creation privileges, and Tableau Desktop to inspect the workbook. The Docker worker is the reference runtime. Host Python is needed for source initialization. Java 17 is needed if running Spark locally.

From the root, copy `.env.example` to `.env` and fill in Oracle, MinIO, and Airflow values. Use a host Oracle address such as `localhost:1521/XEPDB1` for host commands. Compose overrides Oracle to `host.docker.internal:1521/XEPDB1` and MinIO to `http://host.docker.internal:9000`; adapt those explicit settings for another service/platform. On Linux, supply host-gateway routing or reachable alternative addresses.

Use `http://localhost:9000` for host-side MinIO. Choose your own access/secret keys, bucket, Airflow admin password, Fernet key, and JWT secret. Existing MinIO credentials must match the running server; editing client `.env` alone does not change server credentials.

For example, generate a valid Fernet key using the image and put the result in the local `.env`:

```powershell
docker run --rm --entrypoint python apache/airflow:3.3.1 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Generate a separate JWT secret, for example with `python -c "import secrets; print(secrets.token_urlsafe(48))"`. Keep real values outside Git. Compose loads root `.env` into containers; `--env-file ../.env` supplies Compose interpolation. A second `airflow/.env` is no longer required.

### 2. Initialize a dedicated demo Oracle schema

For a new, empty schema, run from the root:

```powershell
Copy-Item .env.example .env
# Fill in .env before running the connection or generator.
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe src/oracle_db_connection.py
.\.venv\Scripts\python.exe src/seed_insurance_oracle.py --mode initial --customers 500
```

Run `sql/SANLAM.sql` once in your Oracle SQL client to initialize `ETL_METADATA` and its nine rows. The generator creates business tables but not this control table. For an existing project, inspect tables and metadata instead of reinitializing them.

Optional `--reset` drops business tables without clearing watermarks or lake history. It is not ordinary setup/recovery; mixing reused keys with old Raw history can invalidate a demo.

### 3. Start MinIO and create the bucket

If the project's preserved MinIO container exists, start it with its existing credentials. For a new installation, set these process variables to the same credentials chosen for `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY`, then run:

```powershell
$env:MINIO_ROOT_USER = "<your MINIO_ACCESS_KEY>"
$env:MINIO_ROOT_PASSWORD = "<your MINIO_SECRET_KEY>"
docker run -d --name minio -p 9000:9000 -p 9001:9001 -e MINIO_ROOT_USER -e MINIO_ROOT_PASSWORD -v minio-data:/data quay.io/minio/minio server /data --console-address ":9001"
```

Open `http://localhost:9001` and create the bucket named by `MINIO_BUCKET` (example: `sanlam-data-lake`). MinIO and bucket initialization are separate from Airflow Compose.

### 4. Start and check Airflow

```powershell
Set-Location airflow
docker compose --env-file ../.env up -d --build
docker compose --env-file ../.env ps
docker compose --env-file ../.env exec airflow-scheduler airflow dags list-import-errors
```

Open `http://localhost:8080`, use the configured admin login, enable the DAG, and trigger it once. The seed task changes the demo source. Silver needs readable Parquet in every expected Raw prefix; there is no empty-table initialization policy.

Inspect logs with `docker compose --env-file ../.env logs airflow-worker` or the equivalent scheduler command. Rebuild after dependency/image changes. Mounted Python edits ordinarily do not require rebuilding.

### 5. Validate and connect Tableau

Confirm task completion; inspect schemas, primary/grain keys, and reconciled totals. Canonical CSV names are the five Gold names above. Customer policy/claim counts and claim amounts should agree with the corresponding policy/claim exports.

`dashboard.twb` contains 17 worksheets and four dashboards. It retains development-machine paths, temporary Hyper extract references, and `policy_performance_fresh.csv` / `premium_collection_fresh.csv` connections. Reconnect/replace connections with freshly generated canonical CSVs in Tableau, rebuild/refresh extracts as necessary, and verify dependent sheets remain intact. Display names can retain `_fresh`; names alone do not establish freshness. Do not close/delete sources to migrate them.

The workbook is not an embedded data package. No CSVs or Hyper extracts are published. Refresh is manual; there is no Tableau Cloud/Server refresh integration.

## 8. Notebooks and compatibility

The four notebooks preserve the original ingestion, Silver, Gold, and export development path. Saved outputs and execution counts are cleared for publication, with original versions backed up locally. `src/pipeline_jobs.py` is the scheduled reference implementation.

Known differences:

- Local Spark fallback requests Hadoop-AWS 3.3.4 / SDK v1 1.12.262 from the historical Spark 3.5 environment. Installing declared PySpark 4.0.1 locally does not align that fallback. Prefer container transformations until local dependencies are aligned.
- The Gold notebook contains count-star pivots that differ from operational identifier counts and can fail under Spark 4.
- Notebook 04 does not enforce `GOLD_EXPORT_COLUMNS` or normalize every pivot name. Its destination depends on the working directory, while `src` imports need the root on the Python path. Resolve these differences before using it to replace canonical exports.
- Some introductory text predates publishing cells. Running notebooks can write lake tables; they are not read-only examples.

## 9. Operations, evidence, and next improvements

After an ingestion failure, retry failed ingestions and downstream stages. After Silver/Gold/export failures, retry from the failed stage once its cause is fixed. Reuse completed upstream work where appropriate; rerunning generation adds changes. Stopped MinIO, mismatched credentials, missing S3A libraries, or incompatible Java dependencies can fail tasks independently of valid Python syntax.

The report contains historical execution/dashboard evidence. Local canonical CSVs were previously checked for headers, grains, and aggregate consistency. Publication checks inspect current source syntax, notebook structure, workbook XML, canonical export contracts, and the Git file set. They do not establish a new successful Oracle/MinIO/Airflow/Tableau execution or production capacity.

Priority improvements are late-change/delete capture, batch/snapshot boundaries, versioned atomic publication, idempotent source simulation, persisted manifests/quality results, regression/integration tests, aligned runtimes, and governed Tableau refresh. CI/CD, managed secrets, streaming, a fraud model, and cloud deployment are not implemented.

## 10. Publication boundary

Publish reusable code, notebooks without outputs, orchestration/build files, metadata SQL, environment template, README, this contract, Tableau definition, and only `output/pdf/sanlam_project_report_25_26.pdf`.

The `.gitignore` allowlist excludes other root files and limits included directories. Other docs, report sources/assets/research, older PDFs, handoff/history notes, temporary material, local environments, extracts, logs, configuration state, and recovery files remain on disk. `.dockerignore` limits image build context to the required dependency file.

The report includes third-party illustrations and source credits. Names, logos, and those assets retain their respective ownership. No new repository-wide license is assigned.
