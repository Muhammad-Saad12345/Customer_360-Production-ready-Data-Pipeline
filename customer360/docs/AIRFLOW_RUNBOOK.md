# Customer 360 Airflow Runbook

## A. Overview

This runbook provides complete operational procedures for running, monitoring, testing, and troubleshooting the Apache Airflow orchestration stack for the Customer 360 data pipeline.

The pipeline ingests from six heterogeneous source systems (HubSpot, OMS, PIM, Loyalty, Marketing/Mailchimp, Support/Zendesk), lands raw payloads in AWS S3, validates records against entity schema & DQ contracts via the Silver Gate, partitions valid records into Silver Parquet, and routes invalid records to S3 Quarantine.

---

## B. System Architecture & Prerequisites

- **OS**: Windows 10/11 with Docker Desktop (Linux container engine / WSL2)
- **Runtime**: Python 3.11+ (local venv and Docker base container)
- **Airflow**: Apache Airflow 2.9.3 running in Docker Compose with LocalExecutor & PostgreSQL 15
- **Storage**: AWS S3 Bucket (e.g., `c360-bucket786`)
- **Networking**: `host.docker.internal:host-gateway` bridge mapping container requests to host-bound mock services (ports 8001, 8002, 8003)

---

## C. Environment Setup & Configuration

1. **Configure `.env`**:
   Ensure `.env` in the project root (`customer360/.env`) contains active credentials and configuration:
   ```env
   AWS_REGION=us-east-1
   S3_BUCKET=c360-bucket786
   AWS_ACCESS_KEY_ID=AKIA...
   AWS_SECRET_ACCESS_KEY=...
   OMS_BASE_URL=http://localhost:8001
   PIM_BASE_URL=http://localhost:8002
   LOYALTY_BASE_URL=http://localhost:8003
   LOG_LEVEL=INFO
   ```
   *(Note: Inside Docker Compose, `OMS_BASE_URL`, `PIM_BASE_URL`, and `LOYALTY_BASE_URL` are automatically overridden to `http://host.docker.internal:800x`.)*

2. **Python Dependencies**:
   Ensure `pyarrow>=14.0.0` is present in `requirements.txt` alongside `apache-airflow==2.9.3`, `fastapi`, `uvicorn`, `boto3`, `pydantic`.

---

## D. Starting Mock Services (Host)

If live external APIs are not connected, launch the mock source services on the host machine:

```powershell
# OMS (Port 8001)
.venv\Scripts\python.exe -m uvicorn mock_sources.oms.main:app --host 0.0.0.0 --port 8001

# PIM (Port 8002)
.venv\Scripts\python.exe -m uvicorn mock_sources.pim.main:app --host 0.0.0.0 --port 8002

# Loyalty (Port 8003)
.venv\Scripts\python.exe -m uvicorn mock_sources.loyalty.main:app --host 0.0.0.0 --port 8003
```

Verify mock service health from host or container:
```powershell
curl http://localhost:8001/health  # Returns: {"status":"ok","service":"mock-oms"}
curl http://localhost:8002/health  # Returns: {"status":"ok","service":"mock-pim"}
curl http://localhost:8003/health  # Returns: {"status":"ok","service":"mock-loyalty"}
```

---

## E. Starting Airflow with Docker Compose

1. **Launch the Compose Stack**:
   ```powershell
   cd "D:\Data Engineer Projects\Customer 360\customer360"
   docker compose up -d --build
   ```

2. **Verify Containers**:
   ```powershell
   docker compose ps
   ```
   Expected running containers:
   - `customer360-postgres-1` (Up, healthy on port 5432)
   - `customer360-airflow-webserver-1` (Up on port 8080)
   - `customer360-airflow-scheduler-1` (Up)

3. **Check Container Logs**:
   ```powershell
   docker compose logs -f airflow-webserver
   docker compose logs -f airflow-scheduler
   ```

---

## F. Airflow Web UI & CLI Operations

- **Web UI**: Access `http://localhost:8080`
- **Default Credentials**: `admin` / `admin`

### CLI Operational Commands

1. **Check DAG Discovery & Import Errors**:
   ```powershell
   docker compose exec airflow-scheduler airflow dags list-import-errors
   # Expected output: No data found (0 errors)

   docker compose exec airflow-scheduler airflow dags list
   # Expected: customer360_batch listed with owner data-engineering
   ```

2. **Unpause DAG**:
   ```powershell
   docker compose exec airflow-scheduler airflow dags unpause customer360_batch
   ```

3. **Trigger DAG**:
   ```powershell
   docker compose exec airflow-scheduler airflow dags trigger customer360_batch
   ```

4. **Monitor DAG Runs**:
   ```powershell
   docker compose exec airflow-scheduler airflow dags list-runs -d customer360_batch
   ```

5. **Inspect Task Instance States**:
   ```powershell
   docker compose exec airflow-scheduler airflow tasks states-for-dag-run customer360_batch <RUN_ID>
   ```
   All 27 tasks should show `state: success`:

   **Tier 1-3 (per source: ingest → silver → quality):**
   - `ingest_hubspot` >> `validate_and_write_silver_hubspot` >> `quality_check_hubspot`
   - `ingest_oms` >> `validate_and_write_silver_oms` >> `quality_check_oms`
   - `ingest_pim` >> `validate_and_write_silver_pim` >> `quality_check_pim`
   - `ingest_loyalty` >> `validate_and_write_silver_loyalty` >> `quality_check_loyalty`
   - `ingest_marketing` >> `validate_and_write_silver_marketing` >> `quality_check_marketing`
   - `ingest_zendesk` >> `validate_and_write_silver_zendesk` >> `quality_check_zendesk`

   **Tier 4 (Iceberg load — per source):**
   - `load_iceberg_hubspot`, `load_iceberg_oms`, `load_iceberg_pim`
   - `load_iceberg_loyalty`, `load_iceberg_marketing`, `load_iceberg_zendesk`

   **Tier 5 (dbt — fan-in from all 6 Iceberg loads):**
   - `dbt_run` >> `dbt_test`

   **Tier 6 (Iceberg maintenance — compaction + vacuum):**
   - `iceberg_maintenance`

---

## G. Verifying S3 Outputs

Verify that raw payloads, silver parquet, quarantine records, and metadata files exist in S3:

1. **Raw Landing Zone**:
   - `raw/hubspot/contacts/ingestion_date={YYYY-MM-DD}/run_id={UUID}/data.json`
   - `raw/oms/orders/ingestion_date={YYYY-MM-DD}/run_id={UUID}/data.json`
   - `raw/pim/products/ingestion_date={YYYY-MM-DD}/run_id={UUID}/data.json`
   - `raw/loyalty/loyalty/ingestion_date={YYYY-MM-DD}/run_id={UUID}/data.json`
   - `raw/support/tickets/ingestion_date={YYYY-MM-DD}/run_id={UUID}/data.json`

2. **Silver Parquet Layer**:
   - `silver/hubspot/contacts/ingestion_date={YYYY-MM-DD}/run_id={UUID}/data.parquet`
   - `silver/oms/orders/ingestion_date={YYYY-MM-DD}/run_id={UUID}/data.parquet`
   - `silver/pim/products/ingestion_date={YYYY-MM-DD}/run_id={UUID}/data.parquet`
   - `silver/loyalty/loyalty/ingestion_date={YYYY-MM-DD}/run_id={UUID}/data.parquet`
   - `silver/marketing/campaign_events/ingestion_date={YYYY-MM-DD}/run_id={UUID}/data.parquet`

3. **Quarantine Zone**:
   - `quarantine/pim/products/ingestion_date={YYYY-MM-DD}/run_id={UUID}/invalid_records.json`
   - `quarantine/loyalty/loyalty/ingestion_date={YYYY-MM-DD}/run_id={UUID}/invalid_records.json`
   - `quarantine/oms/orders/ingestion_date={YYYY-MM-DD}/run_id={UUID}/invalid_records.json`

4. **Audit Metadata**:
   - `metadata/<source>/<entity>/ingestion_date={YYYY-MM-DD}/run_id={UUID}/metadata.json`

---

## H. Controlled Failure & Retry Testing

### Testing Ingestion Failure & Downstream Task Block
To verify that downstream silver tasks are blocked when an ingestion task fails:
```powershell
docker compose exec -e OMS_BASE_URL=http://invalid-host:9999 airflow-scheduler airflow tasks test customer360_batch ingest_oms 2026-09-23
```
- **Observed Behavior**: The task catches `ConnectionError`, Airflow marks the task for retry (`retries=2, retry_delay=2m`).
- **Downstream Protection**: `validate_and_write_silver_oms` does not run.
- **Parallel Independence**: Ingestion and validation for HubSpot, PIM, Loyalty, Marketing, and Zendesk continue unaffected.

---

## I. Idempotency & Rerun Verification

- Rerunning the DAG with the same checkpoint watermark reads from the watermark boundary minus lookback window without duplicating records.
- Silver Gate parquet writes use partitioned S3 keys (`ingestion_date=.../run_id=.../data.parquet`) and record-level primary key deduplication, ensuring that re-running identical runs produces idempotent results without corrupted or duplicated entities.

---

## J. Troubleshooting Guide

| Issue | Root Cause | Resolution |
| :--- | :--- | :--- |
| **Airflow Scheduler exits immediately (code 1)** | Database initialization race condition before webserver finishes `db init` | Database has now completed initialization. Run `docker compose up -d airflow-scheduler`. Containers have `restart: on-failure` configured. |
| **DAGs not discovered / "No data found"** | `AIRFLOW_HOME` was defaulting to `/root/airflow` while volumes were at `/opt/airflow` | Configured `AIRFLOW_HOME: /opt/airflow` and `AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags` in `docker-compose.yaml`. |
| **PyArrow missing in container** | Ingestion container image did not install `pyarrow` | Added `pyarrow>=14.0.0` to `requirements.txt` and rebuilt container. |
| **Mock sources unreachable from container** | Container attempting to reach host `localhost:800x` | Use `extra_hosts: host.docker.internal:host-gateway` and set `*_BASE_URL=http://host.docker.internal:800x` in Compose. |
| **Zendesk auth failure** | External Zendesk integration authentication boundary | Zendesk ingestion gracefully falls back to synthetic test tickets (`source_type: synthetic`) with quality check warning note. |
| **S3 NoCredentialsError** | `.env` not mounted or AWS keys missing | Verify `.env` is mounted to `/opt/airflow/.env:ro` and contains valid IAM credentials with S3 Read/Write permissions. |

---

## K. Stopping & Cleaning Up

```powershell
# Stop Airflow stack
docker compose down

# Stop Airflow and remove Postgres metadata volume (destructive clean slate)
docker compose down -v
```

---

## L. Pipeline Health API

The project includes a FastAPI-based health monitoring service.

### Starting the Health API

```powershell
.venv\Scripts\python.exe -m uvicorn src.api.health_api:app --host 0.0.0.0 --port 8360
```

### Key Endpoints

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/` | GET | Service info and uptime check |
| `/health` | GET | Pipeline health summary (overall status, per-source aggregates) |
| `/health/detail` | GET | Full health report with per-run detail |
| `/runs` | GET | List run results (filterable by `?source=` and `?tier=`) |
| `/runs` | POST | Record a run result (called by Airflow callbacks) |
| `/runs/history` | GET | Load historical results from JSONL log |
| `/sources` | GET | List all 6 active source systems |

### Monitoring Callbacks

The batch DAG uses `on_success_callback` and `on_failure_callback` in its `default_args` to automatically record every task's result to `logs/pipeline_run_log.jsonl`. These results are served by the health API.

---

## M. dbt Operations

dbt models transform the Iceberg tables into the Customer 360 analytical mart.

### Running dbt Manually

```powershell
# Using the dbt virtual environment
dbt_venv\Scripts\dbt run --project-dir dbt --profiles-dir dbt --target dev

# Run dbt schema tests
dbt_venv\Scripts\dbt test --project-dir dbt --profiles-dir dbt --target dev
```

### dbt Model Layers

| Layer | Models | Purpose |
| :--- | :--- | :--- |
| `staging/` | `stg_hubspot_contacts`, `stg_oms_orders`, `stg_pim_products`, `stg_loyalty`, `stg_marketing_campaign_events`, `stg_zendesk_tickets` | Type-cast and normalize Iceberg source tables |
| `identity/` | `bridge_customer_identity` | Deterministic identity resolution (email, phone, authoritative ID) |
| `dimensions/` | `dim_customer`, `dim_product`, `dim_campaign`, `dim_date` | Conformed dimensions with survivorship rules |
| `facts/` | `fact_order`, `fact_support_case`, `fact_campaign_engagement`, `fact_loyalty_activity` | Conformed transactional facts |
| `marts/` | `customer_360`, `mart_customer_metrics`, `mart_executive_kpis` | Analytical 360° view, cohort metrics, executive KPIs |

### Schema Tests

All models have `not_null`, `unique`, `accepted_values`, and `relationships` tests defined in `dbt/models/schema.yml`.

---

## N. Iceberg Maintenance

After dbt completes, the `iceberg_maintenance` task runs `OPTIMIZE` (compaction) and `VACUUM` (cleanup) on all 6 Iceberg tables via Athena. This reduces small-file overhead and removes orphaned data files.

### Manual Maintenance

```powershell
.venv\Scripts\python.exe -c "
from src.lakehouse.iceberg_maintenance import IcebergMaintenanceManager
mgr = IcebergMaintenanceManager()
for t in ['contacts','orders','products','loyalty','campaign_events','support_tickets']:
    print(mgr.run_full_maintenance(t))
"
```

