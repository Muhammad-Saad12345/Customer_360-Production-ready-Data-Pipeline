# Customer 360 Lakehouse Ingestion & Quality Framework
**Multi-Source Data Ingestion, Silver Quality Gate, and Apache Iceberg Lakehouse on AWS**

---

## 1. Overview

This repository provides a production-grade, ACID-compliant **Customer 360 Data Lakehouse** pipeline. It extracts data across multiple authoritative source systems, lands immutable raw payloads in Amazon S3, validates and quarantines records via a Silver Quality Gate, and promotes clean records into Apache Iceberg tables registered in AWS Glue Data Catalog and queryable via Amazon Athena Engine v3.

### Authoritative Source Ownership Matrix

To eliminate data conflicts, source ownership is strictly enforced across the pipeline:

| Domain Entity | Authoritative Source System | Non-Authoritative / Disallowed | Storage Destination |
| :--- | :--- | :--- | :--- |
| **Contacts / Customers** | **HubSpot CRM** | OMS, PIM | `hubspot_contacts` (PK: `customer_id`) |
| **Orders / Transactions** | **OMS (Order Management)** | HubSpot CRM | `oms_orders` (PK: `order_id`) |
| **Product Catalog** | **PIM** | HubSpot CRM | `pim_products` (PK: `product_id`) |
| **Loyalty & Rewards** | **Loyalty Engine** | HubSpot CRM | `loyalty_loyalty` (PK: `member_id`) |
| **Campaign Events** | **Marketing (Mailchimp)** | HubSpot CRM | `marketing_campaign_events` (PK: `record_id`) |
| **Support Tickets** | **Zendesk** | HubSpot CRM | `support/tickets` (Silver validated) |

> [!IMPORTANT]
> **HubSpot is CONTACTS ONLY**. Any attempt to ingest orders or products through HubSpot is rejected and quarantined.

---

## 2. Architecture & 4-Tier Lifecycle

Every ingestion batch progresses through an end-to-end 4-tier lifecycle with zero stranded raw data:

1. **Tier 1 — Raw Ingestion**: Incremental extraction with persistent watermark checkpointing. Raw JSON written to `s3://{BUCKET}/raw/{source}/{entity}/ingestion_date=.../run_id=.../data.json`.
2. **Tier 2 — Silver Quality Gate**: Validates raw payloads against machine-readable contracts (`src/contracts/data_contracts.py`). Valid records are converted to Snappy Parquet in `s3://{BUCKET}/silver/...`; invalid records are routed to `s3://{BUCKET}/quarantine/...` with failure reasons.
3. **Tier 3 — Quality Gate Reconciliation**: Enforces record count invariants:
   $$\text{input\_records} = \text{valid\_records} + \text{quarantined\_records}$$
4. **Tier 4 — Iceberg Lakehouse**: Deduplicates business keys intra-batch, dynamically handles Glue schema evolution (`ALTER TABLE ADD COLUMNS`), and executes an atomic Athena Engine v3 `MERGE INTO` operation.

---

## 3. How to Run the Project

### Prerequisites
- Python 3.10+ (managed via `uv` or standard virtualenv)
- AWS credentials configured with S3, Glue, and Athena permissions (or set via `.env`)

### Step 1: Start Local Mock Source API Servers
Start the mock microservices (OMS on port 8001, PIM on port 8002, Loyalty on port 8003):
```bash
uv run python scripts/start_mock_sources.py
```

### Step 2: Validate AWS Connectivity
```bash
uv run python src/main.py validate
```

### Step 3: Run Ingestion
You can ingest individual sources or all sources at once:
```bash
# Ingest individual sources
uv run python src/main.py hubspot     # HubSpot CRM contacts ONLY
uv run python src/main.py oms         # OMS orders
uv run python src/main.py pim         # PIM product catalog
uv run python src/main.py loyalty     # Loyalty/rewards ledger
uv run python src/main.py marketing   # Marketing campaign events
uv run python src/main.py support     # Zendesk support tickets

# Or ingest all sources in sequence
uv run python src/main.py all-sources
```

### Step 4: Process Records Through Silver Quality Gate
```bash
uv run python src/main.py silver-gate
```

### Step 5: Load Records into Apache Iceberg Lakehouse
```bash
# Dry-run validation (checks Glue schema, dedup, and DDL generation without mutating tables)
uv run python src/main.py iceberg-load --dry-run

# Live Athena MERGE INTO execution
uv run python src/main.py iceberg-load
```

### Step 6: Run Live Acceptance & Integration Tests
```bash
# Live S3 Acceptance Check
uv run python scripts/live_acceptance_check.py

# Full test suite (142 unit and integration tests)
uv run pytest
```

---

## 4. Running with Docker Compose (Apache Airflow)

To run the complete orchestration environment including Airflow Webserver, Scheduler, and PostgreSQL metadata DB:

```bash
# 1. Start Airflow cluster
docker compose up -d

# 2. Access Airflow UI
# URL: http://localhost:8080
# Username: admin
# Password: admin

# 3. Trigger the 4-tier DAG
# Unpause and trigger: customer360_hourly_ingestion
```

---

## 5. Documentation Directory

- **[AIRFLOW_ARCHITECTURE.md](docs/AIRFLOW_ARCHITECTURE.md)**: Deep dive into DAG topology, task responsibilities, container networking, and AWS MWAA migration.
- **[ICEBERG_LAKEHOUSE.md](docs/ICEBERG_LAKEHOUSE.md)**: Detailed Iceberg lakehouse architecture, Athena Engine v3 inline VALUES MERGE, Glue schema evolution, table schemas, and snapshot maintenance.
- **[AIRFLOW_RUNBOOK.md](docs/AIRFLOW_RUNBOOK.md)**: Operational runbook and troubleshooting procedures.
