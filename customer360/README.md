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

# Full test suite (211 unit and integration tests)
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

# 3. Trigger the DAGs
# Unpause and trigger: customer360_healthcheck, customer360_hourly, or customer360_batch
```

---

## 5. Authoritative Documentation Suite

The complete engineering documentation is organized under the [`docs/`](docs/) directory:

- **[Integration Baseline](docs/integration_baseline.md)**: System baseline, architectural inventory, source ownership matrix, S3 layout, Iceberg tables, and dbt models.
- **[Pipeline Flow Explained](docs/pipeline_flow_explained.md)**: Beginner-to-architect walkthrough of the end-to-end data lifecycle across all 4 tiers and dbt marts.
- **[Architecture & Component Status](docs/architecture_and_component_status.md)**: Subsystem status matrix with `COMPLETE`, `PARTIAL`, `BLOCKED`, and `NOT IMPLEMENTED` ratings.
- **[Incident Root Cause Analysis (RCA)](docs/incident_root_cause_analysis.md)**: Post-mortem of all prior MWAA failures, packaging issues, and code remediations.
- **[AWS Service Health Audit](docs/aws_service_health.md)**: Cloud infrastructure audit covering MWAA, S3, Glue Data Catalog, Athena Engine v3, CloudWatch, and IAM roles.
- **[Production Readiness Assessment](docs/production_readiness.md)**: 12-pillar scorecard, staging approval verdict, and production cutover checklist.
- **[Deployment & Operations Runbook](docs/deployment_runbook.md)**: Operational instructions for S3 synchronization, MWAA deployment, Athena verification queries, and incident triage.
- **[Test Results Report](docs/test_results.md)**: Comprehensive breakdown of the 211 automated tests passing across 15 test suites.
- **[Next Steps Roadmap](docs/next_steps_roadmap.md)**: Prioritized engineering roadmap spanning immediate staging runs to production cutover and BI dashboards.
- **[Engineering Handover](docs/antigravity_handover.md)**: Master handover guide covering repository map, CLI commands, gotchas, and defect history.
