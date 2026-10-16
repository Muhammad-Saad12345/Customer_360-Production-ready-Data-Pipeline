# Customer 360: AWS Step Functions Migration Plan & Architecture

## 1. Executive Summary

This document details the migration of the **Customer 360 Data Lakehouse pipeline** from Apache Airflow on Amazon MWAA to a fully serverless orchestration architecture based on **AWS Step Functions (Standard Workflow)**, **AWS Lambda**, **AWS Glue (Python Shell)**, and **Amazon EventBridge Scheduler**.

### Migration Drivers
- **Cost Reduction:** Amazon MWAA environments incur persistent charges (~$350–$500/month) for running continuous webservers and schedulers even when pipelines run once daily. The new serverless architecture operates strictly on-demand, lowering monthly orchestrator costs to **~$20–$25/month** (a **~94% cost reduction**).
- **Reduced Operational Overhead:** Zero maintenance of Celery workers, Airflow webservers, VPC ENIs, or Airflow provider constraints.
- **Improved Failure Isolation & Visibility:** Native state-by-state execution graphs, automated retries with exponential backoff, and direct Amazon SNS alerting via AWS Step Functions Catch blocks.

---

## 2. Architecture Comparison

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          PREVIOUS ARCHITECTURE (MWAA)                           │
│                                                                                 │
│  Amazon MWAA (Managed Airflow 2.9.2)                                            │
│  ├── Webserver ($0.49/hr)                                                       │
│  ├── Scheduler ($0.49/hr)                                                       │
│  └── Celery Workers (VPC Subnets, NAT Gateways ~$65/mo)                         │
│                                                                                 │
│  Monthly Cost: ~$350 – $500/month                                               │
└─────────────────────────────────────────────────────────────────────────────────┘
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        NEW SERVERLESS ARCHITECTURE                              │
│                                                                                 │
│  [EventBridge Scheduler] (Daily at 02:00 UTC)                                   │
│            │                                                                    │
│            ▼                                                                    │
│  [Step Functions: Standard State Machine]                                       │
│  ├── State 1: Initialize (Lambda)                                               │
│  ├── State 2: Parallel 6-Source Ingest & Validation (Lambdas)                  │
│  │   ├── HubSpot (Contacts)   ── [Ingest] ── [Silver] ── [DQ] ── [Iceberg]     │
│  │   ├── OMS (Orders)         ── [Ingest] ── [Silver] ── [DQ] ── [Iceberg]     │
│  │   ├── PIM (Products)       ── [Ingest] ── [Silver] ── [DQ] ── [Iceberg]     │
│  │   ├── Loyalty (Points)     ── [Ingest] ── [Silver] ── [DQ] ── [Iceberg]     │
│  │   ├── Marketing (Campaign) ── [Ingest] ── [Silver] ── [DQ] ── [Iceberg]     │
│  │   └── Support (Tickets)    ── [Ingest] ── [Silver] ── [DQ] ── [Iceberg]     │
│  ├── State 3: Reconcile Run (Lambda)                                            │
│  ├── State 4: dbt Run (Glue Python Shell, 1/16 DPU, dbt-athena)                │
│  ├── State 5: dbt Test (Glue Python Shell, 1/16 DPU)                            │
│  ├── State 6: Iceberg Maintenance (Lambda: OPTIMIZE & VACUUM via Athena)        │
│  └── Failure Catch: Notify (Lambda -> SNS Alert Topic)                          │
│                                                                                 │
│  Monthly Cost: ~$20 – $25/month                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Component Mapping Matrix

| MWAA / Airflow Component | Step Functions Architecture Equivalent | Execution Model | Resource / Sizing |
| :--- | :--- | :--- | :--- |
| `customer360_batch` DAG | `c360-{env}-batch-orchestrator` | AWS Step Functions (Standard) | Pay-per-state transition |
| `ingest_{source}` tasks | `c360-{env}-ingest` | AWS Lambda (Python 3.11) | 512 MB, 15 min timeout |
| `validate_and_write_silver_{source}` | `c360-{env}-silver_gate` | AWS Lambda (Python 3.11) | 1024 MB, 15 min timeout |
| `quality_check_{source}` | `c360-{env}-quality_check` | AWS Lambda (Python 3.11) | 512 MB, 5 min timeout |
| `load_iceberg_{source}` | `c360-{env}-iceberg_load` | AWS Lambda (Python 3.11) | 1024 MB, 15 min timeout |
| `reconcile_counts` (new) | `c360-{env}-reconcile` | AWS Lambda (Python 3.11) | 256 MB, 5 min timeout |
| `dbt_run` | `c360-{env}-dbt-runner` (`--dbt-action run`) | AWS Glue Python Shell Job | 0.0625 DPU (1/16 DPU), ~$0.04/hr |
| `dbt_test` | `c360-{env}-dbt-runner` (`--dbt-action test`) | AWS Glue Python Shell Job | 0.0625 DPU (1/16 DPU), ~$0.04/hr |
| `iceberg_maintenance` | `c360-{env}-iceberg_maintenance` | AWS Lambda (Python 3.11) | 512 MB, 15 min timeout |
| Airflow Failure Callbacks | `c360-{env}-notify` + SNS | AWS Lambda + SNS Topic | 256 MB, 1 min timeout |
| Airflow Cron Schedule | `c360-{env}-daily-batch` | Amazon EventBridge Scheduler | `cron(0 2 * * ? *)` |

---

## 4. State Machine Payload Contract

To prevent payload bloat (Step Functions 256 KB limit), state transitions pass **compact metadata references** (S3 paths, record counts, execution IDs) rather than raw records.

### Initialization State (`InitializePipeline`)
```json
{
  "run_id": "c360-20261016T020000Z-a1b2c3d4",
  "execution_start": "2026-10-16T02:00:00Z",
  "environment": "staging",
  "sources": ["hubspot", "oms", "pim", "loyalty", "marketing", "support"],
  "dry_run": false
}
```

### Ingestion State (`Ingest_{source}`)
```json
{
  "run_id": "c360-20261016T020000Z-a1b2c3d4",
  "source": "hubspot",
  "entity": "contacts",
  "status": "success",
  "records_ingested": 50,
  "s3_raw_key": "raw/hubspot/contacts/c360-20261016T020000Z-a1b2c3d4/data.json"
}
```

### Silver Gate State (`SilverGate_{source}`)
```json
{
  "run_id": "c360-20261016T020000Z-a1b2c3d4",
  "source": "hubspot",
  "entity": "contacts",
  "input_record_count": 50,
  "valid_count": 48,
  "invalid_count": 2,
  "silver_path": "s3://c360-bucket786/silver/hubspot/contacts/c360-.../data.parquet",
  "quarantine_path": "s3://c360-bucket786/quarantine/hubspot/contacts/c360-.../rejected.json",
  "status": "success"
}
```

### Quality Check State (`QualityCheck_{source}`)
```json
{
  "run_id": "c360-20261016T020000Z-a1b2c3d4",
  "source": "hubspot",
  "entity": "contacts",
  "valid_count": 48,
  "invalid_count": 2,
  "rejection_rate_pct": 4.0,
  "silver_path": "s3://c360-bucket786/silver/hubspot/contacts/c360-.../data.parquet",
  "status": "success"
}
```

### Iceberg Load State (`IcebergLoad_{source}`)
```json
{
  "run_id": "c360-20261016T020000Z-a1b2c3d4",
  "source": "hubspot",
  "entity": "contacts",
  "table_name": "contacts",
  "silver_row_count": 48,
  "deduped_row_count": 48,
  "status": "success"
}
```

---

## 5. Cost Analysis Breakdown

| Component | Usage Assumptions | Estimated Monthly Cost |
| :--- | :--- | :--- |
| **AWS Step Functions** | 1 daily run × 30 days × 35 state transitions | < $0.05 / month |
| **AWS Lambda** | 8 functions × 30 daily runs × ~2-3 min average execution | ~$0.80 / month |
| **AWS Glue (Python Shell)** | 2 jobs (dbt run, dbt test) × 30 days × 5 min @ 0.0625 DPU | ~$0.25 / month |
| **Amazon Athena (Engine v3)** | Data scans for Iceberg MERGE, OPTIMIZE, VACUUM (~10 GB/day) | ~$1.50 / month |
| **Amazon S3 Storage** | Raw, Silver, Iceberg Parquet & metadata (~50 GB) | ~$1.15 / month |
| **Amazon CloudWatch** | Custom metrics, 8 log groups (30-day retention), 1 dashboard | ~$5.00 / month |
| **NAT Gateway & Data Transfer** | Only if private VPC endpoints used; zero if using serverless endpoints | ~$0 - $15 / month |
| **Total Estimated Cost** | **Fully operational daily pipeline** | **~$10 – $25 / month** |
| *Previous MWAA Cost* | *Base MWAA mw1.small + NAT + ENIs* | *$350 – $500 / month* |
