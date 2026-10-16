# Customer 360 Step Functions Pipeline Runbook

## 1. Pipeline Overview

The Customer 360 Lakehouse pipeline orchestrates data ingestion from 6 sources, validates records against schemas, routes bad records to quarantine, loads valid data into Apache Iceberg tables via Athena MERGE queries, executes dbt dimensional transformations, and runs Iceberg compaction and snapshot expiration.

---

## 2. Triggering Pipeline Executions

### Method A: Manual Trigger via AWS CLI (CloudShell or Local)

#### Full Production Run
```bash
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:734143568343:stateMachine:c360-staging-batch-orchestrator" \
  --name "manual-run-$(date +%s)" \
  --input '{"environment": "staging", "dry_run": false}'
```

#### Safe Dry-Run (Validates contracts, skips DB writes)
```bash
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:734143568343:stateMachine:c360-staging-batch-orchestrator" \
  --name "dry-run-$(date +%s)" \
  --input '{"environment": "staging", "dry_run": true}'
```

#### Selective Source Execution (e.g. HubSpot and OMS only)
```bash
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:734143568343:stateMachine:c360-staging-batch-orchestrator" \
  --name "partial-run-$(date +%s)" \
  --input '{"environment": "staging", "sources": ["hubspot", "oms"], "dry_run": false}'
```

### Method B: Manual Trigger via AWS Management Console
1. Navigate to **AWS Step Functions** in `us-east-1`.
2. Select **State machines** > `c360-staging-batch-orchestrator` (or `c360-prod-batch-orchestrator`).
3. Click **Start execution**.
4. Enter execution payload:
   ```json
   {
     "environment": "staging",
     "dry_run": false
   }
   ```
5. Click **Start execution** to view live visual execution.

---

## 3. Monitoring & Observability

### CloudWatch Log Groups

| Component | CloudWatch Log Group | Retention |
| :--- | :--- | :--- |
| **State Machine History** | `/aws/vendedlogs/states/c360-staging-batch-orchestrator` | 30 days |
| **Initialize Lambda** | `/aws/lambda/c360-staging-initialize` | 30 days |
| **Ingest Lambda** | `/aws/lambda/c360-staging-ingest` | 30 days |
| **Silver Gate Lambda** | `/aws/lambda/c360-staging-silver_gate` | 30 days |
| **Quality Check Lambda** | `/aws/lambda/c360-staging-quality_check` | 30 days |
| **Iceberg Load Lambda** | `/aws/lambda/c360-staging-iceberg_load` | 30 days |
| **Reconcile Lambda** | `/aws/lambda/c360-staging-reconcile` | 30 days |
| **Notify Lambda** | `/aws/lambda/c360-staging-notify` | 30 days |
| **Iceberg Maintenance Lambda** | `/aws/lambda/c360-staging-iceberg_maintenance` | 30 days |
| **dbt Runner (Glue Job)** | `/aws-glue/python-jobs/output` and `/aws-glue/python-jobs/error` | 30 days |

### Real-Time Metric Alarms

1. **`c360-staging-dq-gate-failed`**
   - Trigger: `DQGateFailed >= 1` in 5 minutes
   - Impact: Iceberg promotion blocked due to rejection rate exceeding hard threshold (10%).
   - Action: Inspect `/customer360/quarantine/` in S3 to review rejected JSON records.

2. **`c360-staging-task-failures`**
   - Trigger: `TaskFailure >= 1` in 5 minutes
   - Impact: An ingestion or processing task failed.
   - Action: Open failed execution in Step Functions console to inspect state error message.

3. **`c360-staging-high-rejection-rate`**
   - Trigger: `RejectionRate >= 10%` in 5 minutes
   - Impact: Upstream schema contract change or anomalous source data payload.

---

## 4. Troubleshooting Guide

### Failure Case 1: Ingest Timeout or API Error
- **Symptom:** State `Ingest_{source}` fails with `States.TaskFailed`.
- **Cause:** Upstream API (HubSpot, OMS mock, etc.) returned 5xx, 429, or took longer than 15 minutes.
- **Resolution:**
  1. Inspect `/aws/lambda/c360-staging-ingest` CloudWatch logs.
  2. Verify Secrets Manager API token: `aws secretsmanager get-secret-value --secret-id customer360/staging/hubspot`
  3. If rate-limited, wait for rate limit window to expire and retry the execution.

### Failure Case 2: Quality Gate Blocked (Rejection Rate >= 10%)
- **Symptom:** State `QualityCheck_{source}` fails with `rejection rate X% >= hard threshold 10.0%`.
- **Cause:** Schema validation rejected >10% of records; records quarantined.
- **Resolution:**
  1. Review quarantine file:
     ```bash
     aws s3 cp s3://c360-bucket786/quarantine/{source}/{entity}/{run_id}/rejected.json - | head -n 20
     ```
  2. Inspect rejection reasons in the JSON (e.g. missing required field, type mismatch).
  3. If schema evolved upstream, update Pydantic models in `src/models/` and deploy.

### Failure Case 3: Athena Iceberg MERGE Error
- **Symptom:** State `IcebergLoad_{source}` fails during Athena query execution.
- **Cause:** Athena engine lock, DDL syntax error, or Glue Data Catalog partition discrepancy.
- **Resolution:**
  1. Check Athena Query History in AWS Console (`customer360-staging` workgroup).
  2. Locate the failed query ID and view the detailed error message.
  3. Verify Iceberg table metadata in Glue Catalog:
     ```bash
     aws glue get-table --database-name customer360_iceberg_stage --name contacts
     ```

### Failure Case 4: dbt Runner Failure
- **Symptom:** State `DbtRun` or `DbtTest` fails with non-zero exit code.
- **Cause:** dbt SQL syntax error, schema test assertion failure (e.g. duplicate customer_id).
- **Resolution:**
  1. View Glue Job run logs in CloudWatch under `/aws-glue/python-jobs/error`.
  2. Identify the specific failing model or dbt test.
  3. If a test assertion failed (e.g. uniqueness), query the table via Athena to inspect the offending records.

---

## 5. Automated Schedulers

The pipeline triggers automatically every day at **02:00 UTC** via Amazon EventBridge Scheduler.

- **Schedule Name:** `c360-staging-daily-batch`
- **Schedule Expression:** `cron(0 2 * * ? *)`
- **Timezone:** UTC
- **To Pause Scheduled Runs:**
  ```bash
  aws scheduler update-schedule \
    --name "c360-staging-daily-batch" \
    --state "DISABLED"
  ```
- **To Resume Scheduled Runs:**
  ```bash
  aws scheduler update-schedule \
    --name "c360-staging-daily-batch" \
    --state "ENABLED"
  ```
