# Customer 360 Lakehouse — Production Operations Runbook

## 1. Architecture Summary

```
External Sources (HubSpot, OMS, PIM, Loyalty, Marketing, Support)
    │
    ▼
AWS MWAA / Apache Airflow  ←── DAGs: customer360_batch.py, customer360_hourly.py
    │
    ▼
Raw S3  (s3://c360-prod-lakehouse/raw/<source>/<entity>/ingestion_date=<date>/run_id=<id>/)
    │
    ▼
Silver Gate  (schema validation · duplicate detection · DQ contract enforcement)
    │    ├── PASS → Silver Parquet (silver/)
    │    └── FAIL → Quarantine JSON (quarantine/)
    │
    ▼
DQ Quality Gate  (10% hard rejection threshold · CloudWatch metrics · SNS alert)
    │
    ▼
Apache Iceberg  (MERGE upserts via Athena Engine v3 · Glue Data Catalog)
    │
    ▼
dbt-athena  (19 models · Customer 360 mart · Executive mart · Analytics mart)
    │
    ▼
Athena Engine v3  →  Amazon QuickSight / BI
```

---

## 2. Environment Reference

| Variable              | Dev                        | Staging                     | Prod                        |
|-----------------------|----------------------------|-----------------------------|-----------------------------|
| `ENVIRONMENT`         | `dev`                      | `staging`                   | `prod`                      |
| S3 Bucket             | `c360-dev-lakehouse`       | `c360-staging-lakehouse`    | `c360-prod-lakehouse`       |
| MWAA Bucket           | *(local Airflow)*          | `c360-staging-mwaa-dags`    | `c360-prod-mwaa-dags`       |
| Glue Database         | `customer360_dev`          | `customer360_staging`       | `customer360_prod`          |
| Athena Workgroup      | `customer360-dev`          | `customer360-staging`       | `customer360-prod`          |
| MWAA Class            | *(local)*                  | `mw1.small`                 | `mw1.large`                 |
| DQ Hard Threshold     | 10%                        | 10%                         | 10%                         |
| Synthetic Data        | ✅ Allowed (dev guard)     | ❌ Blocked                  | ❌ Blocked                  |

---

## 3. Secrets Management

All API credentials are stored in AWS Secrets Manager under:
```
customer360/<environment>/<source>
```

**Required secret keys per source:**

| Source     | Secret Path                              | Keys                                        |
|------------|------------------------------------------|---------------------------------------------|
| HubSpot    | `customer360/prod/hubspot`               | `HUBSPOT_API_KEY`                           |
| Mailchimp  | `customer360/prod/mailchimp`             | `MAILCHIMP_API_KEY`, `MAILCHIMP_AUDIENCE_ID`|
| OMS        | `customer360/prod/oms`                   | `OMS_API_KEY`, `OMS_BASE_URL`               |
| PIM        | `customer360/prod/pim`                   | `PIM_API_KEY`, `PIM_BASE_URL`               |
| Loyalty    | `customer360/prod/loyalty`               | `LOYALTY_API_KEY`, `LOYALTY_BASE_URL`       |
| Zendesk    | `customer360/prod/zendesk`               | `ZENDESK_API_KEY`, `ZENDESK_BASE_URL`       |

To create/update a secret:
```bash
aws secretsmanager create-secret \
  --name "customer360/prod/hubspot" \
  --secret-string '{"HUBSPOT_API_KEY": "your-key-here"}'
```

---

## 4. MWAA Deployment

### 4.1 Initial Deployment (Terraform)

```bash
cd terraform/environments/prod
terraform init
terraform plan -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars
```

### 4.2 DAG Deployment (CI/CD — automated via GitHub Actions)

On every push to `main`:
1. Tests pass (≥70% coverage)
2. dbt parse validates syntax
3. Terraform plan runs against staging
4. GitHub Actions syncs DAGs to MWAA bucket:

```bash
aws s3 sync dags/  s3://c360-prod-mwaa-dags/dags/  --delete --exclude "__pycache__/*"
aws s3 sync src/   s3://c360-prod-mwaa-dags/dags/src/  --delete --exclude "*.pyc"
aws s3 sync dbt/   s3://c360-prod-mwaa-dags/dags/dbt/  --delete --exclude "target/*"
```

### 4.3 Manual DAG Trigger

Via Airflow UI or CLI:
```bash
aws mwaa create-cli-token --name customer360-prod | \
  python -c "import sys,json; token=json.load(sys.stdin); \
  print(f\"export MWAA_TOKEN={token['CliToken']}\")"

curl -X POST \
  "https://<mwaa-webserver-url>/api/v1/dags/customer360_batch/dagRuns" \
  -H "Authorization: Bearer $MWAA_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"conf": {}}'
```

---

## 5. Data Quality Policy

### 5.1 Rejection Rate Thresholds

| Threshold       | Default | Env Variable        | Action                                    |
|-----------------|---------|---------------------|-------------------------------------------|
| Hard (block)    | 10%     | `DQ_REJECT_HARD_PCT`| Raises exception, blocks Iceberg promotion|
| Soft (warn)     | 5%      | `DQ_REJECT_WARN_PCT`| Logs WARNING, pipeline continues          |

### 5.2 Overriding Thresholds

Set as Airflow Variables (via Secrets Manager):
```bash
aws secretsmanager put-secret-value \
  --secret-id customer360/prod/airflow/variables/DQ_REJECT_HARD_PCT \
  --secret-string "15"
```

### 5.3 Reviewing Quarantine Records

```bash
# List quarantine files for a given run
aws s3 ls s3://c360-prod-lakehouse/quarantine/ --recursive | grep <run_id>

# Download and inspect
aws s3 cp s3://c360-prod-lakehouse/quarantine/hubspot/contacts/\
ingestion_date=2026-10-01/run_id=<id>/invalid_records.json .
cat invalid_records.json | python -m json.tool | head -100
```

---

## 6. CloudWatch Monitoring

### 6.1 Dashboard

Open: **Customer360-Prod** dashboard in CloudWatch console.

Panels:
- DQ Gate Verdicts (5-min)
- Rejection Rate % by Source
- Task Outcomes (Success / Failure)
- Pipeline Health Status
- Record Counts (Input / Valid / Invalid)

### 6.2 Key Alarms

| Alarm Name                        | Trigger                              | Action         |
|-----------------------------------|--------------------------------------|----------------|
| `c360-prod-dq-gate-failed`        | Any DQ gate block                    | SNS → Email    |
| `c360-prod-high-rejection-rate`   | Rejection rate ≥ 10%                 | SNS → Email    |
| `c360-prod-pipeline-task-failures`| Any task failure                     | SNS → Email    |
| `c360-prod-pipeline-health-error` | Overall health = ERROR               | SNS → Email    |

---

## 7. Incident Response

### P1 — DQ Gate Blocked (Iceberg Promotion Halted)

1. Check SNS alert for source/entity and rejection rate.
2. Query quarantine records (see §5.3) to identify failure pattern.
3. If upstream data issue: coordinate with source team.
4. If false positive threshold: temporarily raise `DQ_REJECT_HARD_PCT` via Airflow Variable.
5. Re-trigger the failed task from the Airflow UI (clear + re-run).

### P2 — Task Failure (Non-DQ)

1. Check Airflow task logs in MWAA UI or CloudWatch (`/customer360/prod/mwaa/task`).
2. Check AWS service health (S3, Glue, Athena) at https://health.aws.amazon.com.
3. If transient: clear the task and retry (Airflow auto-retries 1-2 times).
4. If credential issue: verify Secrets Manager paths and IAM role policies.

### P3 — dbt Failure

1. Check `dbt_run` or `dbt_test` task logs.
2. Run locally:
   ```bash
   cd dbt && dbt run --no-partial-parse --target prod
   ```
3. Common causes: Iceberg table schema drift, Athena timeout (increase `query_timeout_seconds` in `dbt_project.yml`).

---

## 8. GitHub Actions Secrets Required

Configure these in GitHub repository settings → Secrets → Actions:

| Secret Name                    | Description                                     |
|--------------------------------|-------------------------------------------------|
| `AWS_DEPLOY_ROLE_ARN_STAGING`  | IAM OIDC role ARN for staging deployments       |
| `AWS_DEPLOY_ROLE_ARN_PROD`     | IAM OIDC role ARN for production deployments    |
| `MWAA_DAG_BUCKET_STAGING`      | S3 bucket name for staging MWAA DAGs            |
| `MWAA_DAG_BUCKET_PROD`         | S3 bucket name for production MWAA DAGs         |
| `SNS_ALERT_TOPIC_ARN_PROD`     | SNS topic ARN for production deployment alerts  |

---

## 9. Terraform State Bootstrap

Before first `terraform init`, create remote state resources:

```bash
# State bucket
aws s3api create-bucket \
  --bucket c360-prod-tfstate \
  --region us-east-1

aws s3api put-bucket-versioning \
  --bucket c360-prod-tfstate \
  --versioning-configuration Status=Enabled

aws s3api put-bucket-encryption \
  --bucket c360-prod-tfstate \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"aws:kms"},"BucketKeyEnabled":true}]}'

# Lock table
aws dynamodb create-table \
  --table-name c360-prod-tflock \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1
```

---

## 10. Maintenance Windows

| Task                         | Frequency    | Airflow Task              |
|------------------------------|--------------|---------------------------|
| Iceberg OPTIMIZE + VACUUM    | Daily        | `iceberg_maintenance`     |
| dbt model refresh            | Every run    | `dbt_run` + `dbt_test`    |
| Checkpoint S3 pruning        | Weekly       | Manual or cron            |
| Quarantine record expiry     | 30 days      | S3 lifecycle policy       |
| Raw landing zone transition  | 90 days      | S3 lifecycle → Glacier IR |
