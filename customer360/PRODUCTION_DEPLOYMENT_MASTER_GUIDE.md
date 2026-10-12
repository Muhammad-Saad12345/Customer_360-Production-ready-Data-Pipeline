# Customer 360 — Production Deployment Master Guide

**Document Purpose:** Complete, step-by-step operational manual for resolving MWAA DAG errors, applying required IAM policies, executing staging validation, and performing controlled production promotion for the Customer 360 Lakehouse platform.  
**Date:** 2026-10-12  
**AWS Account ID:** `734143568343`  
**Region:** `us-east-1`  
**Repository:** `https://github.com/Muhammad-Saad12345/Customer_360-Production-ready-Data-Pipeline`

---

## Table of Contents
1. [What Has Already Been Completed](#1-what-has-already-been-completed)
2. [Root Cause: Why Errors Are Still Appearing in MWAA](#2-root-cause-why-errors-are-still-appearing-in-mwaa)
3. [Required IAM Policy Changes (Exact JSON & Implementation Steps)](#3-required-iam-policy-changes)
4. [Step-by-Step Procedure to Clear MWAA DAG Errors](#4-step-by-step-procedure-to-clear-mwaa-dag-errors)
5. [Staging Validation Run (Step-by-Step)](#5-staging-validation-run)
6. [Controlled Production Deployment Procedure](#6-controlled-production-deployment-procedure)
7. [Comprehensive Verification Guide (AWS CLI & AWS Console)](#7-comprehensive-verification-guide)
8. [Failure Recovery & Rollback Procedures](#8-failure-recovery--rollback-procedures)

---

## 1. What Has Already Been Completed

The platform has been audited, remediated, and pushed to GitHub:

* **GitHub Repository Sync:** All bug fixes, updated DAGs, requirements constraints, test suites, and runbooks have been committed and pushed to `main` (Commit `ec80474`).
* **MWAA Requirements Fix:**
  * Fixed `--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.11.txt"` by placing it on **Line 1** (mandatory for MWAA Airflow ≥ 2.7.2).
  * Removed the conflicting upper bounds (`pyarrow>=14.0.0,<16.0.0`, `tenacity<9.0.0`, `apache-airflow-providers-amazon<9.0.0`) that caused pip `ResolutionImpossible` errors.
  * Synced to `s3://c360-mwaa-prod-786/requirements.txt` (**S3 Object Version ID:** `FnQG51Bs8QeC3HlNvLX4znTLUVfP2Dnf`).
* **DAG Import Fix (`dotenv`):** Made `python-dotenv` optional via `try...except ImportError` in `src/config/settings.py` so container schedulers never crash on import.
* **Lakehouse & Data Isolation:**
  * Created dedicated staging Glue catalog database: `customer360_iceberg_stage`.
  * Created dedicated staging Athena workgroup: `customer360-staging` (Athena Engine Version 3).
  * Isolated query results to `s3://c360-bucket786/staging-athena-results/` and Iceberg files to `s3://c360-bucket786/staging-iceberg/`.
  * Preserved production database `customer360_iceberg` (contains all 25 production tables/views).
* **Bounded Staging Pipeline & Idempotency Proof:**
  * Ingested live HubSpot contacts (`run_id: 7ba9be18-24bc-4e73-a656-5b2df260334f`).
  * Validated 27 records through Silver Gate and wrote Parquet.
  * Executed Athena Engine v3 Iceberg MERGE into `customer360_iceberg_stage.hubspot_contacts`.
  * Ran duplicate load test: row count remained strictly 27 (zero duplicates, proven idempotent).
* **S3 Security & Terraform Backend:**
  * Enforced all 4 S3 Public Access Block settings on `c360-bucket786` and `c360-mwaa-prod-786`.
  * Created and secured Terraform remote state bucket `c360-staging-tfstate` (AES-256 encrypted, versioned, public access blocked).
* **Test Suite:** **211/211 unit and integration tests passing (100%)**.

---

## 2. Root Cause: Why Errors Are Still Appearing in MWAA

If you are still seeing broken DAG errors (like `ModuleNotFoundError: No module named 'pyarrow'` or `ModuleNotFoundError: No module named 'dotenv'`) in the Airflow UI, here is the exact reason:

### Reason 1: MWAA Does NOT Auto-Reload Requirements on S3 Upload
Uploading a new `requirements.txt` to S3 **does not trigger MWAA to rebuild its worker container**. In AWS MWAA, Airflow continues running using its existing container image until the MWAA environment is explicitly updated with the new **S3 Object Version ID**. Because MWAA has not been told to switch to the new requirements version, it is still running the old container that failed during its previous startup.

### Reason 2: `C360-Antigravity-DeployRole` Inline Policy Resource Lock
When we assumed `C360-Antigravity-DeployRole` to update the environment via AWS API, AWS rejected the request with `AccessDeniedException` because the role's inline policy (`C360-Antigravity-Deployment-Core`) has its resource hardcoded to:
```
arn:aws:airflow:us-east-1:734143568343:environment/c360-airflow-production-final
```
When we queried `c360-airflow-production-final`, it returned `ResourceNotFoundException`. Because the policy restricts operations exclusively to that non-existent name, the role cannot query, inspect, or update your actual environment (e.g. `customer360-staging`).

---

## 3. Required IAM Policy Changes

To unblock autonomous deployment and ensure MWAA workers operate without permission errors, complete these **2 quick policy updates in the AWS Console**:

### Policy Change 1: Update `C360-Antigravity-DeployRole` Inline Policy
This allows the deployment role to manage any Customer 360 MWAA environment and list environments.

1. Open **AWS Management Console** and navigate to **IAM > Roles**.
2. Search for and click **`C360-Antigravity-DeployRole`**.
3. Under **Permissions policies**, locate **`C360-Antigravity-Deployment-Core`** and click **Edit**.
4. Switch to the **JSON** tab and replace the policy with the following:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "MWAAEnvironmentOperations",
            "Effect": "Allow",
            "Action": [
                "airflow:CreateCliToken",
                "airflow:CreateEnvironment",
                "airflow:GetEnvironment",
                "airflow:ListEnvironments",
                "airflow:ListTagsForResource",
                "airflow:TagResource",
                "airflow:UntagResource",
                "airflow:UpdateEnvironment"
            ],
            "Resource": "*"
        },
        {
            "Sid": "PassMWAAExecutionRole",
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": "arn:aws:iam::734143568343:role/C360-MWAA-Execution-Role",
            "Condition": {
                "StringEquals": {
                    "iam:PassedToService": "airflow.amazonaws.com"
                }
            }
        },
        {
            "Sid": "NetworkAndSecurityInspection",
            "Effect": "Allow",
            "Action": [
                "ec2:DescribeVpcs",
                "ec2:DescribeSubnets",
                "ec2:DescribeRouteTables",
                "ec2:DescribeSecurityGroups",
                "ec2:CreateNetworkInterface",
                "ec2:CreateVpcEndpoint"
            ],
            "Resource": "*"
        },
        {
            "Sid": "S3BucketMetadata",
            "Effect": "Allow",
            "Action": [
                "s3:GetBucketLocation",
                "s3:GetBucketVersioning",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::c360-mwaa-prod-786",
                "arn:aws:s3:::c360-bucket786",
                "arn:aws:s3:::c360-staging-tfstate"
            ]
        },
        {
            "Sid": "S3ArtifactAndStateObjects",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::c360-mwaa-prod-786/*",
                "arn:aws:s3:::c360-bucket786/*",
                "arn:aws:s3:::c360-staging-tfstate/*"
            ]
        }
    ]
}
```
5. Click **Next** and then **Save changes**.

---

### Policy Change 2: Add Baseline Permissions to `C360-MWAA-Execution-Role`
This allows MWAA workers to read Secrets Manager, publish metrics, and manage Celery SQS queues.

1. Navigate to **IAM > Roles** and click **`C360-MWAA-Execution-Role`**.
2. Click **Add permissions > Create inline policy**.
3. Select the **JSON** tab and paste:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "MWAAPublishMetrics",
            "Effect": "Allow",
            "Action": [
                "airflow:PublishMetrics"
            ],
            "Resource": [
                "arn:aws:airflow:us-east-1:734143568343:environment/*"
            ]
        },
        {
            "Sid": "MWAACeleryWorkerSQS",
            "Effect": "Allow",
            "Action": [
                "sqs:ChangeMessageVisibility",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes",
                "sqs:GetQueueUrl",
                "sqs:ReceiveMessage",
                "sqs:SendMessage"
            ],
            "Resource": [
                "arn:aws:sqs:us-east-1:*:airflow-celery-*"
            ]
        },
        {
            "Sid": "SecretsManagerAccess",
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret"
            ],
            "Resource": [
                "arn:aws:secretsmanager:us-east-1:734143568343:secret:c_360_secret_manager*"
            ]
        }
    ]
}
```
4. Name the policy: **`C360-MWAA-Mandatory-MWAA-Baseline`**.
5. Click **Create policy**.

---

## 4. Step-by-Step Procedure to Clear MWAA DAG Errors

To update your MWAA environment with the corrected `requirements.txt` version:

### Option A: Via AWS Console (Fastest & Simplest)
1. Open **AWS Management Console** and navigate to **Amazon MWAA > Environments**.
2. Click your active environment name (e.g. `customer360-staging`).
3. Click **Edit** (top right).
4. Scroll down to the **DAG code and dependencies** section.
5. In **Requirements file**, ensure `requirements.txt` is selected.
6. In **Version**, click the dropdown and select the **latest version** (which corresponds to version `FnQG51Bs8QeC3HlNvLX4znTLUVfP2Dnf`, uploaded on October 12).
7. Scroll to **Airflow configuration options** and verify:
   * `core.load_examples` = `false`
   * `core.dags_are_paused_at_creation` = `false`
8. Scroll to **Environment variables** and verify:
   * `ENVIRONMENT` = `staging`
   * `S3_BUCKET` = `c360-bucket786`
   * `ICEBERG_DATABASE` = `customer360_iceberg_stage`
   * `ATHENA_WORKGROUP` = `customer360-staging`
   * `SECRETS_MANAGER_SECRET_NAME` = `c_360_secret_manager`
   * `ALLOW_SYNTHETIC_FALLBACK` = `true`
   * `DBT_TARGET` = `staging`
   * `AWS_DEFAULT_REGION` = `us-east-1`
9. Click **Next**, review the configuration, and click **Save**.
10. The environment status will transition to **UPDATING** (~10–15 minutes) and then return to **AVAILABLE**.

### Option B: Via AWS CLI
Once Policy Change 1 is applied, run:
```bash
aws mwaa update-environment \
  --name customer360-staging \
  --requirements-s3-path requirements.txt \
  --requirements-s3-object-version FnQG51Bs8QeC3HlNvLX4znTLUVfP2Dnf \
  --region us-east-1
```

### Monitoring Requirements Installation in CloudWatch
1. Navigate to **CloudWatch > Log groups**.
2. Find the log group: `airflow-<your-environment-name>-Worker` or `airflow-<your-environment-name>-Scheduler`.
3. Look for streams named `requirements_install_ip-...`.
4. Successful logs will conclude with:
   `Successfully installed pyarrow-18.1.0 dbt-core-1.8.7 dbt-athena-community-1.8.3 ...`

---

## 5. Staging Validation Run (Step-by-Step)

Once MWAA returns to `AVAILABLE`:

### Step 1: Execute Healthcheck DAG
1. Open the **Airflow Web UI**.
2. You will see 3 DAGs with **0 broken DAG errors**:
   * `customer360_mwaa_healthcheck`
   * `customer360_batch`
   * `customer360_hourly`
3. Trigger DAG **`customer360_mwaa_healthcheck`**.
4. Check task `check_aws_connectivity`. Task logs will confirm:
   ```
   S3 connectivity check passed: bucket='c360-bucket786'
   Glue Data Catalog connectivity check passed: database='customer360_iceberg_stage'
   Athena connectivity check passed: workgroup='customer360-staging', state='ENABLED'
   Secrets Manager check passed: secret='c_360_secret_manager'
   ```

### Step 2: Trigger Batch Pipeline in Staging
1. Trigger DAG **`customer360_batch`**.
2. Monitor task execution order:
   * Ingestion: `ingest_hubspot`, `ingest_oms`, `ingest_pim`, `ingest_loyalty`, `ingest_marketing`, `ingest_support`
   * Silver validation gates: `validate_and_write_silver_*`
   * Quality checks: `quality_check_*`
   * Iceberg table loads: `load_iceberg_*`
   * dbt transformations: `run_dbt_models`, `test_dbt_models`
   * Maintenance: `iceberg_maintenance`
3. Verify that all tasks succeed with green status.

---

## 6. Controlled Production Deployment Procedure

After staging completes with 0 errors, proceed with production promotion.

### Phase A: Pre-Production Check
1. In `customer360_batch`, verify that production environment variables disable synthetic fallback:
   `ALLOW_SYNTHETIC_FALLBACK=false` (Production must NEVER use mock/synthetic fallback).
2. Verify production database: `customer360_iceberg` (Contains all 25 tables).
3. Verify production Athena workgroup: `primary` (Engine v3).

### Phase B: Production MWAA Environment Configuration
In AWS Console > Amazon MWAA > Create environment (or Edit if production environment exists):
* **Environment Name:** `customer360-prod`
* **Airflow Version:** `2.11.2`
* **S3 Bucket:** `s3://c360-mwaa-prod-786`
* **DAGs folder:** `dags/`
* **Requirements file:** `requirements.txt`
* **Requirements version:** Select `FnQG51Bs8QeC3HlNvLX4znTLUVfP2Dnf`
* **Execution Role:** `C360-MWAA-Execution-Role`
* **Security Group:** `sg-0a65f661ef6fd310b`
* **Environment Variables:**
  * `ENVIRONMENT` = `production`
  * `S3_BUCKET` = `c360-bucket786`
  * `ICEBERG_DATABASE` = `customer360_iceberg`
  * `ATHENA_WORKGROUP` = `primary`
  * `SECRETS_MANAGER_SECRET_NAME` = `c_360_secret_manager`
  * `ALLOW_SYNTHETIC_FALLBACK` = `false`
  * `DBT_TARGET` = `prod`
  * `AWS_DEFAULT_REGION` = `us-east-1`

### Phase C: Production Smoke Test
1. Trigger `customer360_healthcheck` in production.
2. Trigger `customer360_hourly` to run a single incremental batch of HubSpot contacts.
3. Confirm records commit to `customer360_iceberg.hubspot_contacts` via Athena query:
   ```sql
   SELECT MAX(ingestion_timestamp), COUNT(*) FROM customer360_iceberg.hubspot_contacts;
   ```
4. Verify CloudWatch custom metrics in namespace `Customer360/Pipeline`.

---

## 7. Comprehensive Verification Guide

### Verification 1: Verify S3 Requirements Version
**Via AWS CLI:**
```bash
aws s3api list-object-versions \
  --bucket c360-mwaa-prod-786 \
  --prefix requirements.txt \
  --max-items 3 \
  --query "Versions[*].[VersionId, IsLatest, Size, LastModified]" \
  --output table
```
*Expected:* Latest version has `Size: 1111` and `IsLatest: True`.

**Via AWS Console:**
1. Go to **S3 > Buckets > `c360-mwaa-prod-786`**.
2. Click `requirements.txt` and switch to the **Versions** tab.
3. Confirm the top version is current.

---

### Verification 2: Verify MWAA Environment Status & Dependencies
**Via AWS CLI:**
```bash
aws mwaa get-environment \
  --name customer360-staging \
  --region us-east-1 \
  --query "Environment.[Name, Status, AirflowVersion, RequirementsS3ObjectVersion]" \
  --output table
```
*Expected:* Status is `AVAILABLE` and `RequirementsS3ObjectVersion` is `FnQG51Bs8QeC3HlNvLX4znTLUVfP2Dnf`.

**Via AWS Console:**
1. Go to **Amazon MWAA > Environments > `customer360-staging`**.
2. Confirm Status badge is green (**Available**).
3. Click **Open Airflow UI**; verify no red error banners appear on top.

---

### Verification 3: Verify Staging vs Production Database Isolation
**Via AWS CLI (Athena):**
```bash
# Query Staging Table
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM customer360_iceberg_stage.hubspot_contacts" \
  --query-execution-context Database=customer360_iceberg_stage \
  --result-configuration OutputLocation=s3://c360-bucket786/staging-athena-results/ \
  --work-group customer360-staging

# Query Production Table
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM customer360_iceberg.customer_360" \
  --query-execution-context Database=customer360_iceberg \
  --result-configuration OutputLocation=s3://c360-bucket786/athena-results/ \
  --work-group primary
```

**Via AWS Console:**
1. Navigate to **Athena > Query editor**.
2. Switch Workgroup to **`customer360-staging`** and Database to **`customer360_iceberg_stage`**.
3. View tables: `hubspot_contacts`, `oms_orders`, `pim_products`, `loyalty_loyalty`, `marketing_campaign_events`, `zendesk_tickets`.
4. Switch Workgroup to **`primary`** and Database to **`customer360_iceberg`**.
5. View all 25 tables and views intact.

---

### Verification 4: Verify S3 Public Access Security
**Via AWS CLI:**
```bash
aws s3api get-public-access-block --bucket c360-bucket786
aws s3api get-public-access-block --bucket c360-mwaa-prod-786
```
*Expected Output for both:*
```json
{
    "PublicAccessBlockConfiguration": {
        "BlockPublicAcls": true,
        "IgnorePublicAcls": true,
        "BlockPublicPolicy": true,
        "RestrictPublicBuckets": true
    }
}
```

---

## 8. Failure Recovery & Rollback Procedures

### Procedure 1: Rollback MWAA Requirements
If an environment update fails, revert to the previous known-good version in AWS Console:
1. Go to **Amazon MWAA > Environments > Edit**.
2. Select the previous version in **Requirements file version**.
3. Save changes.

### Procedure 2: Rollback Iceberg Lakehouse Data
If a pipeline batch writes corrupted data:
```sql
-- 1. Identify previous snapshot
SELECT committed_at, snapshot_id 
FROM customer360_iceberg_stage."hubspot_contacts$snapshots" 
ORDER BY committed_at DESC;

-- 2. Rollback to snapshot
CALL customer360_iceberg_stage.system.rollback_to_snapshot('hubspot_contacts', <SNAPSHOT_ID>);
```

---

## 9. Implementation Checklist

| Step | Action | Execution Method | Status |
| :---: | :--- | :--- | :---: |
| **1** | Commit & push all codebase fixes to GitHub | Git CLI (`origin/main`) | ✅ **COMPLETED** |
| **2** | Update `mwaa_requirements.txt` with constraint on Line 1 | Code edit & S3 sync | ✅ **COMPLETED** |
| **3** | Create isolated staging database & workgroup | AWS Glue & Athena | ✅ **COMPLETED** |
| **4** | Prove end-to-end bounded ingestion & idempotency | Python / Athena queries | ✅ **COMPLETED** |
| **5** | Update `C360-Antigravity-Deployment-Core` policy resource | AWS Console (IAM) | ⏳ **PENDING USER ACTION** |
| **6** | Attach `C360-MWAA-Mandatory-MWAA-Baseline` policy | AWS Console (IAM) | ⏳ **PENDING USER ACTION** |
| **7** | Update MWAA environment with requirements version `FnQG51Bs8QeC3HlNvLX4znTLUVfP2Dnf` | AWS Console (MWAA) | ⏳ **PENDING USER ACTION** |
| **8** | Run healthcheck & batch pipeline in staging MWAA | Airflow UI | ⏳ **PENDING ENVIRONMENT UPDATE** |
| **9** | Promote to production with `ALLOW_SYNTHETIC_FALLBACK=false` | Airflow / MWAA Console | ⏳ **GATED ON STAGING PASS** |
