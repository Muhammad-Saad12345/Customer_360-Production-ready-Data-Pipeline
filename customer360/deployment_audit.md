# Customer 360 — Live Deployment Audit

**Audit Timestamp:** 2026-10-12T11:46:00Z  
**AWS Account:** `734143568343`  
**Region:** `us-east-1`  
**Repository:** `Muhammad-Saad12345/Customer_360-Production-ready-Data-Pipeline`

---

## 1. Identity & Access Management (Live Discovery)

| Identity / Role | ARN | Status | Assessment |
| :--- | :--- | :--- | :--- |
| **Current Caller Identity** | `arn:aws:iam::734143568343:user/C360` | ✅ Active (`AIDA2V3S53HLUX76XPQ4X`) | Data-plane authorized; lacks IAM mutation & MWAA API permissions. |
| **Antigravity Deploy Role** | `arn:aws:iam::734143568343:role/C360-Antigravity-DeployRole` | ⚠️ Discovered (`AROA2V3S53HL3DKZ6LS46`) | Has `airflow:*` scoped to `c360-airflow-production-final`. User `C360` cannot assume due to root-only trust principal and lack of user identity policy. |
| **MWAA Execution Role** | `arn:aws:iam::734143568343:role/C360-MWAA-Execution-Role` | ⚠️ Active (`AROA2V3S53HLT35U36PBO`) | Valid trust policy (`airflow.amazonaws.com`). Needs inline Secrets Manager & SQS celery worker baseline policy. |
| **GitHub OIDC Deploy Role** | `arn:aws:iam::734143568343:role/github-mwaa-deploy-role` | ⚠️ Active (`AROA2V3S53HLSPPATUGVB`) | Trust condition missing repository owner prefix (`repo:Customer_360...` vs `repo:Muhammad-Saad12345/Customer_360...`). |

---

## 2. MWAA Environments & Runtime State

* **Discovered Production Environment:** `c360-airflow-production-final`
* **Artifacts S3 Bucket:** `s3://c360-mwaa-prod-786`
* **Requirements Object Version:** `FnQG51Bs8QeC3HlNvLX4znTLUVfP2Dnf`
* **Dependency Diagnosis & Remediation:**
  * **Failure:** `ModuleNotFoundError: No module named 'pyarrow'`
  * **Root Cause:** Upper bound pin `pyarrow>=14.0.0,<16.0.0` on line 26 conflicted directly with the official Airflow 2.11.2 constraints file (`pyarrow==18.1.0`), and `--constraint` was on line 10 instead of line 1. Pip failed with `ResolutionImpossible`, aborting installation of all custom dependencies.
  * **Fix Applied:** Moved `--constraint` to **Line 1** and unblocked bounds to `pyarrow>=14.0.0`, `tenacity>=8.2.0`, and `apache-airflow-providers-amazon>=8.0.0`. New version uploaded to S3.

---

## 3. Staging and Production Isolation Architecture

| Plane | Staging (Isolated) | Production (Isolated) | Status |
| :--- | :--- | :--- | :--- |
| **Glue Database** | `customer360_iceberg_stage` | `customer360_iceberg` | ✅ Both active in AWS Glue |
| **Athena Workgroup** | `customer360-staging` (Engine v3) | `primary` (Engine v3) | ✅ Both active & enabled |
| **Athena Query Results** | `s3://c360-bucket786/staging-athena-results/` | `s3://c360-bucket786/athena-results/` | ✅ Strict S3 prefix isolation |
| **Iceberg S3 Data Prefix** | `s3://c360-bucket786/staging-iceberg/` | `s3://c360-bucket786/iceberg/` | ✅ Strict S3 prefix isolation |
| **Synthetic Fallback** | Allowed for bounded testing (`true`) | Prohibited (`false`) | ✅ Code-enforced gate |

---

## 4. Lakehouse Catalog & Tables

### Production Database: `customer360_iceberg` (25 Tables / Views)
* All 25 tables active, queryable, and populated.

### Staging Database: `customer360_iceberg_stage` (6 Initialized Tables)
* Initialized Iceberg Tables: `hubspot_contacts`, `oms_orders`, `pim_products`, `loyalty_loyalty`, `marketing_campaign_events`, `zendesk_tickets`.
* Validated Bounded Ingestion: `hubspot_contacts` loaded with 27 records. Idempotency verified on re-run (count remained strictly 27).

---

## 5. Storage Security & Infrastructure

| Bucket | Versioning | Encryption | Public Access Block | Verification Status |
| :--- | :--- | :--- | :--- | :--- |
| `c360-bucket786` | Enabled | AES-256 | Enabled (All 4 settings True) | ✅ Verified secure |
| `c360-mwaa-prod-786` | Enabled | AES-256 | Enabled (All 4 settings True) | ✅ Verified secure |
| `c360-staging-tfstate` | Enabled | AES-256 | Enabled (All 4 settings True) | ✅ Verified secure (Terraform Backend) |
