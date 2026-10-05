# Customer 360 Lakehouse — Production Implementation Plan

**Target Architecture**: AWS MWAA (Airflow 2.8+) → Raw S3 → Silver S3 Parquet (Pydantic contract validation & quarantine) → Apache Iceberg v2 → AWS Glue Data Catalog → Athena Engine v3 → dbt-athena (staging, intermediate, dimensions, facts, marts) → CloudWatch / SNS.

---

## 1. Remediation Status Assessment

| Area / Component | Current Implementation | Implementation Gap | Status |
| :--- | :--- | :--- | :--- |
| **Credential Security** | `.env.example` has real HubSpot PAT token. Flat `.env` used for containers. | Credentials committed in template; no secret scanner in CI. | **BLOCKED (P0)** |
| **Secrets Abstraction** | `src/config/settings.py` reads exclusively via `os.getenv` / `dotenv`. | No AWS Secrets Manager client; MWAA cannot retrieve credentials dynamically. | **NOT IMPLEMENTED (P0)** |
| **Repo Hygiene** | `.gitignore` only has 8 basic lines. Runtime data, scratch scripts, local logs tracked. | Exposure of customer PII in `quarantine/`, `.checkpoints/`, `dbt/target/`. | **NOT IMPLEMENTED (P0)** |
| **Multi-Env Config** | Hardcoded dev config in `dbt/profiles.yml`, `settings.py`, and `config.py`. | No dynamic resolution for DEV, STAGING, PROD buckets, catalogs, and workgroups. | **NOT IMPLEMENTED (P0)** |
| **Ingestion Fallback** | Bare `except Exception:` silently falls back to synthetic data across all 6 sources. | Outages in staging/prod masked by fake records; customer marts corrupted silently. | **HIGH RISK (P0)** |
| **Watermark State** | `src/checkpoint.py` writes to local `.checkpoints/` JSON directory. | MWAA workers are ephemeral containers; local disk checkpoints disappear between tasks. | **HIGH RISK (P0)** |
| **Quality Gates** | `_quality_gate` in Airflow checks only `input == valid + invalid`. | No rejection rate threshold (>10% fail); 100% quarantine passes silently. | **HIGH RISK (P0)** |
| **Athena & Iceberg** | Fixed 120s timeout in `athena_client.py`; static 500-row batching in `iceberg_loader.py`. | Compactions abort after 2 min; large SQL statements exceed Athena 256KB limit. | **HIGH RISK (P1)** |
| **dbt Dimensions & Facts** | `dim_date.sql` ends in 2025/2027; `fact_order` is full table rebuild. | Date joins break for future records; unnecessary full table scans on large volumes. | **MEDIUM RISK (P1)** |
| **Observability** | `airflow_callbacks.py` appends to local `logs/pipeline_run_log.jsonl`. | Telemetry lost on container exit; no CloudWatch custom metrics emitted. | **BLOCKED (P0)** |
| **Alerting** | `email_on_failure=False`; no SNS topic; no CloudWatch alarms. | Pipeline failure goes undetected without manual inspection of logs. | **NOT IMPLEMENTED (P0)** |
| **Infrastructure as Code**| Zero IaC exists in repo; resources created manually. | Inability to audit, replicate, or automate deployment of AWS environments. | **NOT IMPLEMENTED (P0)** |
| **CI/CD Automation** | `.github/workflows/ci.yml` runs pytest and masks dbt errors (`\|\| echo`). | No OIDC auth; no lint/security scan; no Terraform plan; no staging/prod CD. | **BLOCKED (P0)** |
| **MWAA Deployment** | Local docker-compose with local bind mounts. | No packaging/sync mechanism for DAGs, packages, plugins to MWAA S3 bucket. | **NOT IMPLEMENTED (P1)** |

---

## 2. Detailed Remediation Plan by Phase

### Phase 1 — Security Hardening
* **Finding**: Hardcoded HubSpot token in `.env.example` line 5; unencrypted local secrets; `.gitignore` leaves sensitive runtime directories exposed.
* **Current Implementation**: Static token in example template; basic 8-line `.gitignore`.
* **Required Change**:
  1. Remove PAT token from `.env.example` and replace with dummy placeholder.
  2. Expand `.gitignore` to strictly exclude `.env`, `.checkpoints/`, `quarantine/`, `logs/`, `dbt/target/`, `dbt/dbt_packages/`, `scratch/`, `*.log`, `*.parquet`.
  3. Add secret scanning hook / CI step using `gitleaks` configuration.
  4. Create `src/config/secrets.py` providing `get_secret(secret_name, default=None)` which fetches from AWS Secrets Manager in production/staging (`c_360_secret_manager`) with local environment fallback.
  5. Add unit tests in `tests/test_secrets.py` validating secret loading, masking, and missing secret exceptions.
* **Files Affected**:
  - `.env.example`
  - `.gitignore`
  - `.gitleaks.toml`
  - `src/config/secrets.py`
  - `src/config/settings.py`
  - `tests/test_secrets.py`
* **AWS Resources Required**: AWS Secrets Manager (`c_360_secret_manager`, ARN: `arn:aws:secretsmanager:us-east-1:734143568343:secret:c_360_secret_manager-K6zw6f`).
* **Task Type**: Automated code implementation + Manual credential rotation in 3rd party portals.
* **Validation Method**: `pytest tests/test_secrets.py`; regex scanning for secret patterns; verify git status ignores runtime folders.
* **Acceptance Criteria**: Zero plaintext secrets in git; `src/config/secrets.py` loads successfully with test mocks and live fallback; `.gitignore` covers all transient/PII paths.

---

### Phase 2 — Environment Configuration
* **Finding**: Hardcoded bucket `c360-bucket786`, schema `customer360_iceberg`, and workgroup `primary`. No DEV/STAGING/PROD logical separation.
* **Current Implementation**: Static dev values in `dbt/profiles.yml`, `settings.py`, and `config.py`.
* **Required Change**:
  1. Parameterize `src/config/settings.py` with `ENVIRONMENT` (`dev`, `staging`, `prod`).
  2. Implement environment-aware naming conventions:
     - DEV: `customer360-lakehouse-dev`, `customer360_iceberg_dev`, workgroup `c360_dev`
     - STAGING: `customer360-lakehouse-stage`, `customer360_iceberg_stage`, workgroup `c360_staging`
     - PROD: `customer360-lakehouse-prod`, `customer360_iceberg`, workgroup `c360_prod`
  3. Update `dbt/profiles.yml` with `{{ env_var('DBT_TARGET', 'dev') }}` and parameterized outputs for `dev`, `staging`, and `prod`.
  4. Ensure settings dynamically resolve defaults according to the active `ENVIRONMENT`.
* **Files Affected**:
  - `src/config/settings.py`
  - `src/lakehouse/config.py`
  - `dbt/profiles.yml`
  - `tests/test_config.py`
* **AWS Resources Required**: S3 buckets and Glue Catalogs per environment.
* **Task Type**: Automated.
* **Validation Method**: Unit tests asserting resolution of environment-specific variables for all three tiers; `dbt parse --target <env>`.
* **Acceptance Criteria**: `profiles.yml` compiles without hardcoded credentials; setting `ENVIRONMENT=prod` resolves prod bucket and Glue catalog.

---

### Phase 3 — Ingestion Reliability & Checkpointing
* **Finding**: Unconditional synthetic fallback suppresses API failures; watermarks stored only in local filesystem `.checkpoints/`.
* **Current Implementation**: Ingesters catch `Exception` and return synthetic rows; `CheckpointStore` reads/writes local JSON.
* **Required Change**:
  1. In `src/ingestion/oms.py`, `pim.py`, `loyalty.py`, `marketing.py`, `support.py`, and `hubspot.py`, check `ENVIRONMENT`. In `staging` and `prod`, re-raise exceptions immediately on API failure. Only fall back to synthetic data if `ENVIRONMENT in ('dev', 'test')`.
  2. Enhance `src/checkpoint.py` to support dual backends: S3 persistent checkpointing (`s3://<bucket>/checkpoints/{source}/{entity}.json`) in cloud mode, and local disk fallback for tests/offline development.
  3. Ensure checkpoint updates are atomic, idempotent, and include execution metadata (`run_id`, `updated_at`, `status`).
* **Files Affected**:
  - `src/ingestion/base.py`
  - `src/ingestion/oms.py`
  - `src/ingestion/pim.py`
  - `src/ingestion/loyalty.py`
  - `src/ingestion/marketing.py`
  - `src/ingestion/support.py`
  - `src/ingestion/hubspot.py`
  - `src/checkpoint.py`
  - `tests/test_checkpoint.py`
  - `tests/test_ingestion_reliability.py`
* **AWS Resources Required**: S3 prefix `s3://<bucket>/checkpoints/`.
* **Task Type**: Automated.
* **Validation Method**: Unit tests verifying that prod mode raises `RuntimeError`/API error, and test/dev mode falls back to synthetic; S3 checkpoint read/write verification with moto/boto3 mock.
* **Acceptance Criteria**: Ingestion fails loud in prod on API errors; checkpoints persist to S3.

---

### Phase 4 — Iceberg / Athena Hardening
* **Finding**: Query timeout is fixed at 120s; batch chunking uses static 500 rows; `dim_date` date spine ends prematurely; large fact tables use full rebuilds.
* **Current Implementation**: `AthenaClient.execute_query` defaults to 120s; `iceberg_loader.py` uses fixed `BATCH_SIZE = 500`; `fact_order` materialized as `'table'`.
* **Required Change**:
  1. Update `AthenaClient.execute_query` to allow query-specific timeout overrides with default 900s for DDL/MERGE.
  2. In `iceberg_loader.py`, implement dynamic SQL batch chunking based on query string byte size (<192 KB safe margin) and maximum 250 rows to avoid exceeding Athena 256KB limits.
  3. Parameterize Iceberg maintenance retention in `IcebergMaintenanceManager` via `settings.ICEBERG_MAINTENANCE_RETENTION_DAYS` (default 7 days).
  4. Extend `dbt/models/dimensions/dim_date.sql` to 2035 with a comprehensive date generation macro.
  5. Refactor `fact_order.sql` to support `incremental` materialization with `unique_key='order_id'` and `incremental_strategy='merge'` on Iceberg.
* **Files Affected**:
  - `src/lakehouse/athena_client.py`
  - `src/lakehouse/iceberg_loader.py`
  - `src/lakehouse/iceberg_maintenance.py`
  - `dbt/models/dimensions/dim_date.sql`
  - `dbt/models/facts/fact_order.sql`
  - `tests/test_iceberg_hardening.py`
* **AWS Resources Required**: Athena Engine v3 workgroup, Glue Catalog.
* **Task Type**: Automated.
* **Validation Method**: Run unit tests on dynamic batch generation; run `dbt parse` and verify incremental SQL compilation.
* **Acceptance Criteria**: MERGE SQL stays under 192KB; date dimension extends to 2035; fact models support incremental merge.

---

### Phase 5 — Data Quality Production Gates
* **Finding**: `_quality_gate` only verifies `input == valid + invalid`. Upstream total schema corruptions (100% quarantine) pass without failure.
* **Current Implementation**: Arithmetic balance check in Airflow quality task.
* **Required Change**:
  1. Implement three-tier rejection rate policy in `_quality_gate` and `silver_gate.py`:
     - `< 2%`: HEALTHY (info log)
     - `2% – 10%`: WARNING (warning log, emit CloudWatch metric)
     - `> 10%`: CRITICAL / Pipeline Failure (raise ValueError, fail Airflow task)
  2. Implement zero-record anomaly detection: if `input_count == 0` for an expected active source, fail the production run or raise an upstream alert.
  3. Guarantee record reconciliation: `input_count == valid_count + invalid_count`.
  4. Add automated unit tests covering all 4 scenarios.
* **Files Affected**:
  - `dags/customer360_batch.py`
  - `src/processing/silver_gate.py`
  - `tests/test_data_quality_gates.py`
* **AWS Resources Required**: None (logic level); CloudWatch metrics destination.
* **Task Type**: Automated.
* **Validation Method**: Run `pytest tests/test_data_quality_gates.py`.
* **Acceptance Criteria**: Rejection rate > 10% raises exception; zero-record anomalies on required sources fail task; balance equation holds.

---

### Phase 6 — Observability & CloudWatch Telemetry
* **Finding**: Telemetry is written to local `logs/pipeline_run_log.jsonl`, disappearing upon container teardown.
* **Current Implementation**: In-memory JSONL file writing.
* **Required Change**:
  1. Create `src/monitoring/cloudwatch_metrics.py` to publish metrics via boto3 `put_metric_data` under namespace `Customer360/Pipeline`.
  2. Implement metrics:
     - `PipelineExecutionSuccess`, `PipelineExecutionFailure`
     - `TaskDurationSeconds` (with dimensions: Source, Entity, Tier)
     - `RecordsIngested`, `RecordsQuarantined`
     - `RejectionRate`
     - `ZeroRecordAnomaly`
     - `IcebergMaintenanceFailure`
  3. Update `airflow_callbacks.py` to call CloudWatch publisher on success and failure.
  4. Never log tokens, passwords, or customer PII.
* **Files Affected**:
  - `src/monitoring/cloudwatch_metrics.py`
  - `src/monitoring/airflow_callbacks.py`
  - `tests/test_observability.py`
* **AWS Resources Required**: CloudWatch Metrics Namespace `Customer360/Pipeline`.
* **Task Type**: Automated code + IAM permissions for `cloudwatch:PutMetricData`.
* **Validation Method**: Unit tests mocking boto3 CloudWatch client and asserting metric payloads.
* **Acceptance Criteria**: Callback emits metrics to CloudWatch; no local file dependency for critical health telemetry.

---

### Phase 7 — Alerting & SNS Integration
* **Finding**: No SNS alert topics or CloudWatch alarms exist in code.
* **Current Implementation**: `email_on_failure: False`.
* **Required Change**:
  1. Create Terraform module `terraform/modules/sns/` provisioning SNS topic `c360-pipeline-alerts`.
  2. Create Terraform module `terraform/modules/cloudwatch/` with CloudWatch alarms:
     - Pipeline task failure alarm (`TaskFailures > 0`)
     - Critical rejection rate alarm (`RejectionRate > 10%`)
     - Ingestion freshness / SLA breach alarm
     - Iceberg maintenance failure alarm
  3. Wire `on_task_failure` callback to publish diagnostic alert payloads directly to SNS if `SNS_ALERT_TOPIC_ARN` is configured.
* **Files Affected**:
  - `src/monitoring/airflow_callbacks.py`
  - `terraform/modules/sns/`
  - `terraform/modules/cloudwatch/`
* **AWS Resources Required**: Amazon SNS topic `c360-pipeline-alerts`, CloudWatch Metric Alarms.
* **Task Type**: Automated code + Terraform provisioning.
* **Validation Method**: Validate Terraform syntax (`terraform validate`); test SNS publisher logic with mock.
* **Acceptance Criteria**: Alarms defined as code; callback publishes failure messages to SNS.

---

### Phase 8 — Infrastructure as Code (Terraform)
* **Finding**: No IaC exists in the repository.
* **Current Implementation**: Manual resource provisioning.
* **Required Change**:
  1. Construct modular Terraform layout:
     ```
     terraform/
       modules/
         s3/
         kms/
         iam/
         glue/
         athena/
         cloudwatch/
         sns/
         mwaa/
       environments/
         dev/
         staging/
         prod/
     ```
  2. Enforce security baselines: SSE-KMS, S3 Public Access Block, TLS-only bucket policies, versioning, least-privilege IAM execution roles for MWAA and Athena.
  3. Parameterize environments (`dev`, `staging`, `prod`) cleanly.
* **Files Affected**:
  - `terraform/` (all modules and environment configs)
* **AWS Resources Required**: Full target AWS footprint.
* **Task Type**: Automated Terraform definitions; Manual `terraform apply` by engineer.
* **Validation Method**: `terraform fmt -check`, `terraform validate`.
* **Acceptance Criteria**: Clean `terraform validate` across all environments; zero hardcoded secrets.

---

### Phase 9 — Multi-Stage CI/CD Pipeline
* **Finding**: CI only runs `pytest` and masks `dbt parse` errors.
* **Current Implementation**: `.github/workflows/ci.yml`.
* **Required Change**:
  1. Build multi-stage pipeline:
     - Stage 1 (PR): Lint (Ruff), Security Scan (Gitleaks), Unit & Contract Tests (pytest), dbt parse & compile, Terraform fmt & validate.
     - Stage 2 (Merge to main): Terraform Plan Staging, Deploy DAGs/artifacts to Staging MWAA, Run Staging Integration Tests.
     - Stage 3 (Prod Promotion): Manual Environment Approval Gate, Terraform Plan/Apply Prod, Deploy DAGs to Prod MWAA, Run Post-Deployment Verification.
  2. Implement AWS authentication via GitHub Actions OIDC (IAM Role assumption), avoiding long-lived static AWS keys.
* **Files Affected**:
  - `.github/workflows/ci.yml`
  - `.github/workflows/deploy.yml`
* **AWS Resources Required**: IAM Role with OIDC Trust Policy for GitHub Actions.
* **Task Type**: Automated workflow code + Manual AWS IAM OIDC configuration.
* **Validation Method**: GitHub Actions schema validation, local syntax linting.
* **Acceptance Criteria**: Multi-stage workflow defined with security scan, tests, OIDC, and manual approval gates.

---

### Phase 10 — MWAA Deployment Packaging & Mechanism
* **Finding**: Ingestion modules and DAGs assume local filesystem paths; no automated packaging for MWAA.
* **Current Implementation**: Docker-compose volume mounts.
* **Required Change**:
  1. Create MWAA deployment packaging script `scripts/deploy_mwaa.py` / `scripts/package_mwaa.sh`:
     - Packages `src/` as a wheel or zip library.
     - Syncs `dags/` to `s3://<mwaa-bucket>/dags/`.
     - Syncs `dbt/` project to `s3://<mwaa-bucket>/dbt/`.
     - Validates DAG parsing locally before upload.
  2. Provide clean `mwaa_requirements.txt` tailored to AWS MWAA 2.8+ constraints.
* **Files Affected**:
  - `scripts/deploy_mwaa.py`
  - `mwaa_requirements.txt`
  - `dags/customer360_batch.py` (MWAA path resolution compatibility)
* **AWS Resources Required**: MWAA S3 bucket (`s3://customer360-mwaa-prod/`).
* **Task Type**: Automated.
* **Validation Method**: Test packaging script locally; verify DAG import works with mock Airflow environment.
* **Acceptance Criteria**: DAGs and internal modules package cleanly and pass local DAG parsing check.

---

### Phase 11 & 12 — Documentation & Production Runbooks
* **Required Documents**:
  1. `PRODUCTION_IMPLEMENTATION_PLAN.md` (this file)
  2. `PRODUCTION_IMPLEMENTATION_REPORT.md` (detailed completion report per phase)
  3. `docs/PRODUCTION_DEPLOYMENT_RUNBOOK.md` (actionable runbook separating CI/CD automation from manual actions)
  4. `docs/UPDATED_ARCHITECTURE.md` (updated end-to-end architecture documentation)
  5. `docs/MANUAL_PREREQUISITES.md` (numbered checklist of manual engineer actions)
