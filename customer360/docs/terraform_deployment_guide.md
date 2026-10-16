# Customer 360 Terraform Deployment Guide

## 1. Prerequisites

- **Terraform:** Version `1.7.5` or higher installed.
- **AWS Permissions:** Operational access to AWS Account `734143568343` (via AWS CloudShell or GitHub Actions OIDC).
- **Python:** Python 3.10+ with standard virtual environment (`.venv`).

---

## 2. Directory Structure

```
customer360/
├── terraform/
│   ├── modules/
│   │   ├── core/            # S3 Lakehouse & Artifacts, Glue DB, Athena, KMS
│   │   ├── iam/             # Service execution & OIDC deploy roles
│   │   ├── lambda/          # 8 Serverless pipeline handlers & log groups
│   │   ├── glue_jobs/       # dbt Runner Python Shell job (1/16 DPU)
│   │   ├── step_functions/  # Standard State Machine orchestrator
│   │   ├── eventbridge/     # Daily schedule trigger
│   │   └── observability/   # Alarms & CloudWatch dashboards
│   └── environments/
│       ├── staging/         # Staging root configuration
│       └── prod/            # Production root configuration
```

---

## 3. Deployment Workflow

### Phase 1: Package Serverless Pipeline Artifacts

Before deploying Terraform, package the Lambda functions and dbt project:

```bash
# In repository root (customer360/)
python scripts/package_artifacts.py
```

This generates:
- `build/lambdas/*.zip` (8 Lambda zip archives with `handler.py` and `src/` modules)
- `build/artifacts/dbt/dbt_project.tar.gz` (bundled dbt models)
- `build/glue_jobs/dbt_runner.py` (staged Glue runner script)

---

### Phase 2: Upload Artifacts to S3

Upload the packaged bundles to your S3 bucket:

```bash
# Set your lakehouse/artifacts bucket name
export S3_BUCKET="c360-bucket786"

# Sync Lambda deployment zips
aws s3 sync build/lambdas/ s3://${S3_BUCKET}/artifacts/lambdas/

# Upload dbt bundle and Glue script
aws s3 cp build/artifacts/dbt/dbt_project.tar.gz s3://${S3_BUCKET}/artifacts/dbt/dbt_project.tar.gz
aws s3 cp build/glue_jobs/dbt_runner.py s3://${S3_BUCKET}/glue_jobs/dbt_runner.py
```

---

### Phase 3: Terraform Plan & Apply (Staging)

Navigate to the staging environment directory:

```bash
cd terraform/environments/staging
```

#### Step 1: Initialize Terraform
```bash
terraform init
```

#### Step 2: Validate Configuration
```bash
terraform validate
```

#### Step 3: Review Infrastructure Plan
```bash
terraform plan -out=tfplan-staging.bin -var-file=terraform.tfvars
```
> [!IMPORTANT]
> Always inspect the plan output before applying to ensure no destructive actions are scheduled against existing S3 data or Iceberg tables.

#### Step 4: Apply Infrastructure Changes
```bash
terraform apply tfplan-staging.bin
```

---

### Phase 4: Deploying via GitHub Actions (Automated CI/CD)

The repository is configured for automated keyless deployment via GitHub Actions:

1. **Pull Requests to `main`:**
   - Automatically runs linting, unit tests, coverage checks, dbt parsing, and `terraform plan` for staging.
2. **Push / Merge to `main`:**
   - Packages artifacts, uploads to S3, executes `terraform apply` for staging, and runs an automated end-to-end integration test via Step Functions.
3. **Production Deployment (`deploy-prod`):**
   - Protected by a GitHub Environment manual approval gate (`production`). Once approved by an authorized reviewer, changes are applied to production.

---

## 4. Post-Deployment Verification

Verify that all resources have deployed properly:

```bash
# 1. Verify Step Functions State Machine exists and is active
aws stepfunctions list-state-machines --query "stateMachines[?contains(name, 'c360')]"

# 2. Verify all 8 Lambda functions are active
aws lambda list-functions --query "Functions[?contains(FunctionName, 'c360-staging')].FunctionName"

# 3. Verify Glue dbt runner job
aws glue get-job --job-name "c360-staging-dbt-runner"

# 4. Execute a dry-run test
aws stepfunctions start-execution \
  --state-machine-arn "$(terraform output -raw step_functions_state_machine_arn)" \
  --name "post-deploy-verify-$(date +%s)" \
  --input '{"environment": "staging", "dry_run": true}'
```
