# Customer 360 — Deployment Runbook & Troubleshooting Guide

**Environment:** Staging & Production  
**Region:** `us-east-1`  
**Account:** `734143568343`  

---

## 1. Repeatable Deployment & Synchronization Procedure

### Step 1: Pre-Deployment Validation
Run the local validation suite:
```bash
# Verify unit & integration test suite (211/211 must pass)
pytest -q

# Test isolated staging connectivity
python -c "import os; os.environ['ENVIRONMENT']='staging'; from dags.customer360_healthcheck import test_aws_connectivity; print(test_aws_connectivity())"
```

### Step 2: S3 Artifact Synchronization
Sync DAGs, source packages, dbt models, and requirements to the MWAA artifacts bucket:
```bash
python scripts/sync_mwaa_artifacts.py
```
This ensures:
* `requirements.txt` has `--constraint` on **Line 1**.
* All DAGs ([customer360_batch.py](file:///d:/Data%20Engineer%20Projects/Customer%20360/customer360/dags/customer360_batch.py), [customer360_hourly.py](file:///d:/Data%20Engineer%20Projects/Customer%20360/customer360/dags/customer360_hourly.py), [customer360_healthcheck.py](file:///d:/Data%20Engineer%20Projects/Customer%20360/customer360/dags/customer360_healthcheck.py)) are updated.
* `dags/src/` and `dags/dbt/` are refreshed.

### Step 3: Update MWAA Requirements Version (If Environment Running)
When updating `requirements.txt`, update the environment to point to the new S3 object version:
```bash
aws mwaa update-environment \
  --name c360-airflow-production-final \
  --requirements-s3-path requirements.txt \
  --requirements-s3-object-version FnQG51Bs8QeC3HlNvLX4znTLUVfP2Dnf
```

### Step 4: Staging Validation & Production Gating
1. Trigger DAG `customer360_mwaa_healthcheck` in staging.
2. Trigger bounded run of `customer360_batch` in staging.
3. Validate row counts and Athena query results in `customer360_iceberg_stage`.
4. Only promote to production once staging run completes with 0 errors.

---

## 2. Troubleshooting Guide

### Issue A: Dependency Installation Failure (`ModuleNotFoundError: No module named 'pyarrow'`)
* **Symptom:** Airflow scheduler or worker logs show `ModuleNotFoundError: No module named 'pyarrow'` during DAG processing.
* **Root Cause:** 
  1. The `--constraint` line was not on **Line 1** of `requirements.txt`.
  2. `pyarrow>=14.0.0,<16.0.0` conflicted with the official Airflow constraints file which pinned `pyarrow==18.1.0`. Pip aborted with `ResolutionImpossible`, failing the entire package installation.
* **Resolution:** Ensure `--constraint` is on Line 1 and use compatible bounds: `pyarrow>=14.0.0`.

### Issue B: AssumeRole Access Denied on `C360-Antigravity-DeployRole`
* **Symptom:** `sts:AssumeRole` fails with `AccessDenied` on `C360-Antigravity-DeployRole`.
* **Root Cause:** The role's trust policy lists `"Principal": { "AWS": "arn:aws:iam::734143568343:root" }`, but the caller identity `arn:aws:iam::734143568343:user/C360` does not have an identity-based policy granting `sts:AssumeRole`.
* **Resolution:** 
  * Administrator updates the trust policy of `C360-Antigravity-DeployRole` to explicitly include `arn:aws:iam::734143568343:user/C360` as a trusted principal.

### Issue C: GitHub Actions OIDC Authentication Failure
* **Symptom:** GitHub Actions job fails during `configure-aws-credentials`.
* **Root Cause:** `github-mwaa-deploy-role` trust policy condition specifies `repo:Customer_360-Production-ready-Data-Pipeline/*` without owner `Muhammad-Saad12345/`.
* **Resolution:** Update the trust condition to `repo:Muhammad-Saad12345/Customer_360-Production-ready-Data-Pipeline:*`.

---

## 3. Rollback Procedures

### Rollback S3 Artifacts
```bash
# Revert requirements.txt or DAGs using S3 Object Versions
aws s3api copy-object \
  --bucket c360-mwaa-prod-786 \
  --copy-source c360-mwaa-prod-786/requirements.txt?versionId=<LAST_KNOWN_GOOD_VERSION> \
  --key requirements.txt
```

### Rollback Iceberg Lakehouse Tables
```sql
-- Query table snapshots in Athena
SELECT * FROM customer360_iceberg_stage."hubspot_contacts$snapshots" ORDER BY committed_at DESC;

-- Rollback to prior snapshot
CALL customer360_iceberg_stage.system.rollback_to_snapshot('hubspot_contacts', <SNAPSHOT_ID>);
```
