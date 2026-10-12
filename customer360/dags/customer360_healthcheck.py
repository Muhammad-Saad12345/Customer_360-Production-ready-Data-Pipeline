"""Lightweight Amazon MWAA Healthcheck DAG for Customer 360 platform.

Validates:
1. Basic worker execution & python runtime
2. IAM execution role permissions: S3, Glue, Athena, Secrets Manager
3. Network routing to AWS APIs via VPC NAT Gateway
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    HAS_AIRFLOW = True
except ImportError:
    HAS_AIRFLOW = False

# Ensure DAG directory and repository root are in sys.path
_dag_dir = Path(__file__).resolve().parent
_repo_dir = _dag_dir.parent
for _p in [str(_dag_dir), str(_repo_dir)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)


default_args = {
    "owner": "customer360-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def test_aws_connectivity(**context: Any) -> Dict[str, Any]:
    """Validate S3, Glue, Athena, and Secrets Manager connectivity using IAM role."""
    import boto3

    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    bucket = os.environ.get("S3_BUCKET", "c360-bucket786")
    env = (os.environ.get("ENVIRONMENT") or "staging").lower()
    default_db = "customer360_iceberg_stage" if env in ("staging", "stage") else "customer360_iceberg"
    default_wg = "customer360-staging" if env in ("staging", "stage") else "primary"
    db = os.environ.get("ICEBERG_DATABASE", default_db)
    workgroup = os.environ.get("ATHENA_WORKGROUP", default_wg)
    secret_name = os.environ.get("SECRETS_MANAGER_SECRET_NAME", "c_360_secret_manager")

    results = {}
    failures = []

    # 1. S3 check
    try:
        s3 = boto3.client("s3", region_name=region)
        s3_resp = s3.list_objects_v2(Bucket=bucket, MaxKeys=5)
        key_count = s3_resp.get("KeyCount", 0)
        results["s3"] = f"OK: bucket='{bucket}', key_count={key_count}"
        logging.info("S3 connectivity check passed: bucket='%s', sample_keys=%d", bucket, key_count)
    except Exception as exc:
        err = f"S3 check failed for bucket='{bucket}': {exc}"
        results["s3"] = f"FAILED: {exc}"
        failures.append(err)
        logging.error("%s", err)

    # 2. Glue check
    try:
        glue = boto3.client("glue", region_name=region)
        glue_resp = glue.get_database(Name=db)
        db_name = glue_resp["Database"]["Name"]
        results["glue"] = f"OK: database='{db_name}'"
        logging.info("Glue Data Catalog connectivity check passed: database='%s'", db_name)
    except Exception as exc:
        err = f"Glue check failed for database='{db}': {exc}"
        results["glue"] = f"FAILED: {exc}"
        failures.append(err)
        logging.error("%s", err)

    # 3. Athena check
    try:
        athena = boto3.client("athena", region_name=region)
        athena_resp = athena.get_work_group(WorkGroup=workgroup)
        wg_name = athena_resp["WorkGroup"]["Name"]
        wg_state = athena_resp["WorkGroup"]["State"]
        results["athena"] = f"OK: workgroup='{wg_name}', state='{wg_state}'"
        logging.info("Athena connectivity check passed: workgroup='%s', state='%s'", wg_name, wg_state)
    except Exception as exc:
        err = f"Athena check failed for workgroup='{workgroup}': {exc}"
        results["athena"] = f"FAILED: {exc}"
        failures.append(err)
        logging.error("%s", err)

    # 4. Secrets Manager check (Metadata / availability only — never log secrets)
    try:
        sm = boto3.client("secretsmanager", region_name=region)
        try:
            sm_resp = sm.describe_secret(SecretId=secret_name)
            results["secrets_manager"] = f"OK: secret='{sm_resp['Name']}' (verified via describe_secret)"
            logging.info("Secrets Manager check passed: secret='%s' described", sm_resp["Name"])
        except Exception:
            # Fallback to get_secret_value metadata check if describe is restricted
            val_resp = sm.get_secret_value(SecretId=secret_name)
            has_val = bool(val_resp.get("SecretString") or val_resp.get("SecretBinary"))
            results["secrets_manager"] = f"OK: secret='{secret_name}' (verified via get_secret_value, payload_present={has_val})"
            logging.info("Secrets Manager check passed: secret='%s' retrieved value successfully", secret_name)
    except Exception as exc:
        err = f"Secrets Manager check failed for secret='{secret_name}': {exc}"
        results["secrets_manager"] = f"FAILED: {exc}"
        failures.append(err)
        logging.error("%s", err)

    logging.info("Customer 360 MWAA Healthcheck Summary: %s", results)

    if failures:
        error_msg = f"Customer 360 Healthcheck failed on {len(failures)} components: {'; '.join(failures)}"
        logging.error("%s", error_msg)
        raise RuntimeError(error_msg)

    return results


if HAS_AIRFLOW:
    with DAG(
        dag_id="customer360_mwaa_healthcheck",
        default_args=default_args,
        description="Customer 360 MWAA Environment & AWS IAM Health Check",
        schedule_interval=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["customer360", "healthcheck", "mwaa"],
    ) as dag:
        healthcheck_task = PythonOperator(
            task_id="check_aws_connectivity",
            python_callable=test_aws_connectivity,
        )
else:
    dag = None
