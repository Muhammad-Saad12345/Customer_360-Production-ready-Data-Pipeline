import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict

# Ensure DAG directory and repository root are in sys.path for MWAA and local compatibility
_dag_dir = Path(__file__).resolve().parent
_repo_dir = _dag_dir.parent
for _p in [str(_dag_dir), str(_repo_dir)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingestion.hubspot import HubSpotIngest
from src.processing.silver_gate import HubSpotSilverProcessor
from src.lakehouse.iceberg_loader import IcebergLoader


def _ingest_hubspot_hourly(**context: Any) -> Dict[str, Any]:
    """
    Incremental hourly HubSpot customer contact ingestion.
    HubSpot is CONTACTS ONLY. Orders are owned by OMS; Products are owned by PIM.
    """
    hubspot = HubSpotIngest()
    hubspot.run()
    return {
        "source": "hubspot",
        "entity": "contacts",
        "run_id": hubspot.run_id,
        "status": "success",
    }


def _silver_gate_hourly(**context: Any) -> Dict[str, Any]:
    """Validate raw records and write clean Silver Parquet; route bad records to quarantine."""
    ti = context["ti"]
    upstream = ti.xcom_pull(task_ids="ingest_hubspot") or {}
    run_id = upstream.get("run_id")
    if not run_id:
        raise ValueError("No run_id available from upstream ingest_hubspot task")

    processor = HubSpotSilverProcessor()
    summary = processor.process_records(source="hubspot", entity="contacts", run_id=run_id)
    return {
        "source": "hubspot",
        "entity": "contacts",
        "run_id": run_id,
        "input_record_count": summary["input_record_count"],
        "valid_count": summary["valid_count"],
        "invalid_count": summary["invalid_count"],
        "silver_path": summary["silver_path"],
        "quarantine_path": summary["quarantine_path"],
        "status": "success",
    }


def _quality_gate_hourly(**context: Any) -> Dict[str, Any]:
    """Enforce data quality gates and reconciliation before lakehouse promotion."""
    ti = context["ti"]
    upstream = ti.xcom_pull(task_ids="validate_and_write_silver_hubspot") or {}
    input_count = int(upstream.get("input_record_count") or 0)
    valid_count = int(upstream.get("valid_count") or 0)
    invalid_count = int(upstream.get("invalid_count") or 0)
    silver_path = upstream.get("silver_path")

    if not silver_path:
        raise ValueError("HubSpot contacts Silver path is empty")
    if input_count and (valid_count + invalid_count) != input_count:
        raise ValueError(
            f"HubSpot contacts quality mismatch: input={input_count}, valid={valid_count}, invalid={invalid_count}"
        )

    return {
        "source": "hubspot",
        "entity": "contacts",
        "run_id": upstream.get("run_id"),
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "silver_path": silver_path,
        "status": "success",
    }


def _load_iceberg_hourly(**context: Any) -> Dict[str, Any]:
    """Controlled promotion into Iceberg lakehouse layer via Athena MERGE INTO."""
    ti = context["ti"]
    upstream = ti.xcom_pull(task_ids="quality_check_hubspot") or {}
    run_id = upstream.get("run_id")
    valid_count = int(upstream.get("valid_count") or 0)

    if not run_id:
        raise ValueError("No run_id available for Iceberg load of hubspot/contacts")

    if valid_count == 0:
        return {
            "source": "hubspot",
            "entity": "contacts",
            "run_id": run_id,
            "status": "skipped",
            "reason": "zero_valid_records",
        }

    loader = IcebergLoader()
    summary = loader.load_silver_run(source="hubspot", entity="contacts", run_id=run_id)
    return summary


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="customer360_hourly_ingestion",
    default_args=default_args,
    description="Automated hourly Customer 360 contact pipeline: Raw -> Silver -> Quality -> Iceberg",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["customer360", "hourly", "hubspot", "iceberg", "aws"],
) as dag:
    hubspot_ingest = PythonOperator(
        task_id="ingest_hubspot",
        python_callable=_ingest_hubspot_hourly,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )
    hubspot_validate = PythonOperator(
        task_id="validate_and_write_silver_hubspot",
        python_callable=_silver_gate_hourly,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )
    hubspot_quality = PythonOperator(
        task_id="quality_check_hubspot",
        python_callable=_quality_gate_hourly,
        provide_context=True,
        execution_timeout=timedelta(minutes=15),
    )
    hubspot_iceberg = PythonOperator(
        task_id="load_iceberg_hubspot",
        python_callable=_load_iceberg_hourly,
        provide_context=True,
        execution_timeout=timedelta(minutes=15),
    )

    # Complete 4-tier Lakehouse pipeline — no stranded raw data
    hubspot_ingest >> hubspot_validate >> hubspot_quality >> hubspot_iceberg
