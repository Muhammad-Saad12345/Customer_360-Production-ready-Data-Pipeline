from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BATCH_DAG_FILE = ROOT / "dags" / "customer360_batch.py"
HOURLY_DAG_FILE = ROOT / "dags" / "customer360_hourly.py"
OBSOLETE_OMS_DAG = ROOT / "dags" / "customer360_oms_phase1.py"
OBSOLETE_FULL_LOAD_DAG = ROOT / "dags" / "customer360_full_load.py"


def test_authoritative_dags_exist():
    assert BATCH_DAG_FILE.exists(), "customer360_batch DAG must exist"
    assert HOURLY_DAG_FILE.exists(), "customer360_hourly_ingestion DAG must exist"


def test_redundant_dags_are_removed():
    assert not OBSOLETE_OMS_DAG.exists(), "customer360_oms_phase1 DAG must be removed to avoid duplication"
    assert not OBSOLETE_FULL_LOAD_DAG.exists(), "customer360_full_load DAG must be removed to avoid stranded raw data"


def test_customer360_batch_dag_structure():
    text = BATCH_DAG_FILE.read_text(encoding="utf-8")
    required = [
        'dag_id="customer360_batch"',
        'task_id="ingest_hubspot"',
        'task_id="validate_and_write_silver_hubspot"',
        'task_id="quality_check_hubspot"',
        'task_id="load_iceberg_hubspot"',
        'task_id="ingest_oms"',
        'task_id="validate_and_write_silver_oms"',
        'task_id="quality_check_oms"',
        'task_id="load_iceberg_oms"',
        'task_id="ingest_pim"',
        'task_id="validate_and_write_silver_pim"',
        'task_id="quality_check_pim"',
        'task_id="load_iceberg_pim"',
        'task_id="ingest_loyalty"',
        'task_id="validate_and_write_silver_loyalty"',
        'task_id="quality_check_loyalty"',
        'task_id="load_iceberg_loyalty"',
        'task_id="load_iceberg_marketing"',
        'task_id="ingest_zendesk"',
        'task_id="quality_check_zendesk"',
        # Tier-5 & Tier-6 tasks
        'task_id="dbt_run"',
        'task_id="dbt_test"',
        'task_id="iceberg_maintenance"',
    ]
    for expected in required:
        assert expected in text, f"Missing batch DAG assertion: {expected}"

    # Verify Tier-5 fan-in wiring (all 6 Iceberg loads feed dbt)
    assert ">> dbt_run >> dbt_test >> iceberg_maintenance" in text, (
        "Batch DAG must wire all Iceberg loads into dbt_run -> dbt_test -> iceberg_maintenance"
    )
    # Regression guards: HubSpot owns contacts ONLY
    assert "hubspot_orders" not in text, "Batch DAG must NOT ingest hubspot orders"
    assert "hubspot_products" not in text, "Batch DAG must NOT ingest hubspot products"


def test_customer360_hourly_dag_structure():
    text = HOURLY_DAG_FILE.read_text(encoding="utf-8")
    required = [
        'dag_id="customer360_hourly_ingestion"',
        'task_id="ingest_hubspot"',
        'task_id="validate_and_write_silver_hubspot"',
        'task_id="quality_check_hubspot"',
        'task_id="load_iceberg_hubspot"',
    ]
    for expected in required:
        assert expected in text, f"Missing hourly DAG assertion: {expected}"

    # Verify complete 4-tier lakehouse promotion
    assert "hubspot_ingest >> hubspot_validate >> hubspot_quality >> hubspot_iceberg" in text, (
        "Hourly DAG must promote through all 4 tiers (no stranded raw data)"
    )
    assert "hubspot_orders" not in text
    assert "hubspot_products" not in text
