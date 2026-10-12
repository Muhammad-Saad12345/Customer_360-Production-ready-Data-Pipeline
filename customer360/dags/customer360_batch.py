from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

# ---------------------------------------------------------------------------
# Rejection-rate policy thresholds (overridable via environment variables)
# ---------------------------------------------------------------------------
# DQ_REJECT_HARD_PCT  — raise an exception and block the Iceberg load
# DQ_REJECT_WARN_PCT  — emit a WARNING log but allow the pipeline to continue
# ---------------------------------------------------------------------------
_DQ_REJECT_HARD_PCT: float = float(os.environ.get("DQ_REJECT_HARD_PCT", "10.0"))
_DQ_REJECT_WARN_PCT: float = float(os.environ.get("DQ_REJECT_WARN_PCT", "5.0"))

# Ensure DAG directory and repository root are in sys.path for MWAA and local compatibility
_dag_dir = Path(__file__).resolve().parent
_repo_dir = _dag_dir.parent
for _p in [str(_dag_dir), str(_repo_dir)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingestion.hubspot import HubSpotIngest
from src.ingestion.loyalty import LoyaltyIngest
from src.ingestion.marketing import MarketingIngest
from src.ingestion.oms import OmsIngest
from src.ingestion.pim import PimIngest
from src.ingestion.support import ZendeskIngest
from src.lakehouse.iceberg_loader import IcebergLoader
from src.monitoring.airflow_callbacks import on_task_failure, on_task_success
from src.processing.silver_gate import HubSpotSilverProcessor


def _source_metadata(source: str, entity: str, run_id: str, status: str = "success", **extra: Any) -> Dict[str, Any]:
    payload = {
        "source": source,
        "entity": entity,
        "run_id": run_id,
        "status": status,
    }
    payload.update(extra)
    return payload


def _ingest_and_return(ingest_callable, source: str, entity: str, **context: Any) -> Dict[str, Any]:
    ingester = ingest_callable()
    ingester.run()
    return _source_metadata(source, entity, ingester.run_id)


def _ingest_hubspot(source: str = "hubspot", entity: str = "contacts", **context: Any) -> Dict[str, Any]:
    # HubSpot is CONTACTS ONLY.
    # Orders are owned by OMS. Products are owned by PIM.
    # Do NOT call run_orders() or run_products() here.
    hubspot = HubSpotIngest()
    hubspot.run()
    return _source_metadata(source, entity, hubspot.run_id)


def _silver_gate(source: str, entity: str, **context: Any) -> Dict[str, Any]:
    ti = context["ti"]
    upstream = ti.xcom_pull(task_ids=f"ingest_{source}")
    if not upstream and source in ("support", "zendesk"):
        upstream = ti.xcom_pull(task_ids="ingest_zendesk") or ti.xcom_pull(task_ids="ingest_support")
    upstream = upstream or {}
    run_id = upstream.get("run_id")
    if not run_id:
        raise ValueError(f"No run_id available for {source}/{entity}")

    processor = HubSpotSilverProcessor()
    summary = processor.process_records(source=source, entity=entity, run_id=run_id)
    return {
        "source": source,
        "entity": entity,
        "run_id": run_id,
        "input_record_count": summary["input_record_count"],
        "valid_count": summary["valid_count"],
        "invalid_count": summary["invalid_count"],
        "silver_path": summary["silver_path"],
        "quarantine_path": summary["quarantine_path"],
        "status": "success",
    }


def _quality_gate(source: str, entity: str, **context: Any) -> Dict[str, Any]:
    """Enforce rejection-rate policy and emit CloudWatch DQ metrics.

    Thresholds (configurable via environment variables):
      DQ_REJECT_HARD_PCT (default 10%) — blocks Iceberg promotion, raises error.
      DQ_REJECT_WARN_PCT (default  5%) — logs WARNING, continues.
    """
    from src.monitoring.cloudwatch import emit_dq_gate_verdict, emit_dq_metrics  # lazy import

    ti = context["ti"]
    run_id = context.get("run_id") or (ti.run_id if hasattr(ti, "run_id") else None)

    upstream = ti.xcom_pull(task_ids=f"validate_and_write_silver_{source}")
    if not upstream and source in ("support", "zendesk"):
        upstream = (
            ti.xcom_pull(task_ids="validate_and_write_silver_zendesk")
            or ti.xcom_pull(task_ids="validate_and_write_silver_support")
        )
    upstream = upstream or {}

    input_count = int(upstream.get("input_record_count") or 0)
    valid_count = int(upstream.get("valid_count") or 0)
    invalid_count = int(upstream.get("invalid_count") or 0)
    silver_path = upstream.get("silver_path")
    upstream_run_id = upstream.get("run_id") or run_id

    # ── Guard: Silver path must exist ──────────────────────────────────────
    if not silver_path:
        emit_dq_gate_verdict(source, entity, passed=False, rejection_rate_pct=100.0, run_id=upstream_run_id)
        raise ValueError(f"{source}/{entity} Silver path is empty — upstream silver task may have failed")

    # ── Guard: Count bookkeeping integrity ─────────────────────────────────
    if input_count and (valid_count + invalid_count) != input_count:
        emit_dq_gate_verdict(source, entity, passed=False, rejection_rate_pct=100.0, run_id=upstream_run_id)
        raise ValueError(
            f"{source}/{entity} quality count mismatch: "
            f"input={input_count}, valid={valid_count}, invalid={invalid_count}"
        )

    # ── Emit DQ metrics regardless of outcome ──────────────────────────────
    emit_dq_metrics(
        source=source,
        entity=entity,
        input_count=input_count,
        valid_count=valid_count,
        invalid_count=invalid_count,
        tier="quality",
        run_id=upstream_run_id,
    )

    # ── Rejection-rate policy ───────────────────────────────────────────────
    rejection_rate_pct = (invalid_count / input_count * 100.0) if input_count else 0.0

    if rejection_rate_pct >= _DQ_REJECT_HARD_PCT:
        emit_dq_gate_verdict(source, entity, passed=False, rejection_rate_pct=rejection_rate_pct, run_id=upstream_run_id)
        raise ValueError(
            f"{source}/{entity} DQ gate FAILED: rejection rate {rejection_rate_pct:.2f}% "
            f">= hard threshold {_DQ_REJECT_HARD_PCT:.1f}% "
            f"(invalid={invalid_count}, input={input_count}). "
            f"Iceberg promotion blocked."
        )

    if rejection_rate_pct >= _DQ_REJECT_WARN_PCT:
        logging.warning(
            "%s/%s DQ gate WARNING: rejection rate %.2f%% >= soft threshold %.1f%% "
            "(invalid=%d, input=%d)",
            source, entity, rejection_rate_pct, _DQ_REJECT_WARN_PCT, invalid_count, input_count,
        )

    emit_dq_gate_verdict(source, entity, passed=True, rejection_rate_pct=rejection_rate_pct, run_id=upstream_run_id)

    return {
        "source": source,
        "entity": entity,
        "run_id": upstream_run_id,
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "rejection_rate_pct": round(rejection_rate_pct, 4),
        "silver_path": silver_path,
        "status": "success",
    }


def _load_iceberg_gate(source: str, entity: str, **context: Any) -> Dict[str, Any]:
    ti = context["ti"]
    upstream = ti.xcom_pull(task_ids=f"quality_check_{source}")
    if not upstream and source in ("support", "zendesk"):
        upstream = ti.xcom_pull(task_ids="quality_check_zendesk") or ti.xcom_pull(task_ids="quality_check_support")
    upstream = upstream or {}
    run_id = upstream.get("run_id")
    valid_count = int(upstream.get("valid_count") or 0)

    if not run_id:
        raise ValueError(f"No run_id available for Iceberg load of {source}/{entity}")

    if valid_count == 0:
        return {
            "source": source,
            "entity": entity,
            "run_id": run_id,
            "status": "skipped",
            "reason": "zero_valid_records",
        }

    loader = IcebergLoader()
    summary = loader.load_silver_run(source=source, entity=entity, run_id=run_id)
    return summary


def _resolve_dbt_project_dir() -> str:
    if "DBT_PROJECT_DIR" in os.environ:
        return os.environ["DBT_PROJECT_DIR"]
    sibling_dbt = Path(__file__).resolve().parent / "dbt"
    if sibling_dbt.exists() and (sibling_dbt / "dbt_project.yml").exists():
        return str(sibling_dbt)
    parent_dbt = Path(__file__).resolve().parents[1] / "dbt"
    if parent_dbt.exists() and (parent_dbt / "dbt_project.yml").exists():
        return str(parent_dbt)
    cwd_dbt = Path.cwd() / "dbt"
    if cwd_dbt.exists() and (cwd_dbt / "dbt_project.yml").exists():
        return str(cwd_dbt)
    return str(sibling_dbt if sibling_dbt.exists() else parent_dbt)


def _resolve_dbt_executable() -> Optional[str]:
    import shutil
    found = shutil.which("dbt")
    if found:
        return found
    candidates = [
        Path(__file__).resolve().parents[1] / "dbt_venv" / "Scripts" / "dbt.exe",
        Path(__file__).resolve().parents[1] / "dbt_venv" / "Scripts" / "dbt",
        Path(__file__).resolve().parents[1] / "dbt_venv" / "bin" / "dbt",
        Path(__file__).resolve().parents[1] / ".venv" / "Scripts" / "dbt.exe",
        Path(__file__).resolve().parents[1] / ".venv" / "bin" / "dbt",
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return None


def _run_dbt(**context: Any) -> Dict[str, Any]:
    """Run all dbt models using the dbt interpreter."""
    dbt_executable = _resolve_dbt_executable()
    dbt_project_dir = _resolve_dbt_project_dir()
    project_dir = Path(dbt_project_dir)

    if not project_dir.exists() or not (project_dir / "dbt_project.yml").exists():
        logging.warning("dbt project directory not found at %s. Skipping dbt run in test/container mode.", dbt_project_dir)
        return {"status": "skipped", "reason": "dbt_project_dir_not_found"}

    if not dbt_executable:
        logging.warning("dbt executable not found in PATH or virtual environments. Skipping dbt run in container.")
        return {"status": "skipped", "reason": "dbt_not_installed"}

    env = os.environ.copy()
    env.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    # Load .env if present
    env_file = Path(__file__).resolve().parents[1] / ".env"
    if env_file.exists():
        try:
            from dotenv import dotenv_values
            env.update({k: v for k, v in dotenv_values(env_file).items() if v is not None})
        except ImportError:
            pass

    dbt_target = os.environ.get("DBT_TARGET", "dev")

    cmd = [
        dbt_executable,
        "run",
        "--project-dir", dbt_project_dir,
        "--profiles-dir", dbt_project_dir,
        "--target", dbt_target,
        "--no-use-colors",
        "--no-partial-parse",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800, env=env)
    stdout = result.stdout[-4000:] if result.stdout else ""
    stderr = result.stderr[-2000:] if result.stderr else ""
    if result.returncode != 0:
        raise RuntimeError(
            f"dbt run failed (exit {result.returncode}).\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        )
    return {"status": "success", "returncode": result.returncode, "stdout_tail": stdout}


def _run_dbt_tests(**context: Any) -> Dict[str, Any]:
    """Run dbt schema/data tests after models materialize."""
    dbt_executable = _resolve_dbt_executable()
    dbt_project_dir = _resolve_dbt_project_dir()
    project_dir = Path(dbt_project_dir)

    if not project_dir.exists() or not (project_dir / "dbt_project.yml").exists():
        logging.warning("dbt project directory not found at %s. Skipping dbt test.", dbt_project_dir)
        return {"status": "skipped", "reason": "dbt_project_dir_not_found"}

    if not dbt_executable:
        logging.warning("dbt executable not found in PATH or virtual environments. Skipping dbt test in container.")
        return {"status": "skipped", "reason": "dbt_not_installed"}

    env = os.environ.copy()
    env.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    env_file = Path(__file__).resolve().parents[1] / ".env"
    if env_file.exists():
        try:
            from dotenv import dotenv_values
            env.update({k: v for k, v in dotenv_values(env_file).items() if v is not None})
        except ImportError:
            pass

    dbt_target = os.environ.get("DBT_TARGET", "dev")

    cmd = [
        dbt_executable,
        "test",
        "--project-dir", dbt_project_dir,
        "--profiles-dir", dbt_project_dir,
        "--target", dbt_target,
        "--no-use-colors",
        "--no-partial-parse",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=900, env=env)
    stdout = result.stdout[-4000:] if result.stdout else ""
    stderr = result.stderr[-2000:] if result.stderr else ""
    if result.returncode != 0:
        raise RuntimeError(
            f"dbt test failed (exit {result.returncode}).\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        )
    return {"status": "success", "returncode": result.returncode, "stdout_tail": stdout}


def _run_iceberg_maintenance(**context: Any) -> Dict[str, Any]:
    """Run OPTIMIZE + VACUUM on all active Iceberg tables via Athena."""
    from src.lakehouse.iceberg_maintenance import IcebergMaintenanceManager

    manager = IcebergMaintenanceManager()
    tables = [
        "contacts",
        "orders",
        "products",
        "loyalty",
        "campaign_events",
        "support_tickets",
    ]
    results = {}
    for table in tables:
        try:
            results[table] = manager.run_full_maintenance(table)
        except Exception as exc:  # noqa: BLE001
            # Non-fatal: maintenance errors should not block pipeline.
            results[table] = {"status": "error", "error": str(exc)}
    return {"status": "complete", "tables": results}


def _build_task(task_id: str, source: str, entity: str, ingester_factory):
    ingest_task = PythonOperator(
        task_id=f"ingest_{source}",
        python_callable=lambda ingester_factory=ingester_factory, source=source, entity=entity, **context: _ingest_and_return(ingester_factory, source, entity, **context),
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
    )
    validate_task = PythonOperator(
        task_id=f"validate_and_write_silver_{source}",
        python_callable=lambda source=source, entity=entity, **context: _silver_gate(source, entity, **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
    )
    quality_task = PythonOperator(
        task_id=f"quality_check_{source}",
        python_callable=lambda source=source, entity=entity, **context: _quality_gate(source, entity, **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
    )
    return ingest_task, validate_task, quality_task


with DAG(
    dag_id="customer360_batch",
    description="Local batch orchestration for the active Customer 360 sources",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        "owner": "Saad",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "email_on_failure": False,
        "email_on_retry": False,
        "on_success_callback": on_task_success,
        "on_failure_callback": on_task_failure,
    },
    tags=["customer360", "batch", "airflow", "s3"],
) as dag:
    hubspot_ingest = PythonOperator(
        task_id="ingest_hubspot",
        python_callable=_ingest_hubspot,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
    )
    hubspot_validate = PythonOperator(
        task_id="validate_and_write_silver_hubspot",
        python_callable=lambda **context: _silver_gate("hubspot", "contacts", **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
    )
    hubspot_quality = PythonOperator(
        task_id="quality_check_hubspot",
        python_callable=lambda **context: _quality_gate("hubspot", "contacts", **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
    )

    oms_ingest = PythonOperator(
        task_id="ingest_oms",
        python_callable=lambda **context: _ingest_and_return(lambda: OmsIngest(), "oms", "orders", **context),
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
    )
    oms_validate = PythonOperator(
        task_id="validate_and_write_silver_oms",
        python_callable=lambda **context: _silver_gate("oms", "orders", **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
    )
    oms_quality = PythonOperator(
        task_id="quality_check_oms",
        python_callable=lambda **context: _quality_gate("oms", "orders", **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
    )

    pim_ingest = PythonOperator(
        task_id="ingest_pim",
        python_callable=lambda **context: _ingest_and_return(lambda: PimIngest(), "pim", "products", **context),
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
    )
    pim_validate = PythonOperator(
        task_id="validate_and_write_silver_pim",
        python_callable=lambda **context: _silver_gate("pim", "products", **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
    )
    pim_quality = PythonOperator(
        task_id="quality_check_pim",
        python_callable=lambda **context: _quality_gate("pim", "products", **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
    )

    loyalty_ingest = PythonOperator(
        task_id="ingest_loyalty",
        python_callable=lambda **context: _ingest_and_return(lambda: LoyaltyIngest(), "loyalty", "loyalty", **context),
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
    )
    loyalty_validate = PythonOperator(
        task_id="validate_and_write_silver_loyalty",
        python_callable=lambda **context: _silver_gate("loyalty", "loyalty", **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
    )
    loyalty_quality = PythonOperator(
        task_id="quality_check_loyalty",
        python_callable=lambda **context: _quality_gate("loyalty", "loyalty", **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
    )

    hubspot_iceberg = PythonOperator(
        task_id="load_iceberg_hubspot",
        python_callable=lambda **context: _load_iceberg_gate("hubspot", "contacts", **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
    )
    oms_iceberg = PythonOperator(
        task_id="load_iceberg_oms",
        python_callable=lambda **context: _load_iceberg_gate("oms", "orders", **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
    )
    pim_iceberg = PythonOperator(
        task_id="load_iceberg_pim",
        python_callable=lambda **context: _load_iceberg_gate("pim", "products", **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
    )
    loyalty_iceberg = PythonOperator(
        task_id="load_iceberg_loyalty",
        python_callable=lambda **context: _load_iceberg_gate("loyalty", "loyalty", **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
    )

    marketing_ingest, marketing_validate, marketing_quality = _build_task("marketing", "marketing", "campaign_events", lambda: MarketingIngest())
    marketing_iceberg = PythonOperator(
        task_id="load_iceberg_marketing",
        python_callable=lambda **context: _load_iceberg_gate("marketing", "campaign_events", **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
    )
    zendesk_ingest = PythonOperator(
        task_id="ingest_zendesk",
        python_callable=lambda **context: _ingest_and_return(lambda: ZendeskIngest(), "support", "tickets", **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
    )
    zendesk_validate = PythonOperator(
        task_id="validate_and_write_silver_zendesk",
        python_callable=lambda **context: _silver_gate("support", "tickets", **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
    )
    zendesk_quality = PythonOperator(
        task_id="quality_check_zendesk",
        python_callable=lambda **context: _quality_gate("support", "tickets", **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
    )
    zendesk_iceberg = PythonOperator(
        task_id="load_iceberg_zendesk",
        python_callable=lambda **context: _load_iceberg_gate("support", "tickets", **context),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
    )

    for source_task in [
        hubspot_ingest,
        oms_ingest,
        pim_ingest,
        loyalty_ingest,
        marketing_ingest,
        zendesk_ingest,
        hubspot_iceberg,
        oms_iceberg,
        pim_iceberg,
        loyalty_iceberg,
        marketing_iceberg,
        zendesk_iceberg,
    ]:
        source_task

    # ── Tier-1: Ingest → Tier-2: Silver Validation → Tier-3: Quality → Tier-4: Iceberg ──
    hubspot_ingest >> hubspot_validate >> hubspot_quality >> hubspot_iceberg
    oms_ingest >> oms_validate >> oms_quality >> oms_iceberg
    pim_ingest >> pim_validate >> pim_quality >> pim_iceberg
    loyalty_ingest >> loyalty_validate >> loyalty_quality >> loyalty_iceberg
    marketing_ingest >> marketing_validate >> marketing_quality >> marketing_iceberg
    zendesk_ingest >> zendesk_validate >> zendesk_quality >> zendesk_iceberg

    # ── Tier-5: dbt (fan-in from all 6 Iceberg loads) ──────────────────────────
    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=_run_dbt,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=60),
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=_run_dbt_tests,
        provide_context=True,
        retries=0,
        execution_timeout=timedelta(minutes=30),
    )

    # ── Tier-6: Iceberg maintenance (compaction + vacuum) ───────────────────────
    iceberg_maintenance = PythonOperator(
        task_id="iceberg_maintenance",
        python_callable=_run_iceberg_maintenance,
        provide_context=True,
        retries=0,
        execution_timeout=timedelta(minutes=30),
        trigger_rule="all_done",  # run even if dbt_test has warnings
    )

    [
        hubspot_iceberg,
        oms_iceberg,
        pim_iceberg,
        loyalty_iceberg,
        marketing_iceberg,
        zendesk_iceberg,
    ] >> dbt_run >> dbt_test >> iceberg_maintenance
