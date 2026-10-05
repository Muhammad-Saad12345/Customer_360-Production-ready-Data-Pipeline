"""Airflow callback functions for pipeline health monitoring.

These callbacks are designed to be set as on_success_callback and
on_failure_callback on Airflow DAG tasks. They:

  1. Persist run results to the JSONL log (pipeline_run_log.jsonl)
  2. Emit CloudWatch task outcome metrics (TaskSuccess / TaskFailure)
  3. Publish an SNS alert on failure (opt-in via C360_SNS_ALERT_TOPIC_ARN)

Usage in DAG default_args::

    default_args = {
        ...
        "on_success_callback": on_task_success,
        "on_failure_callback": on_task_failure,
    }
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger("customer360.monitoring")

# Direct import avoids needing the health API server to be running.
# Results are written to JSONL and the in-memory monitor if available.
_RUN_LOG = Path(os.environ.get(
    "C360_RUN_LOG",
    str(Path(__file__).resolve().parents[2] / "logs" / "pipeline_run_log.jsonl"),
))

# SNS topic ARN for pipeline failure alerts (optional)
_SNS_ALERT_TOPIC_ARN: Optional[str] = os.environ.get("C360_SNS_ALERT_TOPIC_ARN")

_ENV = os.environ.get("ENVIRONMENT", "dev").lower()


def _parse_task_metadata(context: Dict[str, Any]) -> Dict[str, Any]:
    """Extract source, entity, and tier from an Airflow task context."""
    task_id: str = context.get("task_instance", {}).task_id if hasattr(context.get("task_instance"), "task_id") else context.get("task_id", "unknown")

    # Map task_id prefixes to tiers
    tier = "unknown"
    if task_id.startswith("ingest_"):
        tier = "ingest"
    elif task_id.startswith("validate_and_write_silver_"):
        tier = "silver"
    elif task_id.startswith("quality_check_"):
        tier = "quality"
    elif task_id.startswith("load_iceberg_"):
        tier = "iceberg"
    elif task_id in ("dbt_run", "dbt_test"):
        tier = "dbt"
    elif task_id == "iceberg_maintenance":
        tier = "maintenance"

    # Extract source from task_id suffix
    source = "pipeline"
    entity = "all"
    source_map = {
        "hubspot": ("hubspot", "contacts"),
        "oms": ("oms", "orders"),
        "pim": ("pim", "products"),
        "loyalty": ("loyalty", "loyalty"),
        "marketing": ("marketing", "campaign_events"),
        "zendesk": ("support", "tickets"),
    }
    for suffix, (src, ent) in source_map.items():
        if task_id.endswith(f"_{suffix}"):
            source = src
            entity = ent
            break

    return {"source": source, "entity": entity, "tier": tier, "task_id": task_id}


def _write_run_result(
    source: str,
    entity: str,
    tier: str,
    status: str,
    duration_seconds: Optional[float] = None,
    error_message: Optional[str] = None,
    run_id: Optional[str] = None,
    input_count: int = 0,
    valid_count: int = 0,
    invalid_count: int = 0,
    **extra: Any,
) -> None:
    """Persist a run result to the JSONL log."""
    record = {
        "source": source,
        "entity": entity,
        "tier": tier,
        "status": status,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": duration_seconds,
        "input_count": input_count,
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "error_message": error_message,
        "run_id": run_id,
    }
    record.update(extra)
    try:
        _RUN_LOG.parent.mkdir(parents=True, exist_ok=True)
        with _RUN_LOG.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, default=str) + "\n")
    except OSError as exc:
        logger.warning("Failed to persist run result: %s", exc)


def _publish_sns_alert(subject: str, message: str) -> None:
    """Publish a failure alert to the configured SNS topic (non-fatal)."""
    if not _SNS_ALERT_TOPIC_ARN:
        return
    try:
        import boto3  # noqa: PLC0415
        region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        sns = boto3.client("sns", region_name=region)
        sns.publish(
            TopicArn=_SNS_ALERT_TOPIC_ARN,
            Subject=subject[:100],  # SNS subject max 100 chars
            Message=message[:256000],  # SNS message max 256 KB
        )
        logger.info("SNS alert published to %s", _SNS_ALERT_TOPIC_ARN)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to publish SNS alert (non-fatal): %s", exc)


def on_task_success(context: Dict[str, Any]) -> None:
    """Airflow on_success_callback — records healthy tier execution."""
    meta = _parse_task_metadata(context)
    ti = context.get("task_instance")
    duration = None
    if ti and hasattr(ti, "duration") and ti.duration is not None:
        duration = float(ti.duration)

    # Try to extract run_id and counts from XCom return value
    xcom = {}
    if ti:
        try:
            xcom = ti.xcom_pull(task_ids=meta["task_id"]) or {}
        except Exception:
            pass

    _write_run_result(
        source=meta["source"],
        entity=meta["entity"],
        tier=meta["tier"],
        status="ok",
        duration_seconds=duration,
        run_id=xcom.get("run_id"),
        input_count=int(xcom.get("input_record_count", xcom.get("input_count", 0))),
        valid_count=int(xcom.get("valid_count", 0)),
        invalid_count=int(xcom.get("invalid_count", 0)),
    )

    # Emit CloudWatch task success metric (non-fatal)
    try:
        from src.monitoring.cloudwatch import emit_task_outcome  # noqa: PLC0415
        emit_task_outcome(
            source=meta["source"],
            entity=meta["entity"],
            tier=meta["tier"],
            success=True,
            duration_seconds=duration,
            run_id=xcom.get("run_id"),
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("CloudWatch emit skipped on success callback: %s", exc)

    logger.info(
        "Pipeline health recorded: %s/%s [%s] → OK",
        meta["source"], meta["entity"], meta["tier"],
    )


def on_task_failure(context: Dict[str, Any]) -> None:
    """Airflow on_failure_callback — records failed tier execution and alerts."""
    meta = _parse_task_metadata(context)
    ti = context.get("task_instance")
    exception = context.get("exception")
    duration = None
    if ti and hasattr(ti, "duration") and ti.duration is not None:
        duration = float(ti.duration)

    error_msg = str(exception) if exception else "Unknown error"
    dag_run_id = ti.run_id if ti and hasattr(ti, "run_id") else "unknown"
    dag_id = ti.dag_id if ti and hasattr(ti, "dag_id") else "customer360"

    _write_run_result(
        source=meta["source"],
        entity=meta["entity"],
        tier=meta["tier"],
        status="error",
        duration_seconds=duration,
        error_message=error_msg[:1000],
    )

    # Emit CloudWatch task failure metric (non-fatal)
    try:
        from src.monitoring.cloudwatch import emit_task_outcome  # noqa: PLC0415
        emit_task_outcome(
            source=meta["source"],
            entity=meta["entity"],
            tier=meta["tier"],
            success=False,
            duration_seconds=duration,
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("CloudWatch emit skipped on failure callback: %s", exc)

    # SNS alert (non-fatal, opt-in via C360_SNS_ALERT_TOPIC_ARN)
    _publish_sns_alert(
        subject=f"[Customer360/{_ENV.upper()}] FAILURE: {dag_id}/{meta['task_id']}",
        message=(
            f"Environment : {_ENV}\n"
            f"DAG         : {dag_id}\n"
            f"Task        : {meta['task_id']}\n"
            f"Source/Entity: {meta['source']}/{meta['entity']}\n"
            f"Tier        : {meta['tier']}\n"
            f"Run ID      : {dag_run_id}\n"
            f"Error       : {error_msg[:500]}\n"
        ),
    )

    logger.error(
        "Pipeline health recorded: %s/%s [%s] → ERROR: %s",
        meta["source"], meta["entity"], meta["tier"], error_msg[:200],
    )

