"""CloudWatch custom metrics emitter for Customer 360 Lakehouse.

All metric emissions are fire-and-forget — a failure to publish a metric
MUST NOT propagate an exception into the pipeline.  The emitter degrades
gracefully to a no-op when:
  - boto3 is unavailable
  - AWS credentials are absent
  - The process is running in a unit-test context (ENVIRONMENT=test)

Metric namespace: Customer360/Pipeline
Dimensions:
  - Environment  (dev | staging | prod)
  - Source       (hubspot | oms | pim | loyalty | marketing | support)
  - Entity       (contacts | orders | products | loyalty | campaign_events | tickets)
  - Tier         (ingest | silver | quality | iceberg | dbt | maintenance)

Key metrics published:
  - InputRecordCount   (Count)
  - ValidRecordCount   (Count)
  - InvalidRecordCount (Count)
  - RejectionRate      (Percent, 0-100)
  - TaskDurationSeconds (Seconds)
  - TaskSuccess        (Count, value=1 on success)
  - TaskFailure        (Count, value=1 on failure)
  - DQGatePassed       (Count, value=1 when gate passes)
  - DQGateFailed       (Count, value=1 when gate blocks)
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger("customer360.monitoring.cloudwatch")

_NAMESPACE = "Customer360/Pipeline"
_ENV = os.environ.get("ENVIRONMENT", "dev").lower()

# ---------------------------------------------------------------------------
# Lazy boto3 client — avoids import-time failures in test environments
# ---------------------------------------------------------------------------


def _get_client():
    """Return a boto3 CloudWatch client, or None if unavailable."""
    if _ENV in ("test", "testing"):
        return None
    try:
        import boto3  # noqa: PLC0415
        region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        return boto3.client("cloudwatch", region_name=region)
    except Exception:  # noqa: BLE001
        return None


_cw_client = None


def _client():
    global _cw_client  # noqa: PLW0603
    if _cw_client is None:
        _cw_client = _get_client()
    return _cw_client


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _dimensions(
    source: Optional[str] = None,
    entity: Optional[str] = None,
    tier: Optional[str] = None,
) -> List[Dict[str, str]]:
    dims: List[Dict[str, str]] = [{"Name": "Environment", "Value": _ENV}]
    if source:
        dims.append({"Name": "Source", "Value": source})
    if entity:
        dims.append({"Name": "Entity", "Value": entity})
    if tier:
        dims.append({"Name": "Tier", "Value": tier})
    return dims


def _build_datum(
    name: str,
    value: float,
    unit: str,
    dims: List[Dict[str, str]],
    timestamp: Optional[datetime] = None,
) -> Dict[str, Any]:
    datum: Dict[str, Any] = {
        "MetricName": name,
        "Dimensions": dims,
        "Value": value,
        "Unit": unit,
    }
    if timestamp:
        datum["Timestamp"] = timestamp
    return datum


def _put(metric_data: Sequence[Dict[str, Any]]) -> None:
    """Push up to 20 metrics per call (CloudWatch limit is 1000 but keep batches small)."""
    client = _client()
    if client is None:
        logger.debug("CloudWatch client unavailable; skipping metric push: %s", [m["MetricName"] for m in metric_data])
        return
    try:
        for i in range(0, len(metric_data), 20):
            client.put_metric_data(
                Namespace=_NAMESPACE,
                MetricData=list(metric_data[i : i + 20]),
            )
        logger.debug("Published %d CloudWatch metrics", len(metric_data))
    except Exception as exc:  # noqa: BLE001
        # Never let metric emission crash the pipeline
        logger.warning("CloudWatch put_metric_data failed (non-fatal): %s", exc)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def emit_dq_metrics(
    source: str,
    entity: str,
    input_count: int,
    valid_count: int,
    invalid_count: int,
    tier: str = "silver",
    run_id: Optional[str] = None,
) -> None:
    """Emit Data Quality counters and rejection-rate metric.

    Parameters
    ----------
    source:        Data source name (e.g. ``"hubspot"``).
    entity:        Entity name (e.g. ``"contacts"``).
    input_count:   Total records entering the gate.
    valid_count:   Records passing validation.
    invalid_count: Records rejected to quarantine.
    tier:          Pipeline tier label (default ``"silver"``).
    run_id:        Airflow run_id for structured log correlation.
    """
    dims = _dimensions(source=source, entity=entity, tier=tier)
    ts = datetime.now(timezone.utc)
    rejection_rate_pct = (invalid_count / input_count * 100.0) if input_count else 0.0

    data = [
        _build_datum("InputRecordCount", float(input_count), "Count", dims, ts),
        _build_datum("ValidRecordCount", float(valid_count), "Count", dims, ts),
        _build_datum("InvalidRecordCount", float(invalid_count), "Count", dims, ts),
        _build_datum("RejectionRate", rejection_rate_pct, "Percent", dims, ts),
    ]
    _put(data)
    logger.info(
        "DQ metrics emitted [%s/%s] input=%d valid=%d invalid=%d rejection=%.2f%%  run_id=%s",
        source, entity, input_count, valid_count, invalid_count, rejection_rate_pct, run_id,
    )


def emit_task_outcome(
    source: str,
    entity: str,
    tier: str,
    success: bool,
    duration_seconds: Optional[float] = None,
    run_id: Optional[str] = None,
) -> None:
    """Emit a TaskSuccess or TaskFailure metric plus duration.

    Parameters
    ----------
    source:           Data source name.
    entity:           Entity name.
    tier:             Pipeline tier label.
    success:          True for success, False for failure.
    duration_seconds: Wall-clock time of the task in seconds.
    run_id:           Airflow run_id for correlation.
    """
    dims = _dimensions(source=source, entity=entity, tier=tier)
    ts = datetime.now(timezone.utc)
    data: List[Dict[str, Any]] = []

    if success:
        data.append(_build_datum("TaskSuccess", 1.0, "Count", dims, ts))
    else:
        data.append(_build_datum("TaskFailure", 1.0, "Count", dims, ts))

    if duration_seconds is not None:
        data.append(_build_datum("TaskDurationSeconds", duration_seconds, "Seconds", dims, ts))

    _put(data)
    outcome = "SUCCESS" if success else "FAILURE"
    logger.info(
        "Task outcome emitted [%s/%s/%s] %s  duration=%.1fs  run_id=%s",
        source, entity, tier, outcome, duration_seconds or 0.0, run_id,
    )


def emit_dq_gate_verdict(
    source: str,
    entity: str,
    passed: bool,
    rejection_rate_pct: float,
    run_id: Optional[str] = None,
) -> None:
    """Emit DQGatePassed / DQGateFailed metric.

    Parameters
    ----------
    source:               Data source name.
    entity:               Entity name.
    passed:               True if the gate allowed promotion to Silver/Iceberg.
    rejection_rate_pct:   Observed rejection rate percentage.
    run_id:               Airflow run_id for correlation.
    """
    dims = _dimensions(source=source, entity=entity, tier="quality")
    ts = datetime.now(timezone.utc)
    metric_name = "DQGatePassed" if passed else "DQGateFailed"
    data = [
        _build_datum(metric_name, 1.0, "Count", dims, ts),
        _build_datum("RejectionRate", rejection_rate_pct, "Percent", dims, ts),
    ]
    _put(data)
    verdict = "PASSED" if passed else "FAILED"
    logger.info(
        "DQ gate verdict emitted [%s/%s] %s  rejection=%.2f%%  run_id=%s",
        source, entity, verdict, rejection_rate_pct, run_id,
    )


def emit_pipeline_health(overall_status: str, error_count: int, warning_count: int) -> None:
    """Emit overall pipeline health metric (0=OK, 1=WARNING, 2=ERROR).

    Useful for a single-metric CloudWatch alarm on the full pipeline.
    """
    status_value = {"ok": 0.0, "warning": 1.0, "error": 2.0}.get(overall_status.lower(), 2.0)
    dims = _dimensions()
    ts = datetime.now(timezone.utc)
    data = [
        _build_datum("PipelineHealthStatus", status_value, "None", dims, ts),
        _build_datum("PipelineErrorCount", float(error_count), "Count", dims, ts),
        _build_datum("PipelineWarningCount", float(warning_count), "Count", dims, ts),
    ]
    _put(data)
    logger.info(
        "Pipeline health emitted: status=%s errors=%d warnings=%d",
        overall_status, error_count, warning_count,
    )
