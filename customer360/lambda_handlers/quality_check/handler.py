"""
Lambda handler: quality_check
Step Functions state: QualityCheck_{source}

Enforces rejection-rate policy and emits CloudWatch DQ metrics.
Extracted from customer360_batch._quality_gate().

Thresholds (configurable via environment variables):
  DQ_REJECT_HARD_PCT (default 10.0%) — blocks Iceberg promotion, raises exception.
  DQ_REJECT_WARN_PCT (default  5.0%) — logs WARNING, continues.

Input event:
{
    "run_id": "c360-...",
    "source": "hubspot",
    "entity": "contacts",
    "input_record_count": 50,
    "valid_count": 48,
    "invalid_count": 2,
    "silver_path": "s3://...",
    "environment": "staging",
    "dry_run": false
}

Output:
{
    "run_id": "c360-...",
    "source": "hubspot",
    "entity": "contacts",
    "valid_count": 48,
    "invalid_count": 2,
    "rejection_rate_pct": 4.0,
    "silver_path": "s3://...",
    "status": "success"
}
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_DQ_REJECT_HARD_PCT: float = float(os.environ.get("DQ_REJECT_HARD_PCT", "10.0"))
_DQ_REJECT_WARN_PCT: float = float(os.environ.get("DQ_REJECT_WARN_PCT", "5.0"))


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Entry point for the Quality Check Lambda."""
    source: str = event["source"]
    entity: str = event["entity"]
    run_id: str = event["run_id"]
    environment: str = event.get("environment", os.environ.get("ENVIRONMENT", "staging"))
    dry_run: bool = bool(event.get("dry_run", False))

    logger.info("QualityCheck: source=%s entity=%s run_id=%s dry_run=%s", source, entity, run_id, dry_run)
    os.environ["ENVIRONMENT"] = environment

    if dry_run:
        logger.info("Dry run — skipping quality gate validation for %s/%s", source, entity)
        return {
            "source": source,
            "entity": entity,
            "run_id": run_id,
            "valid_count": 0,
            "invalid_count": 0,
            "rejection_rate_pct": 0.0,
            "silver_path": "",
            "status": "dry_run",
        }

    input_count = int(event.get("input_record_count") or 0)
    valid_count = int(event.get("valid_count") or 0)
    invalid_count = int(event.get("invalid_count") or 0)
    silver_path = event.get("silver_path", "")

    from src.monitoring.cloudwatch import emit_dq_gate_verdict, emit_dq_metrics

    # 1. Guard: Silver path must exist
    if not silver_path:
        emit_dq_gate_verdict(source, entity, passed=False, rejection_rate_pct=100.0, run_id=run_id)
        raise ValueError(f"{source}/{entity} Silver path is empty — upstream silver task may have failed")

    # 2. Guard: Count bookkeeping integrity
    if input_count and (valid_count + invalid_count) != input_count:
        emit_dq_gate_verdict(source, entity, passed=False, rejection_rate_pct=100.0, run_id=run_id)
        raise ValueError(
            f"{source}/{entity} quality count mismatch: "
            f"input={input_count}, valid={valid_count}, invalid={invalid_count}"
        )

    # 3. Emit DQ metrics regardless of outcome
    emit_dq_metrics(
        source=source,
        entity=entity,
        input_count=input_count,
        valid_count=valid_count,
        invalid_count=invalid_count,
        tier="quality",
        run_id=run_id,
    )

    # 4. Rejection-rate policy
    rejection_rate_pct = (invalid_count / input_count * 100.0) if input_count else 0.0

    if rejection_rate_pct >= _DQ_REJECT_HARD_PCT:
        emit_dq_gate_verdict(source, entity, passed=False, rejection_rate_pct=rejection_rate_pct, run_id=run_id)
        raise ValueError(
            f"{source}/{entity} DQ gate FAILED: rejection rate {rejection_rate_pct:.2f}% "
            f">= hard threshold {_DQ_REJECT_HARD_PCT:.1f}% "
            f"(invalid={invalid_count}, input={input_count}). "
            f"Iceberg promotion blocked."
        )

    if rejection_rate_pct >= _DQ_REJECT_WARN_PCT:
        logger.warning(
            "%s/%s DQ gate WARNING: rejection rate %.2f%% >= soft threshold %.1f%% "
            "(invalid=%d, input=%d)",
            source, entity, rejection_rate_pct, _DQ_REJECT_WARN_PCT, invalid_count, input_count,
        )

    emit_dq_gate_verdict(source, entity, passed=True, rejection_rate_pct=rejection_rate_pct, run_id=run_id)

    result = {
        "source": source,
        "entity": entity,
        "run_id": run_id,
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "rejection_rate_pct": round(rejection_rate_pct, 4),
        "silver_path": silver_path,
        "status": "success",
    }
    logger.info("QualityCheck complete: %s", result)
    return result
