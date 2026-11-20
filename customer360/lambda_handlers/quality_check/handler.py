"""
Lambda handler: quality_check  (state QualityCheck)

Input:  {"run_id", "source", "entity", "environment", "dry_run", "valid_count", "invalid_count", "silver_path"}
Output: {"verdict": PASS | WARN | NO_DATA | SKIPPED, "rejection_rate_pct", ...}

A rejection rate >= DQ_REJECT_HARD_PCT raises DataQualityGateFailed (not
retried); the per-source Catch records the source as failed and nothing is
loaded to Iceberg for it.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DataQualityGateFailed(Exception):
    """Rejection rate at or above the hard threshold."""


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    source: str = event["source"]
    entity: str = event["entity"]
    run_id: str = event["run_id"]
    dry_run: bool = bool(event.get("dry_run", False))
    valid_count = int(event.get("valid_count") or 0)
    invalid_count = int(event.get("invalid_count") or 0)
    input_count = int(event.get("input_record_count") or (valid_count + invalid_count))
    silver_path = event.get("silver_path") or ""
    hard = float(os.environ.get("DQ_REJECT_HARD_PCT", "10.0"))
    warn = float(os.environ.get("DQ_REJECT_WARN_PCT", "5.0"))
    base = {"run_id": run_id, "source": source, "entity": entity, "valid_count": valid_count,
            "invalid_count": invalid_count, "silver_path": silver_path}

    if dry_run:
        return {**base, "verdict": "SKIPPED", "rejection_rate_pct": 0.0}
    if input_count == 0:
        logger.info("No records for %s/%s in this run", source, entity)
        return {**base, "verdict": "NO_DATA", "rejection_rate_pct": 0.0}
    if valid_count + invalid_count != input_count:
        raise DataQualityGateFailed(
            f"{source}/{entity} count mismatch: input={input_count} valid={valid_count} invalid={invalid_count}"
        )
    if valid_count > 0 and not silver_path:
        raise DataQualityGateFailed(f"{source}/{entity} has {valid_count} valid rows but no silver_path")

    from src.monitoring.cloudwatch import emit_dq_gate_verdict, emit_dq_metrics

    rate = invalid_count / input_count * 100.0
    emit_dq_metrics(source, entity, input_count, valid_count, invalid_count, tier="quality", run_id=run_id)
    if rate >= hard:
        emit_dq_gate_verdict(source, entity, passed=False, rejection_rate_pct=rate, run_id=run_id)
        raise DataQualityGateFailed(
            f"{source}/{entity} DQ gate FAILED: rejection {rate:.2f}% >= {hard:.1f}% "
            f"(invalid={invalid_count}, input={input_count}); Iceberg load blocked"
        )
    emit_dq_gate_verdict(source, entity, passed=True, rejection_rate_pct=rate, run_id=run_id)
    verdict = "WARN" if rate >= warn else "PASS"
    result = {**base, "verdict": verdict, "rejection_rate_pct": round(rate, 4)}
    logger.info("QualityCheck complete: %s", result)
    return result
