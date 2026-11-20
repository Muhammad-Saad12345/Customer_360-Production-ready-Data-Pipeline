"""
Lambda handler: silver_gate  (state SilverGate)

Input:  {"run_id", "source", "entity", "environment", "dry_run", "s3_raw_key"}
Output: {"valid_count", "invalid_count", "input_record_count", "silver_path", "quarantine_path", "status"}

Validates the exact raw object written by this run's ingest step, writes
valid rows to silver/ (Parquet) and rejected rows to quarantine/.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    source: str = event["source"]
    entity: str = event["entity"]
    run_id: str = event["run_id"]
    raw_key: str = event.get("s3_raw_key") or ""
    dry_run: bool = bool(event.get("dry_run", False))
    base = {"run_id": run_id, "source": source, "entity": entity}

    if dry_run or not raw_key:
        status = "dry_run" if dry_run else "no_data"
        logger.info("SilverGate %s for %s/%s", status, source, entity)
        return {**base, "status": status, "input_record_count": 0, "valid_count": 0, "invalid_count": 0,
                "silver_path": "", "quarantine_path": ""}

    from src.processing.silver_gate import HubSpotSilverProcessor

    summary = HubSpotSilverProcessor().process_records(source=source, entity=entity, run_id=run_id, raw_key=raw_key)
    result = {
        **base,
        "status": "success",
        "input_record_count": summary["input_record_count"],
        "valid_count": summary["valid_count"],
        "invalid_count": summary["invalid_count"],
        "silver_path": summary.get("silver_path") or "",
        "quarantine_path": summary.get("quarantine_path") or "",
    }
    logger.info("SilverGate complete: %s", result)
    return result
