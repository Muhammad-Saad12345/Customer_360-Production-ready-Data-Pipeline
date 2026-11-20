"""
Lambda handler: iceberg_load  (state IcebergLoad)

Input:  {"run_id", "source", "entity", "environment", "dry_run", "silver_path", "valid_count"}
Output: {"status": success | skipped, "loaded_row_count", "athena_executed"}

Any MERGE failure raises (TransientError for Athena throttling, so the
state machine retries). "success" always means the MERGE ran.
"""
from __future__ import annotations

import logging
from typing import Any, Dict
from urllib.parse import urlparse

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    source: str = event["source"]
    entity: str = event["entity"]
    run_id: str = event["run_id"]
    valid_count = int(event.get("valid_count") or 0)
    dry_run: bool = bool(event.get("dry_run", False))
    silver_path: str = event.get("silver_path") or ""
    base = {"run_id": run_id, "source": source, "entity": entity}

    if dry_run or valid_count == 0:
        reason = "dry_run" if dry_run else "zero_valid_records"
        logger.info("IcebergLoad skipped for %s/%s: %s", source, entity, reason)
        return {**base, "status": "skipped", "reason": reason, "loaded_row_count": 0, "athena_executed": False}

    from src.lakehouse.iceberg_loader import IcebergLoader

    silver_key = urlparse(silver_path).path.lstrip("/") if silver_path.startswith("s3://") else None
    summary = IcebergLoader().load_silver_run(source=source, entity=entity, run_id=run_id, silver_key=silver_key)
    if summary.get("status") == "success" and not summary.get("athena_executed"):
        raise RuntimeError(f"{source}/{entity}: loader reported success without running the MERGE")
    result = {
        **base,
        "status": summary.get("status", "success"),
        "table_name": summary.get("table_name"),
        "loaded_row_count": int(summary.get("loaded_row_count") or 0),
        "athena_executed": bool(summary.get("athena_executed")),
    }
    logger.info("IcebergLoad complete: %s", result)
    return result
