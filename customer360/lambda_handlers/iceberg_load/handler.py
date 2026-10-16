"""
Lambda handler: iceberg_load
Step Functions state: IcebergLoad_{source}

Idempotently promotes validated Silver records into AWS Glue Data Catalog Iceberg tables
via Athena MERGE INTO queries. Extracted from customer360_batch._load_iceberg_gate().

Input event:
{
    "run_id": "c360-...",
    "source": "hubspot",
    "entity": "contacts",
    "valid_count": 48,
    "silver_path": "s3://...",
    "environment": "staging",
    "dry_run": false
}

Output:
{
    "run_id": "c360-...",
    "source": "hubspot",
    "entity": "contacts",
    "table_name": "contacts",
    "silver_row_count": 48,
    "deduped_row_count": 48,
    "athena_executed": true,
    "status": "success"
}
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Entry point for the Iceberg Load Lambda."""
    source: str = event["source"]
    entity: str = event["entity"]
    run_id: str = event["run_id"]
    valid_count: int = int(event.get("valid_count", 0))
    environment: str = event.get("environment", os.environ.get("ENVIRONMENT", "staging"))
    dry_run: bool = bool(event.get("dry_run", False))

    logger.info(
        "IcebergLoad: source=%s entity=%s run_id=%s valid_count=%d dry_run=%s",
        source, entity, run_id, valid_count, dry_run,
    )
    os.environ["ENVIRONMENT"] = environment

    if not run_id:
        raise ValueError(f"No run_id available for Iceberg load of {source}/{entity}")

    if valid_count == 0:
        logger.info("Zero valid records for %s/%s — skipping Iceberg load", source, entity)
        return {
            "source": source,
            "entity": entity,
            "run_id": run_id,
            "status": "skipped",
            "reason": "zero_valid_records",
            "silver_row_count": 0,
            "deduped_row_count": 0,
        }

    from src.lakehouse.iceberg_loader import IcebergLoader

    loader = IcebergLoader()
    summary = loader.load_silver_run(
        source=source,
        entity=entity,
        run_id=run_id,
        dry_run=dry_run,
    )

    result = {
        "source": source,
        "entity": entity,
        "run_id": run_id,
        "table_name": summary.get("table_name", f"{entity}"),
        "silver_row_count": summary.get("silver_row_count", valid_count),
        "deduped_row_count": summary.get("deduped_row_count", valid_count),
        "athena_executed": summary.get("athena_executed", not dry_run),
        "status": summary.get("status", "success"),
        "error_note": summary.get("error_note"),
    }

    logger.info("IcebergLoad complete: %s", result)
    return result
