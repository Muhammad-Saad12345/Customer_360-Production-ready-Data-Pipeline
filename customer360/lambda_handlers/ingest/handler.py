"""
Lambda handler: ingest  (state Ingest, inside the per-source Map)

Input:  {"run_id", "source", "entity", "environment", "dry_run"}
Output: {"run_id", "source", "entity", "status", "records_ingested", "s3_raw_key", "source_mode", ...}

status is "success", "no_new_records" or "dry_run". Every field the state
machine selects is always present (a missing field would be an uncatchable
States.Runtime error). Source mode comes from C360_SOURCE_MODES; there is no
synthetic fallback.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from src.ingestion.source_mode import resolve_source_mode
from src.pipeline_contract import get_ingester

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    source: str = event["source"]
    entity: str = event["entity"]
    run_id: str = event["run_id"]
    dry_run: bool = bool(event.get("dry_run", False))
    logger.info("Ingest: source=%s entity=%s run_id=%s dry_run=%s", source, entity, run_id, dry_run)

    base = {"run_id": run_id, "source": source, "entity": entity}
    if dry_run:
        # Still resolves the mode, so a misconfigured source fails the dry run.
        return {**base, "status": "dry_run", "records_ingested": 0, "s3_raw_key": "", "source_mode": resolve_source_mode(source)}

    ingester = get_ingester(source, run_id)
    if ingester.object_name != entity:
        raise ValueError(f"{source} ingests {ingester.object_name!r}, not {entity!r}")
    result = {**base, **ingester.run()}
    logger.info("Ingest complete: %s", result)
    return result
