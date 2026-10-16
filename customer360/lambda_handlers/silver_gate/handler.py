"""
Lambda handler: silver_gate
Step Functions state: SilverGate_{source}

Validates raw records against schema contracts, routes invalid records
to quarantine, and writes valid records as Parquet to the silver zone.
Extracted from customer360_batch._silver_gate() without Airflow XCom.

Input event:
{
    "run_id": "c360-...",
    "source": "hubspot",
    "entity": "contacts",
    "environment": "staging"
}

Output:
{
    "run_id": "c360-...",
    "source": "hubspot",
    "entity": "contacts",
    "input_record_count": 50,
    "valid_count": 48,
    "invalid_count": 2,
    "silver_path": "s3://c360-bucket786/silver/hubspot/contacts/c360-.../data.parquet",
    "quarantine_path": "s3://c360-bucket786/quarantine/hubspot/contacts/c360-.../rejected.json",
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
    """Entry point for the Silver Gate Lambda."""
    source: str = event["source"]
    entity: str = event["entity"]
    run_id: str = event["run_id"]
    environment: str = event.get("environment", os.environ.get("ENVIRONMENT", "staging"))

    logger.info("SilverGate: source=%s entity=%s run_id=%s", source, entity, run_id)
    os.environ["ENVIRONMENT"] = environment

    from src.processing.silver_gate import HubSpotSilverProcessor

    processor = HubSpotSilverProcessor()
    summary = processor.process_records(source=source, entity=entity, run_id=run_id)

    result = {
        "run_id": run_id,
        "source": source,
        "entity": entity,
        "input_record_count": summary.get("input_record_count", 0),
        "valid_count": summary.get("valid_count", 0),
        "invalid_count": summary.get("invalid_count", 0),
        "silver_path": summary.get("silver_path", ""),
        "quarantine_path": summary.get("quarantine_path", ""),
        "status": "success",
    }

    logger.info("SilverGate complete: %s", result)
    return result
