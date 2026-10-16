"""
Lambda handler: ingest
Step Functions state: Ingest_{source}

Runs a single source connector. Extracted directly from the
_ingest_and_return() and _ingest_hubspot() functions in customer360_batch.py.
No Airflow dependencies.

Input event:
{
    "run_id": "c360-20261016T020000Z-abc123",
    "source": "hubspot",        # one of: hubspot, oms, pim, loyalty, marketing, support
    "entity": "contacts",
    "environment": "staging",
    "dry_run": false
}

Output:
{
    "run_id": "c360-...",
    "source": "hubspot",
    "entity": "contacts",
    "status": "success",
    "records_ingested": 42,
    "s3_raw_key": "raw/hubspot/contacts/c360-.../data.json"
}
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _get_ingester(source: str):
    """
    Factory matching customer360_batch._get_ingester().
    HubSpot = contacts only. OMS = orders. PIM = products.
    Loyalty = loyalty. Marketing = campaign events. Zendesk = tickets.
    """
    if source == "hubspot":
        from src.ingestion.hubspot import HubSpotIngest
        return HubSpotIngest()
    elif source == "oms":
        from src.ingestion.oms import OmsIngest
        return OmsIngest()
    elif source == "pim":
        from src.ingestion.pim import PimIngest
        return PimIngest()
    elif source == "loyalty":
        from src.ingestion.loyalty import LoyaltyIngest
        return LoyaltyIngest()
    elif source == "marketing":
        from src.ingestion.marketing import MarketingIngest
        return MarketingIngest()
    elif source in ("support", "zendesk"):
        from src.ingestion.support import ZendeskIngest
        return ZendeskIngest()
    else:
        raise ValueError(f"Unknown source: {source!r}")


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Entry point for the Ingest Lambda."""
    source: str = event["source"]
    entity: str = event["entity"]
    run_id: str = event["run_id"]
    environment: str = event.get("environment", os.environ.get("ENVIRONMENT", "staging"))
    dry_run: bool = bool(event.get("dry_run", False))

    logger.info("Ingest: source=%s entity=%s run_id=%s dry_run=%s", source, entity, run_id, dry_run)

    # Set environment for synthetic fallback logic
    os.environ["ENVIRONMENT"] = environment
    if environment in ("staging", "stage"):
        os.environ.setdefault("ALLOW_SYNTHETIC_FALLBACK", "true")

    if dry_run:
        logger.info("Dry run — skipping actual ingestion for %s/%s", source, entity)
        return {
            "run_id": run_id,
            "source": source,
            "entity": entity,
            "status": "dry_run",
            "records_ingested": 0,
        }

    ingester = _get_ingester(source)
    ingester.run()

    result: Dict[str, Any] = {
        "run_id": run_id,
        "source": source,
        "entity": entity,
        "status": "success",
    }

    # Populate counts from ingester metadata if available
    if hasattr(ingester, "_last_metadata") and ingester._last_metadata:
        meta = ingester._last_metadata
        result["records_ingested"] = meta.get("records_uploaded", 0)
        result["s3_raw_key"] = meta.get("file_path", "")
    else:
        result["records_ingested"] = 0

    logger.info("Ingest complete: %s", result)
    return result
