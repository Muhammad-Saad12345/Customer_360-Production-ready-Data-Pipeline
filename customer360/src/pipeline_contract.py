"""Sources the batch pipeline knows about, and their raw entity names."""
from __future__ import annotations

SOURCE_ENTITIES = {
    "hubspot": "contacts",
    "oms": "orders",
    "pim": "products",
    "loyalty": "loyalty",
    "marketing": "campaign_events",
    "support": "tickets",
}

# A failure of one of these stops the run before dbt; others become warnings.
DEFAULT_MANDATORY_SOURCES = ("hubspot", "oms")


def get_ingester(source: str, run_id: str):
    if source == "hubspot":
        from src.ingestion.hubspot import HubSpotIngest
        return HubSpotIngest(run_id=run_id)
    if source == "oms":
        from src.ingestion.oms import OmsIngest
        return OmsIngest(run_id=run_id)
    if source == "pim":
        from src.ingestion.pim import PimIngest
        return PimIngest(run_id=run_id)
    if source == "loyalty":
        from src.ingestion.loyalty import LoyaltyIngest
        return LoyaltyIngest(run_id=run_id)
    if source == "marketing":
        from src.ingestion.marketing import MarketingIngest
        return MarketingIngest(run_id=run_id)
    if source in ("support", "zendesk"):
        from src.ingestion.support import ZendeskIngest
        return ZendeskIngest(run_id=run_id)
    raise ValueError(f"Unknown source: {source!r}")
