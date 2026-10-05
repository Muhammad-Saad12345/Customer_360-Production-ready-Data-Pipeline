"""
Customer 360 — Data Contracts
==============================

Machine-readable, authoritative definition of every source's data contract.

Each ``DataContract`` specifies:
- Which source system owns the entity (source ownership rule).
- Required fields that must be non-null for a record to reach Silver.
- Optional fields that may be absent without triggering quarantine.
- Field types and allowed enum values (where applicable).
- Numeric constraints (e.g. price >= 0).
- Timestamp fields that must be valid ISO-8601 strings when present.
- Email fields that must pass format validation when present.
- The primary / business key used for deduplication and Iceberg MERGE.
- The corresponding Iceberg table name.

These contracts are the single source of truth for:
1. ``HubSpotSilverProcessor`` validation logic (silver_gate.py).
2. ``TestE2EPipelineXxx`` test fixtures (test_e2e_pipeline.py).
3. Iceberg ``TableDefinition`` schemas (table_definitions.py).
4. Documentation (DATA_CONTRACTS.md).

Usage
-----
>>> from src.contracts.data_contracts import get_contract
>>> c = get_contract("oms", "orders")
>>> c.primary_keys
['order_id']
>>> c.required_fields
['order_id', 'customer_id', 'order_ts', ...]
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, List, Optional, Tuple


@dataclass(frozen=True)
class FieldConstraint:
    """Constraint applied to a single field value."""
    min_value: Optional[float] = None
    max_value: Optional[float] = None


@dataclass(frozen=True)
class DataContract:
    """
    Authoritative schema and validation contract for a source/entity pair.

    Attributes
    ----------
    source : str
        Authoritative source system name (e.g. ``"oms"``).
    entity : str
        Entity / object type within the source (e.g. ``"orders"``).
    iceberg_table : str
        Name of the Iceberg table this entity is loaded into.
    primary_keys : List[str]
        Business keys used for Iceberg MERGE INTO deduplication.
    required_fields : List[str]
        Fields that must be non-null/non-empty for a record to pass validation.
    optional_fields : List[str]
        Fields that may be absent without quarantine.
    timestamp_fields : List[str]
        Fields whose values must be valid ISO-8601 datetime strings when present.
    email_fields : List[str]
        Fields whose values must be valid email addresses when present.
    enum_fields : Dict[str, FrozenSet[str]]
        Mapping of field name → set of allowed enum values.
    numeric_constraints : Dict[str, FieldConstraint]
        Mapping of field name → numeric bounds.
    nested_objects : List[str]
        Top-level keys that must be present as dict objects (HubSpot-style).
    description : str
        Human-readable description of this contract.
    """
    source: str
    entity: str
    iceberg_table: str
    primary_keys: List[str]
    required_fields: List[str]
    optional_fields: List[str] = field(default_factory=list)
    timestamp_fields: List[str] = field(default_factory=list)
    email_fields: List[str] = field(default_factory=list)
    enum_fields: Dict[str, FrozenSet[str]] = field(default_factory=dict)
    numeric_constraints: Dict[str, FieldConstraint] = field(default_factory=dict)
    nested_objects: List[str] = field(default_factory=list)
    description: str = ""

    def validate(self, record: Dict[str, Any]) -> Optional[str]:
        """
        Lightweight structural validation.  Returns ``None`` on success or a
        failure reason string on the first detected violation.

        Full validation (including seen_keys deduplication) is handled by
        ``HubSpotSilverProcessor``; this method is useful for unit testing
        individual field rules independently of the processor state.
        """
        if not isinstance(record, dict):
            return "record is not a JSON object"

        # Nested object presence (HubSpot contacts, etc.)
        for nested in self.nested_objects:
            if not isinstance(record.get(nested), dict):
                return f"missing or non-dict nested object '{nested}'"

        # Resolve record for required field checks
        flat = dict(record)
        props = record.get("properties", {}) if isinstance(record.get("properties"), dict) else {}
        flat.update(props)

        for f_name in self.required_fields:
            value = flat.get(f_name)
            if value is None or (isinstance(value, str) and value.strip() == ""):
                return f"missing required field '{f_name}'"

        for f_name, allowed in self.enum_fields.items():
            value = flat.get(f_name)
            if value is not None and value != "" and value not in allowed:
                return f"invalid enum value for '{f_name}': {value}"

        for f_name, constraint in self.numeric_constraints.items():
            value = flat.get(f_name)
            if value is not None and value != "":
                try:
                    num = float(value)
                except (TypeError, ValueError):
                    return f"non-numeric value for '{f_name}': {value}"
                if constraint.min_value is not None and num < constraint.min_value:
                    return f"value below minimum for '{f_name}': {value}"
                if constraint.max_value is not None and num > constraint.max_value:
                    return f"value above maximum for '{f_name}': {value}"

        return None


# ---------------------------------------------------------------------------
# Contract definitions — one per authoritative source/entity
# ---------------------------------------------------------------------------

HUBSPOT_CONTACTS_CONTRACT = DataContract(
    source="hubspot",
    entity="contacts",
    iceberg_table="hubspot_contacts",
    primary_keys=["customer_id"],
    description=(
        "HubSpot CRM contacts.  HubSpot is the SOLE authoritative source for "
        "customer/contact data.  It must NEVER ingest orders or products — those "
        "are owned by OMS and PIM respectively."
    ),
    required_fields=["id", "email"],
    optional_fields=[
        "firstname", "lastname", "phone", "createdate",
        "lastmodifieddate", "lifecyclestage", "hubspot_owner_id",
    ],
    timestamp_fields=["createdate", "lastmodifieddate"],
    email_fields=["email"],
    enum_fields={},
    numeric_constraints={},
    nested_objects=["properties"],
)

OMS_ORDERS_CONTRACT = DataContract(
    source="oms",
    entity="orders",
    iceberg_table="oms_orders",
    primary_keys=["order_id"],
    description=(
        "Order Management System orders.  OMS is the SOLE authoritative source for "
        "order data.  HubSpot must never produce order records."
    ),
    required_fields=[
        "order_id", "customer_id", "order_ts", "order_status",
        "subtotal", "tax", "shipping", "discount", "net_total",
        "currency", "channel", "updated_at",
    ],
    optional_fields=[],
    timestamp_fields=["order_ts", "updated_at"],
    email_fields=[],
    enum_fields={
        "order_status": frozenset({
            "paid", "shipped", "fulfilled", "cancelled",
            "pending", "refunded", "processing", "failed",
        }),
    },
    numeric_constraints={
        "subtotal": FieldConstraint(min_value=0),
        "tax": FieldConstraint(min_value=0),
        "shipping": FieldConstraint(min_value=0),
        "discount": FieldConstraint(min_value=0),
        "net_total": FieldConstraint(min_value=0),
    },
)

PIM_PRODUCTS_CONTRACT = DataContract(
    source="pim",
    entity="products",
    iceberg_table="pim_products",
    primary_keys=["product_id"],
    description=(
        "Product Information Management catalog.  PIM is the SOLE authoritative "
        "source for product data.  HubSpot must never produce product records."
    ),
    required_fields=[
        "product_id", "sku", "name", "category", "brand",
        "price", "cost", "status", "launch_date", "updated_at",
    ],
    optional_fields=["attributes"],
    timestamp_fields=["launch_date", "updated_at"],
    email_fields=[],
    enum_fields={
        "status": frozenset({"active", "archived", "draft", "retired", "inactive"}),
    },
    numeric_constraints={
        "price": FieldConstraint(min_value=0),
        "cost": FieldConstraint(min_value=0),
    },
)

LOYALTY_CONTRACT = DataContract(
    source="loyalty",
    entity="loyalty",
    iceberg_table="loyalty_loyalty",
    primary_keys=["loyalty_id"],
    description="Customer loyalty and rewards ledger managed by the Loyalty platform.",
    required_fields=[
        "loyalty_id", "customer_id", "tier", "points_balance",
        "earn_ts", "redeem_ts", "benefit_code", "updated_at",
    ],
    optional_fields=[],
    timestamp_fields=["earn_ts", "redeem_ts", "updated_at"],
    email_fields=[],
    enum_fields={
        "tier": frozenset({"gold", "silver", "bronze", "platinum", "diamond"}),
    },
    numeric_constraints={
        "points_balance": FieldConstraint(min_value=0),
    },
)

MARKETING_CAMPAIGN_EVENTS_CONTRACT = DataContract(
    source="marketing",
    entity="campaign_events",
    iceberg_table="marketing_campaign_events",
    primary_keys=["record_id"],
    description=(
        "Marketing and email campaign events sourced from Mailchimp/Brevo. "
        "The 'marketing' source key also accepts 'mailchimp' as an alias."
    ),
    required_fields=[
        "campaign_id", "customer_id", "channel", "send_ts", "open_ts",
        "audience_id", "member_id", "email_address", "status", "source_type",
    ],
    optional_fields=["click_ts", "offer_id"],
    timestamp_fields=["send_ts", "open_ts", "click_ts"],
    email_fields=["email_address"],
    enum_fields={
        "channel": frozenset({"email"}),
        "status": frozenset({
            "subscribed", "unsubscribed", "cleaned", "pending", "transactional",
        }),
    },
    numeric_constraints={},
)

ZENDESK_TICKETS_CONTRACT = DataContract(
    source="support",
    entity="tickets",
    iceberg_table="zendesk_tickets",
    primary_keys=["ticket_id"],
    description=(
        "Customer service and support tickets sourced from Zendesk. "
        "The 'support' source key also accepts 'zendesk' as an alias."
    ),
    required_fields=[
        "ticket_id", "customer_id", "channel", "category",
        "priority", "status", "created_at",
    ],
    optional_fields=["agent_id", "resolved_at", "csat_score", "source_type"],
    timestamp_fields=["created_at", "resolved_at"],
    email_fields=[],
    enum_fields={
        "channel": frozenset({"email", "web", "chat", "phone", "api"}),
        "priority": frozenset({"low", "normal", "high", "urgent"}),
        "status": frozenset({"new", "open", "pending", "hold", "solved", "closed"}),
    },
    numeric_constraints={
        "csat_score": FieldConstraint(min_value=0, max_value=5),
    },
)

# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

CONTRACT_REGISTRY: Dict[Tuple[str, str], DataContract] = {
    ("hubspot", "contacts"): HUBSPOT_CONTACTS_CONTRACT,
    ("oms", "orders"): OMS_ORDERS_CONTRACT,
    ("pim", "products"): PIM_PRODUCTS_CONTRACT,
    ("loyalty", "loyalty"): LOYALTY_CONTRACT,
    ("marketing", "campaign_events"): MARKETING_CAMPAIGN_EVENTS_CONTRACT,
    # Mailchimp is an alias for the marketing campaign_events table
    ("mailchimp", "campaign_events"): MARKETING_CAMPAIGN_EVENTS_CONTRACT,
    # Zendesk / support tickets
    ("support", "tickets"): ZENDESK_TICKETS_CONTRACT,
    ("zendesk", "tickets"): ZENDESK_TICKETS_CONTRACT,
}


def get_contract(source: str, entity: str) -> Optional[DataContract]:
    """Return the DataContract for a given (source, entity) pair, or None."""
    return CONTRACT_REGISTRY.get((source, entity))


def get_all_contracts() -> List[DataContract]:
    """Return unique DataContracts (deduplicated by iceberg_table name)."""
    seen: set = set()
    result = []
    for c in CONTRACT_REGISTRY.values():
        if c.iceberg_table not in seen:
            seen.add(c.iceberg_table)
            result.append(c)
    return result
