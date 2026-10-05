from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class ColumnDefinition:
    name: str
    data_type: str  # Athena / Iceberg type: string, double, bigint, boolean, date, timestamp
    comment: str = ""
    is_primary_key: bool = False
    is_partition_key: bool = False


@dataclass(frozen=True)
class TableDefinition:
    table_name: str
    source: str
    entity: str
    primary_keys: List[str]
    partition_keys: List[str]
    columns: List[ColumnDefinition]
    comment: str = ""
    tblproperties: Dict[str, str] = field(default_factory=lambda: {
        "table_type": "ICEBERG",
        "format": "parquet",
        "write_compression": "snappy",
    })

    @property
    def non_partition_columns(self) -> List[ColumnDefinition]:
        return [c for c in self.columns if not c.is_partition_key]

    @property
    def column_names(self) -> List[str]:
        return [c.name for c in self.columns]


# -------------------------------------------------------------------------
# Customer 360 Iceberg Table Specifications
# -------------------------------------------------------------------------

HUBSPOT_CONTACTS_TABLE = TableDefinition(
    table_name="hubspot_contacts",
    source="hubspot",
    entity="contacts",
    primary_keys=["customer_id"],
    partition_keys=["ingestion_date"],
    comment="Iceberg Silver Lakehouse table for validated HubSpot CRM Contacts",
    columns=[
        ColumnDefinition("customer_id", "string", "HubSpot unique contact ID (Business Key)", is_primary_key=True),
        ColumnDefinition("record_id", "string", "Unified record identifier"),
        ColumnDefinition("first_name", "string", "Customer first name"),
        ColumnDefinition("last_name", "string", "Customer last name"),
        ColumnDefinition("email", "string", "Customer primary email address"),
        ColumnDefinition("phone", "string", "Customer primary phone number"),
        ColumnDefinition("customer_stage", "string", "CRM lifecycle stage e.g. lead, customer"),
        ColumnDefinition("created_at", "string", "Contact creation timestamp ISO-8601"),
        ColumnDefinition("updated_at", "string", "Contact last modification timestamp ISO-8601"),
        ColumnDefinition("owner_id", "string", "HubSpot owner user ID"),
        ColumnDefinition("source", "string", "Originating source system name ('hubspot')"),
        ColumnDefinition("entity", "string", "Originating entity name ('contacts')"),
        ColumnDefinition("run_id", "string", "Airflow pipeline batch execution UUID"),
        ColumnDefinition("ingestion_timestamp", "string", "Timestamp when record was processed into Silver"),
        ColumnDefinition("ingestion_date", "date", "Partition date extracted from ingestion_timestamp", is_partition_key=True),
    ],
)

OMS_ORDERS_TABLE = TableDefinition(
    table_name="oms_orders",
    source="oms",
    entity="orders",
    primary_keys=["order_id"],
    partition_keys=["ingestion_date"],
    comment="Iceberg Silver Lakehouse table for validated OMS Orders",
    columns=[
        ColumnDefinition("order_id", "string", "Order Management System unique order identifier (Business Key)", is_primary_key=True),
        ColumnDefinition("record_id", "string", "Unified record identifier"),
        ColumnDefinition("customer_id", "string", "Foreign key reference to customer identifier"),
        ColumnDefinition("order_ts", "string", "Timestamp when order was placed"),
        ColumnDefinition("order_status", "string", "Order state: paid, shipped, fulfilled, cancelled, etc."),
        ColumnDefinition("subtotal", "double", "Order subtotal amount before tax and shipping"),
        ColumnDefinition("tax", "double", "Tax amount charged on order"),
        ColumnDefinition("shipping", "double", "Shipping charge on order"),
        ColumnDefinition("discount", "double", "Discount applied to order"),
        ColumnDefinition("net_total", "double", "Net total revenue collected for order"),
        ColumnDefinition("currency", "string", "Three-letter ISO currency code e.g. USD"),
        ColumnDefinition("channel", "string", "Order sales channel e.g. web, mobile, retail"),
        ColumnDefinition("updated_at", "string", "Timestamp of last order status change"),
        ColumnDefinition("source", "string", "Originating source system name ('oms')"),
        ColumnDefinition("entity", "string", "Originating entity name ('orders')"),
        ColumnDefinition("run_id", "string", "Airflow pipeline batch execution UUID"),
        ColumnDefinition("ingestion_timestamp", "string", "Timestamp when record was processed into Silver"),
        ColumnDefinition("ingestion_date", "date", "Partition date extracted from ingestion_timestamp", is_partition_key=True),
    ],
)

PIM_PRODUCTS_TABLE = TableDefinition(
    table_name="pim_products",
    source="pim",
    entity="products",
    primary_keys=["product_id"],
    partition_keys=["ingestion_date"],
    comment="Iceberg Silver Lakehouse table for validated PIM Product Catalog",
    columns=[
        ColumnDefinition("product_id", "string", "Product Information Management unique identifier (Business Key)", is_primary_key=True),
        ColumnDefinition("record_id", "string", "Unified record identifier"),
        ColumnDefinition("sku", "string", "Stock keeping unit code"),
        ColumnDefinition("name", "string", "Product title and commercial name"),
        ColumnDefinition("category", "string", "Product catalog category classification"),
        ColumnDefinition("brand", "string", "Manufacturer brand label"),
        ColumnDefinition("price", "double", "Product retail selling price"),
        ColumnDefinition("cost", "double", "Product wholesale unit acquisition cost"),
        ColumnDefinition("attributes", "string", "Serialized JSON string of extended product specifications"),
        ColumnDefinition("status", "string", "Product status: active, archived, draft, retired"),
        ColumnDefinition("launch_date", "string", "Commercial market launch date"),
        ColumnDefinition("updated_at", "string", "Timestamp of last product catalog update"),
        ColumnDefinition("source", "string", "Originating source system name ('pim')"),
        ColumnDefinition("entity", "string", "Originating entity name ('products')"),
        ColumnDefinition("run_id", "string", "Airflow pipeline batch execution UUID"),
        ColumnDefinition("ingestion_timestamp", "string", "Timestamp when record was processed into Silver"),
        ColumnDefinition("ingestion_date", "date", "Partition date extracted from ingestion_timestamp", is_partition_key=True),
    ],
)

LOYALTY_TABLE = TableDefinition(
    table_name="loyalty_loyalty",
    source="loyalty",
    entity="loyalty",
    primary_keys=["loyalty_id"],
    partition_keys=["ingestion_date"],
    comment="Iceberg Silver Lakehouse table for Customer Loyalty & Rewards ledger",
    columns=[
        ColumnDefinition("loyalty_id", "string", "Loyalty membership / ledger account identifier (Business Key)", is_primary_key=True),
        ColumnDefinition("record_id", "string", "Unified record identifier"),
        ColumnDefinition("customer_id", "string", "Foreign key reference to customer identifier"),
        ColumnDefinition("tier", "string", "Customer loyalty tier: bronze, silver, gold, platinum, diamond"),
        ColumnDefinition("points_balance", "double", "Current verified loyalty points balance"),
        ColumnDefinition("earn_ts", "string", "Timestamp of points accrual event"),
        ColumnDefinition("redeem_ts", "string", "Timestamp of points redemption event"),
        ColumnDefinition("benefit_code", "string", "Active promotional reward benefit voucher code"),
        ColumnDefinition("updated_at", "string", "Timestamp of last loyalty record update"),
        ColumnDefinition("source", "string", "Originating source system name ('loyalty')"),
        ColumnDefinition("entity", "string", "Originating entity name ('loyalty')"),
        ColumnDefinition("run_id", "string", "Airflow pipeline batch execution UUID"),
        ColumnDefinition("ingestion_timestamp", "string", "Timestamp when record was processed into Silver"),
        ColumnDefinition("ingestion_date", "date", "Partition date extracted from ingestion_timestamp", is_partition_key=True),
    ],
)

MARKETING_EVENTS_TABLE = TableDefinition(
    table_name="marketing_campaign_events",
    source="marketing",
    entity="campaign_events",
    primary_keys=["record_id"],
    partition_keys=["ingestion_date"],
    comment="Iceberg Silver Lakehouse table for Marketing & Email Campaign Events",
    columns=[
        ColumnDefinition("record_id", "string", "Unique composite campaign event identifier (Business Key)", is_primary_key=True),
        ColumnDefinition("campaign_id", "string", "Marketing campaign broadcast identifier"),
        ColumnDefinition("customer_id", "string", "Customer identifier matched to recipient"),
        ColumnDefinition("channel", "string", "Engagement channel e.g. email, sms"),
        ColumnDefinition("send_ts", "string", "Timestamp message was dispatched"),
        ColumnDefinition("open_ts", "string", "Timestamp recipient opened message"),
        ColumnDefinition("click_ts", "string", "Timestamp recipient clicked link in message"),
        ColumnDefinition("audience_id", "string", "Mailing list or audience segment identifier"),
        ColumnDefinition("offer_id", "string", "Special promotional voucher code offered"),
        ColumnDefinition("member_id", "string", "Subscriber membership identifier in marketing platform"),
        ColumnDefinition("email_address", "string", "Recipient email address"),
        ColumnDefinition("status", "string", "Engagement status: subscribed, pending, transactional"),
        ColumnDefinition("source_type", "string", "Provider type: mailchimp, synthetic, brevo"),
        ColumnDefinition("source", "string", "Originating source system name ('marketing'/'mailchimp')"),
        ColumnDefinition("entity", "string", "Originating entity name ('campaign_events')"),
        ColumnDefinition("run_id", "string", "Airflow pipeline batch execution UUID"),
        ColumnDefinition("ingestion_timestamp", "string", "Timestamp when record was processed into Silver"),
        ColumnDefinition("ingestion_date", "date", "Partition date extracted from ingestion_timestamp", is_partition_key=True),
    ],
)

ZENDESK_TICKETS_TABLE = TableDefinition(
    table_name="zendesk_tickets",
    source="support",
    entity="tickets",
    primary_keys=["ticket_id"],
    partition_keys=["ingestion_date"],
    comment="Iceberg Silver Lakehouse table for validated Zendesk Support Tickets",
    columns=[
        ColumnDefinition("ticket_id", "string", "Zendesk unique ticket identifier (Business Key)", is_primary_key=True),
        ColumnDefinition("record_id", "string", "Unified record identifier"),
        ColumnDefinition("customer_id", "string", "Foreign key reference to customer identifier"),
        ColumnDefinition("channel", "string", "Support channel: email, web, chat, phone, api"),
        ColumnDefinition("category", "string", "Ticket category classification e.g. billing, technical, returns"),
        ColumnDefinition("priority", "string", "Ticket priority: low, normal, high, urgent"),
        ColumnDefinition("status", "string", "Ticket lifecycle status: new, open, pending, hold, solved, closed"),
        ColumnDefinition("agent_id", "string", "Support agent assignee identifier"),
        ColumnDefinition("created_at", "string", "Timestamp when ticket was created"),
        ColumnDefinition("resolved_at", "string", "Timestamp when ticket was resolved"),
        ColumnDefinition("csat_score", "double", "Customer satisfaction rating (1-5)"),
        ColumnDefinition("source_type", "string", "Data origin type: synthetic, zendesk_api"),
        ColumnDefinition("source", "string", "Originating source system name ('support'/'zendesk')"),
        ColumnDefinition("entity", "string", "Originating entity name ('tickets')"),
        ColumnDefinition("run_id", "string", "Airflow pipeline batch execution UUID"),
        ColumnDefinition("ingestion_timestamp", "string", "Timestamp when record was processed into Silver"),
        ColumnDefinition("ingestion_date", "date", "Partition date extracted from ingestion_timestamp", is_partition_key=True),
    ],
)

# NOTE: HubSpot is CONTACTS ONLY.
# Orders come from OMS; Products come from PIM.
# HUBSPOT_ORDERS_TABLE and HUBSPOT_PRODUCTS_TABLE have been intentionally removed.

# Registry mapping (source, entity) -> TableDefinition
# Source ownership:
#   hubspot  -> contacts only (customer/CRM data)
#   oms      -> orders (order management)
#   pim      -> products (product catalog)
#   loyalty  -> loyalty/rewards ledger
#   marketing/mailchimp -> campaign events
#   support/zendesk     -> support tickets
TABLE_REGISTRY: Dict[Tuple[str, str], TableDefinition] = {
    ("hubspot", "contacts"): HUBSPOT_CONTACTS_TABLE,
    ("oms", "orders"): OMS_ORDERS_TABLE,
    ("pim", "products"): PIM_PRODUCTS_TABLE,
    ("loyalty", "loyalty"): LOYALTY_TABLE,
    ("marketing", "campaign_events"): MARKETING_EVENTS_TABLE,
    ("mailchimp", "campaign_events"): MARKETING_EVENTS_TABLE,
    ("support", "tickets"): ZENDESK_TICKETS_TABLE,
    ("zendesk", "tickets"): ZENDESK_TICKETS_TABLE,
}


def get_table_definition(source: str, entity: str) -> Optional[TableDefinition]:
    """Retrieve TableDefinition for given source and entity."""
    return TABLE_REGISTRY.get((source, entity))


def get_all_table_definitions() -> List[TableDefinition]:
    """Return unique list of TableDefinitions."""
    seen = set()
    result = []
    for table_def in TABLE_REGISTRY.values():
        if table_def.table_name not in seen:
            seen.add(table_def.table_name)
            result.append(table_def)
    return result


# -------------------------------------------------------------------------
# DDL & SQL Generation Utilities
# -------------------------------------------------------------------------

def _format_sql_value(val: Any, data_type: str) -> str:
    """Format a Python value into an Athena Engine v3 SQL literal."""
    if val is None:
        return "NULL"
    dtype = data_type.lower()
    if dtype in ("int", "bigint", "smallint", "tinyint"):
        try:
            return str(int(val))
        except (ValueError, TypeError):
            return "NULL"
    elif dtype in ("double", "float", "decimal"):
        try:
            return str(float(val))
        except (ValueError, TypeError):
            return "NULL"
    elif dtype == "boolean":
        return "true" if val else "false"
    elif dtype == "date":
        d_str = str(val).split("T")[0]
        return f"DATE '{d_str}'"
    elif dtype == "timestamp":
        ts_str = str(val).replace("T", " ").replace("Z", "").split(".")[0]
        return f"TIMESTAMP '{ts_str}'"
    else:
        escaped = str(val).replace("'", "''")
        return f"'{escaped}'"


def generate_athena_create_table_ddl(
    table_def: TableDefinition,
    s3_location: str,
    database_name: str = "customer360_iceberg",
) -> str:
    """
    Generate Amazon Athena Engine v3 compatible DDL for creating an Iceberg table.

    Athena Engine v3 (Trino) requirements:
    - Use plain (unquoted) identifiers — backticks are NOT supported.
    - Column COMMENT clauses inside CREATE TABLE are NOT supported.
    - Table-level COMMENT must come after the column list but before PARTITIONED BY.
    - Only supported TBLPROPERTIES: table_type, format, write_compression.
    """
    col_lines = [f"    {col.name} {col.data_type.lower()}" for col in table_def.columns]
    columns_sql = ",\n".join(col_lines)

    partition_sql = ""
    if table_def.partition_keys:
        partition_clause = ", ".join(table_def.partition_keys)
        partition_sql = f"\nPARTITIONED BY ({partition_clause})"

    props = [f"    '{k}'='{v}'" for k, v in table_def.tblproperties.items()]
    tblproperties_sql = ",\n".join(props)

    ddl = (
        f"CREATE TABLE IF NOT EXISTS {database_name}.{table_def.table_name} (\n"
        f"{columns_sql}\n"
        f")"
        f"{partition_sql}\n"
        f"LOCATION '{s3_location}'\n"
        f"TBLPROPERTIES (\n"
        f"{tblproperties_sql}\n"
        f");"
    )
    return ddl


def generate_athena_merge_query(
    table_def: TableDefinition,
    source_subquery_or_table: str,
    database_name: str = "customer360_iceberg",
) -> str:
    """
    Generate Athena Engine v3 MERGE INTO SQL for idempotent upsert using a subquery source.
    Plain (unquoted) identifiers are used — backticks are NOT supported in Athena Engine v3.
    """
    target = f"{database_name}.{table_def.table_name}"
    join_conditions = " AND ".join(
        f"target.{pk} = source.{pk}" for pk in table_def.primary_keys
    )

    update_set_clauses = [
        f"{col.name} = source.{col.name}"
        for col in table_def.columns
        if col.name not in table_def.primary_keys
    ]
    update_sql = ",\n        ".join(update_set_clauses)

    insert_cols = ", ".join(col.name for col in table_def.columns)
    insert_vals = ", ".join(f"source.{col.name}" for col in table_def.columns)

    query = (
        f"MERGE INTO {target} AS target\n"
        f"USING (\n{source_subquery_or_table}\n) AS source\n"
        f"ON {join_conditions}\n"
        f"WHEN MATCHED THEN\n"
        f"    UPDATE SET\n        {update_sql}\n"
        f"WHEN NOT MATCHED THEN\n"
        f"    INSERT ({insert_cols})\n"
        f"    VALUES ({insert_vals});"
    )
    return query


def generate_athena_merge_values_query(
    table_def: TableDefinition,
    records: List[Dict[str, Any]],
    database_name: str = "customer360_iceberg",
) -> str:
    """
    Generate Athena Engine v3 MERGE INTO with inline VALUES rows.

    This avoids the need for a staging table and is the preferred approach for
    Athena Engine v3 which does not support CREATE EXTERNAL TABLE (Hive DDL).
    Plain (unquoted) identifiers are used.
    """
    col_defs = table_def.columns
    col_names = [c.name for c in col_defs]
    value_rows = []
    for r in records:
        row_vals = [_format_sql_value(r.get(c.name), c.data_type) for c in col_defs]
        value_rows.append(f"        ({', '.join(row_vals)})")

    values_body = ",\n".join(value_rows)
    target = f"{database_name}.{table_def.table_name}"
    match_conditions = " AND ".join(
        f"target.{pk} = source.{pk}" for pk in table_def.primary_keys
    )
    update_set_clauses = [
        f"{c.name} = source.{c.name}"
        for c in col_defs
        if c.name not in table_def.primary_keys
    ]
    update_sql = ",\n        ".join(update_set_clauses)
    insert_cols = ", ".join(col_names)
    insert_vals = ", ".join(f"source.{c}" for c in col_names)

    return (
        f"MERGE INTO {target} AS target\n"
        f"USING (\n"
        f"    VALUES\n{values_body}\n"
        f") AS source ({', '.join(col_names)})\n"
        f"ON {match_conditions}\n"
        f"WHEN MATCHED THEN\n"
        f"    UPDATE SET\n        {update_sql}\n"
        f"WHEN NOT MATCHED THEN\n"
        f"    INSERT ({insert_cols})\n"
        f"    VALUES ({insert_vals});"
    )


def generate_athena_add_column_ddl(
    table_name: str,
    new_columns: List[ColumnDefinition],
    database_name: str = "customer360_iceberg",
) -> str:
    """Generate Athena ALTER TABLE ADD COLUMNS DDL for schema evolution."""
    cols_sql = ", ".join(
        f"{c.name} {c.data_type.lower()}"
        for c in new_columns
    )
    return f"ALTER TABLE {database_name}.{table_name} ADD COLUMNS ({cols_sql});"
