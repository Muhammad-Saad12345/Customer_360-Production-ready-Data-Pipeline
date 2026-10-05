# Customer 360 Lakehouse Layer Architecture & Runbook
**Engineered with Apache Iceberg, AWS Glue Data Catalog, Amazon Athena Engine v3, and Amazon S3**

---

## 1. Architectural Overview

The Customer 360 pipeline establishes a high-performance, ACID-compliant **Apache Iceberg Lakehouse Layer** positioned immediately downstream of validated **Silver Parquet** datasets in Amazon S3. 

### End-to-End Pipeline Architecture

```text
+-----------------------------------------------------------------------------------------+
|                                    DATA SOURCES                                         |
|   HubSpot CRM  |  OMS (PostgreSQL)  |  Loyalty Engine  |  PIM Catalog  |  Mailchimp /   |
|   (Contacts)   |  (Orders)          |  (Accounts)      |  (Products)   |  SendGrid      |
+--------+----------------+-------------------+----------------+-----------------+--------+
         |                |                   |                |                 |
         v                v                   v                v                 v
+-----------------------------------------------------------------------------------------+
|                               APACHE AIRFLOW ORCHESTRATION                              |
|   Tier 1: [ingest_*]  -->  Tier 2: [validate_* (Silver Gate)]  -->  Tier 3: [quality_*] |
+-----------------------------------------------------------------------------------------+
                                                               |
                                                               v  (Validated Clean Parquet)
+-----------------------------------------------------------------------------------------+
|                                  S3 SILVER DATA LAKE                                    |
|   s3://{BUCKET}/silver/{source}/{entity}/ingestion_date=YYYY-MM-DD/run_id={RUN_ID}/     |
+-----------------------------------------------------------------------------------------+
                                                               |
                                                               v  (Tier 4: Iceberg Loader)
+-----------------------------------------------------------------------------------------+
|                                 ICEBERG LAKEHOUSE LAYER                                 |
|                                                                                         |
|   +-----------------------+     +------------------------+     +--------------------+   |
|   |   AWS Glue Catalog    | <-> |  Amazon Athena (v3)    | <-> |   Amazon S3        |   |
|   |   (Iceberg Metadata)  |     |  (ACID Query Engine)   |     |   (Data & Manifest)|   |
|   |                       |     |                        |     |                    |   |
|   |  - Database:          |     |  - MERGE INTO (Upsert) |     |  - Data Files      |   |
|   |    customer360_iceberg|     |  - Inline VALUES Stage |     |    (Parquet)       |   |
|   |  - table_type=ICEBERG |     |  - ALTER TABLE Add Col |     |  - Metadata Tree   |   |
|   |  - Schema & Partitions|     |  - Time-Travel Queries |     |    (Snapshots)     |   |
|   |                       |     |  - Deduplication       |     |                    |   |
|   +-----------------------+     +------------------------+     +--------------------+   |
|                                                                                         |
|   Active Lakehouse Tables (5 Canonical):                                                |
|     1. hubspot_contacts           (PK: customer_id)  [Source: HubSpot CRM Contacts]     |
|     2. oms_orders                 (PK: order_id)     [Source: OMS Orders]               |
|     3. pim_products               (PK: product_id)   [Source: PIM Products]             |
|     4. loyalty_loyalty            (PK: member_id)    [Source: Loyalty Engine]           |
|     5. marketing_campaign_events  (PK: record_id)    [Source: Mailchimp / Marketing]    |
+-----------------------------------------------------------------------------------------+
                                                               |
                                                               v  (Downstream Consumption)
+-----------------------------------------------------------------------------------------+
|                              DOWNSTREAM CONSUMERS & MARTS                               |
|   dbt Core (dbt-athena)  -->  Identity Resolution  -->  Golden Records  -->  BI / Ops   |
+-----------------------------------------------------------------------------------------+
```

---

## 2. Architectural Decision Record (ADR): Inline VALUES MERGE INTO vs External Staging

### Context
When loading validated Silver Parquet rows into Apache Iceberg via Amazon Athena Engine v3:
1. **Option A: External Staging Table**: Registering a temporary Glue external table over Silver Parquet, running `MERGE INTO target USING staging`, and dropping the staging table.
2. **Option B: Inline VALUES MERGE INTO**: Reading Silver Parquet in Python, deduplicating business keys intra-batch, escaping SQL literals, and issuing an atomic `MERGE INTO target USING (VALUES ...) ON target.pk = source.pk`.

### Decision
We **selected Option B: Inline VALUES MERGE INTO (`IcebergLoader`)**.

### Justification Matrix

| Dimension | Option A: External Staging Table | Option B: Inline VALUES MERGE (Selected) |
| :--- | :--- | :--- |
| **Catalog Friction** | High: Requires creating and deleting ephemeral Glue Catalog tables per batch run. | Zero: No temporary Glue tables created; direct snapshot commit. |
| **Execution Latency** | Slow: 3 Athena queries per batch (Create Staging Table, MERGE INTO, Drop Staging Table). | Fast: 1 atomic Athena query per batch or chunk. |
| **Query Cost** | Higher: Multiple DDL/DML invocations and S3 catalog operations. | Lower: Single DML statement executed within Athena Engine v3 workgroup. |
| **Batch Chunking** | Not easily partitionable inside Athena without custom partitions. | Fully managed: Batches > 500 rows are cleanly chunked to comply with Athena query string size limits. |
| **Intra-Batch Dedup** | Handled in SQL via `ROW_NUMBER() OVER (...)`. | Handled in-memory before query generation, guaranteeing clean source inputs. |

---

## 3. Authoritative Table Catalog & Schema Specifications

All tables reside in the AWS Glue Data Catalog database specified by `ICEBERG_DATABASE` (default: `customer360_iceberg`), with data stored in `s3://{BUCKET}/iceberg/{table_name}/`.

All tables are partitioned by `ingestion_date` (`DATE`) and maintain Apache Iceberg v2 table format properties with Snappy Parquet write compression.

> [!NOTE]
> **Source Ownership Rule**: HubSpot is **CONTACTS ONLY**. Orders are owned by OMS (`oms_orders`); Products are owned by PIM (`pim_products`). Obsolete tables (`hubspot_orders`, `hubspot_products`, `marketing_events_email_events`, `marketing_events_campaigns`) have been purged from the catalog.

### 3.1 Table Schema Details

#### 1. `hubspot_contacts`
- **Source / Entity**: `hubspot` / `contacts`
- **Primary Key**: `customer_id` (`string`)
- **Partition Key**: `ingestion_date` (`date`)
- **Columns**:
  - `customer_id` (`string`, PK) — HubSpot unique contact ID
  - `record_id` (`string`) — Unified record identifier
  - `first_name` (`string`) — Contact first name
  - `last_name` (`string`) — Contact last name
  - `email` (`string`) — Primary email address
  - `phone` (`string`) — Phone number
  - `customer_stage` (`string`) — Lifecycle stage (lead, subscriber, customer)
  - `created_at` (`string`) — Contact creation timestamp
  - `updated_at` (`string`) — Last modification timestamp
  - `source` (`string`) — Originating source system (`'hubspot'`)
  - `entity` (`string`) — Originating entity name (`'contacts'`)
  - `run_id` (`string`) — Pipeline execution UUID
  - `ingestion_timestamp` (`string`) — Silver processing timestamp
  - `ingestion_date` (`date`, Partition) — Extracted partition date

#### 2. `oms_orders`
- **Source / Entity**: `oms` / `orders`
- **Primary Key**: `order_id` (`string`)
- **Partition Key**: `ingestion_date` (`date`)
- **Columns**:
  - `order_id` (`string`, PK) — OMS order identifier
  - `customer_id` (`string`) — Customer identifier linked to contact
  - `order_ts` (`string`) — Timestamp order was placed
  - `order_status` (`string`) — Lifecycle state (paid, shipped, fulfilled, cancelled)
  - `subtotal` (`double`) — Subtotal amount
  - `tax` (`double`) — Sales tax amount
  - `shipping` (`double`) — Shipping charge
  - `discount` (`double`) — Applied promotional discount
  - `net_total` (`double`) — Net total payable
  - `currency` (`string`) — ISO-4217 currency code (`'USD'`)
  - `channel` (`string`) — Order channel (web, mobile, instore)
  - `updated_at` (`string`) — Timestamp of order status update
  - `source` (`string`) — Originating source system (`'oms'`)
  - `entity` (`string`) — Originating entity name (`'orders'`)
  - `run_id` (`string`) — Pipeline execution UUID
  - `ingestion_timestamp` (`string`) — Silver processing timestamp
  - `ingestion_date` (`date`, Partition) — Extracted partition date

#### 3. `pim_products`
- **Source / Entity**: `pim` / `products`
- **Primary Key**: `product_id` (`string`)
- **Partition Key**: `ingestion_date` (`date`)
- **Columns**:
  - `product_id` (`string`, PK) — Unique product catalog ID
  - `sku` (`string`) — Stock Keeping Unit code
  - `name` (`string`) — Full commercial product name
  - `category` (`string`) — Catalog taxonomy category
  - `brand` (`string`) — Brand / manufacturer label
  - `price` (`double`) — Unit retail price
  - `currency` (`string`) — ISO-4217 currency code
  - `is_active` (`boolean`) — Catalog availability flag
  - `updated_at` (`string`) — Catalog record update timestamp
  - `source` (`string`) — Originating source system (`'pim'`)
  - `entity` (`string`) — Originating entity name (`'products'`)
  - `run_id` (`string`) — Pipeline execution UUID
  - `ingestion_timestamp` (`string`) — Silver processing timestamp
  - `ingestion_date` (`date`, Partition) — Extracted partition date

#### 4. `loyalty_loyalty`
- **Source / Entity**: `loyalty` / `loyalty`
- **Primary Key**: `member_id` (`string`)
- **Partition Key**: `ingestion_date` (`date`)
- **Columns**:
  - `member_id` (`string`, PK) — Unique rewards membership ID
  - `customer_id` (`string`) — Customer identifier linked to contact
  - `tier` (`string`) — Membership tier (Bronze, Silver, Gold, Platinum)
  - `points_balance` (`bigint`) — Current active point balance
  - `points_delta` (`bigint`) — Net change in points this event
  - `earn_ts` (`string`) — Timestamp of point accrual
  - `redeem_ts` (`string`) — Timestamp of point redemption
  - `benefit_code` (`string`) — Voucher benefit code
  - `updated_at` (`string`) — Timestamp of record update
  - `source` (`string`) — Originating source system (`'loyalty'`)
  - `entity` (`string`) — Originating entity name (`'loyalty'`)
  - `run_id` (`string`) — Pipeline execution UUID
  - `ingestion_timestamp` (`string`) — Silver processing timestamp
  - `ingestion_date` (`date`, Partition) — Extracted partition date

#### 5. `marketing_campaign_events`
- **Source / Entity**: `marketing` / `campaign_events` (also handles `mailchimp`)
- **Primary Key**: `record_id` (`string`)
- **Partition Key**: `ingestion_date` (`date`)
- **Columns**:
  - `record_id` (`string`, PK) — Composite unique campaign event ID
  - `campaign_id` (`string`) — Marketing campaign broadcast ID
  - `customer_id` (`string`) — Customer ID matched to recipient
  - `channel` (`string`) — Engagement channel (email, sms)
  - `send_ts` (`string`) — Timestamp message was sent
  - `open_ts` (`string`) — Timestamp message was opened
  - `click_ts` (`string`) — Timestamp link was clicked
  - `audience_id` (`string`) — Mailing list / audience segment ID
  - `offer_id` (`string`) — Promotional voucher ID
  - `member_id` (`string`) — Subscriber membership ID
  - `email_address` (`string`) — Recipient email address
  - `status` (`string`) — Engagement status (subscribed, opened, clicked)
  - `source_type` (`string`) — Provider (`mailchimp`, `synthetic`)
  - `source` (`string`) — Originating source system (`'marketing'`)
  - `entity` (`string`) — Originating entity name (`'campaign_events'`)
  - `run_id` (`string`) — Pipeline execution UUID
  - `ingestion_timestamp` (`string`) — Silver processing timestamp
  - `ingestion_date` (`date`, Partition) — Extracted partition date

---

## 4. Idempotency, Deduplication & Schema Evolution

### 4.1 Two-Tier Deduplication & Zero-Duplicate Guarantee
The pipeline guarantees that re-running ingestion or pipeline tasks produces zero duplicate records:
1. **Tier 1 (Intra-batch deduplication)**: In Python, `IcebergLoader.deduplicate_records_by_primary_key()` groups by the primary key, preserving the latest record in the batch.
2. **Tier 2 (Athena Engine v3 MERGE INTO)**:
   ```sql
   MERGE INTO "customer360_iceberg"."{table_name}" AS target
   USING (
     VALUES ('cust-001', 'John', 'Doe', 'john@example.com', ...)
   ) AS source(customer_id, first_name, last_name, email, ...)
   ON target.customer_id = source.customer_id
   WHEN MATCHED THEN
     UPDATE SET first_name = source.first_name, email = source.email, ...
   WHEN NOT MATCHED THEN
     INSERT (customer_id, first_name, last_name, email, ...)
     VALUES (source.customer_id, source.first_name, source.last_name, source.email, ...);
   ```

### 4.2 Dynamic Glue Schema Evolution
When new columns arrive in validated Silver Parquet:
1. `IcebergLoader.handle_schema_evolution()` queries the live AWS Glue Data Catalog table definition to determine the true set of existing columns.
2. If new columns exist in the Parquet schema:
   ```sql
   ALTER TABLE "customer360_iceberg"."{table_name}" ADD COLUMNS (
     {new_col} {data_type} COMMENT 'Evolved column added dynamically'
   );
   ```
3. Athena updates the Iceberg metadata manifest in place. Historical records read `NULL` for the new column; incoming records populate it immediately.

---

## 5. Operations & CLI Run Guide

### Quick Start: Running the Lakehouse via CLI

1. **Verify AWS Connectivity**:
   ```bash
   uv run python src/main.py validate
   ```

2. **Ingest All Sources**:
   ```bash
   # Ensure mock sources are running first:
   # uv run python scripts/start_mock_sources.py
   uv run python src/main.py all-sources
   ```

3. **Process Records Through Silver Quality Gate**:
   ```bash
   uv run python src/main.py silver-gate
   ```

4. **Load into Apache Iceberg Tables**:
   ```bash
   # Dry-run validation (checks schema, dedup, and DDL generation):
   uv run python src/main.py iceberg-load --dry-run

   # Live Lakehouse MERGE INTO execution:
   uv run python src/main.py iceberg-load
   ```

5. **Run the Automated Lakehouse Acceptance Check**:
   ```bash
   uv run python scripts/live_acceptance_check.py
   ```

6. **Execute Full Lakehouse Unit & Integration Test Suite**:
   ```bash
   uv run pytest tests/test_iceberg_lakehouse.py -v
   uv run pytest tests/test_e2e_pipeline.py -v
   uv run pytest tests/test_data_quality_idempotency_evolution.py -v
   ```

---

## 6. Lakehouse Maintenance & Snapshot Management

Apache Iceberg creates a snapshot metadata tree on every commit. To maintain optimal query performance and minimize storage costs:

### 6.1 Snapshot Expiration
Run periodic maintenance to expire snapshots older than the retention window (e.g., 7 days):
```sql
CALL "customer360_iceberg".system.expire_snapshots(
  table_name => '{table_name}',
  older_than => TIMESTAMP '2026-09-23 00:00:00.000 UTC',
  retain_last => 10
);
```

### 6.2 Compaction (File Optimization)
To merge small files generated by frequent hourly incremental MERGE operations:
```sql
OPTIMIZE "customer360_iceberg"."{table_name}" REWRITE DATA
USING BIN_PACK
WHERE ingestion_date >= CURRENT_DATE - INTERVAL '7' DAY;
```

### 6.3 Orphan File Removal
To clean up unreferenced data files in S3 after snapshot expiration:
```sql
CALL "customer360_iceberg".system.remove_orphan_files(
  table_name => '{table_name}',
  older_than => TIMESTAMP '2026-09-23 00:00:00.000 UTC'
);
```
