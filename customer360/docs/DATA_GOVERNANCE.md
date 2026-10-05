# Customer 360 Lakehouse — Data Governance Framework

This document outlines the end-to-end data governance policies, ownership boundaries, PII classifications, lifecycle retention rules, and compliance mechanisms implemented in the Customer 360 Lakehouse platform.

---

## 1. Governance Classification Matrix

To ensure absolute transparency between operational capabilities and future roadmap items, governance components are classified under three formal states:
- **[IMPLEMENTED]**: Actively enforced in code, CI/CD, data contracts, or database schemas.
- **[DOCUMENTED]**: Formally defined operational policy and standard operating procedure.
- **[PLANNED]**: Target architecture scheduled for future phases (e.g., Lake Formation tags, automated GDPR delete hooks).

---

## 2. Authoritative Source Ownership Matrix [IMPLEMENTED]

To prevent cross-system data corruption and split-brain states, domain ownership is strictly partitioned:

| Domain Entity | Authoritative Source System | Prohibited / Rejected Sources | Lakehouse Table Name | Business Primary Key | Enforcement Mechanism |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Contacts / Customers** | **HubSpot CRM** | OMS, PIM | `hubspot_contacts` | `customer_id` | Contract schema gate + Airflow separation |
| **Orders / Transactions** | **OMS (Postgres/API)** | HubSpot CRM | `oms_orders` | `order_id` | Contract schema gate + Airflow separation |
| **Product Catalog** | **PIM** | HubSpot CRM | `pim_products` | `product_id` | Contract schema gate + Airflow separation |
| **Loyalty & Rewards** | **Loyalty Engine** | HubSpot CRM | `loyalty_loyalty` | `member_id` | Contract schema gate + Airflow separation |
| **Campaign Events** | **Mailchimp / Marketing** | HubSpot CRM | `marketing_campaign_events` | `recipient_id` + `campaign_id` | Contract schema gate + Airflow separation |
| **Support Tickets** | **Zendesk** | HubSpot CRM | `zendesk_tickets` | `ticket_id` | Contract schema gate + Airflow separation |

### Hard Governance Rule
*HubSpot CRM must never ingest or update order or product information.* Any attempt to push transaction or catalog records through the HubSpot connector is rejected at the contract validation gate and diverted directly to Quarantine.

---

## 3. Data Ownership & Stewardship [DOCUMENTED]

| Domain | Business Owner (Executive) | Data Steward (Lead) | Primary Responsibilities |
| :--- | :--- | :--- | :--- |
| **Customer CRM** | VP Marketing & Sales | CRM Systems Specialist | Contact accuracy, opt-in/opt-out status, email bounce maintenance |
| **Commerce / Orders** | VP E-Commerce Operations | OMS Data Engineer | Order status integrity, refund tracking, currency formatting |
| **Merchandising / PIM**| Head of Merchandising | Catalog Specialist | SKU taxonomy, category hierarchies, pricing specifications |
| **Loyalty Programs** | Director of Customer Retention | Loyalty Analyst | Tier calculation rules, point expiration, reward accrual |
| **Growth / Campaigns** | Head of Growth Marketing | Marketing Operations Lead | Campaign attribution, click tracking, delivery metric accuracy |
| **Customer Support** | VP Customer Experience | Zendesk Administrator | Ticket taxonomy, resolution time tracking, CSAT auditability |
| **Enterprise Lakehouse**| Chief Data Officer | Principal Data Engineer | Pipeline SLA, schema evolution, Lakehouse storage optimization |

---

## 4. PII Classification & Sensitive Columns [IMPLEMENTED & DOCUMENTED]

Every column ingested into the Customer 360 Lakehouse is categorized into security classification tiers:
- **Restricted PII (Tier 1)**: Direct identifiers requiring hashing/encryption and restricted access.
- **Confidential (Tier 2)**: Commercial, behavioral, or transaction details accessible to internal analysts.
- **Internal / Public (Tier 3)**: Non-sensitive aggregated dimensions or metadata.

### PII Column Inventory

| Source / Entity | Column Name | Classification | Governance Handling |
| :--- | :--- | :--- | :--- |
| `hubspot_contacts` | `email` | **Restricted PII** | Lowercased, trimmed, validated. Restricted access via IAM/Athena. |
| `hubspot_contacts` | `phone` | **Restricted PII** | E.164 normalized. Masked in analytical reports. |
| `hubspot_contacts` | `first_name`, `last_name` | **Restricted PII** | Standardized casing. Restricted access. |
| `hubspot_contacts` | `address`, `postal_code` | **Restricted PII** | Restricted to logistics and fraud workflows. |
| `oms_orders` | `customer_id` | **Confidential** | Pseudonymized foreign key linked via identity bridge. |
| `oms_orders` | `total_amount`, `currency` | **Confidential** | Financial reporting only; validated non-negative. |
| `loyalty_loyalty` | `points_balance`, `tier` | **Confidential** | Analytical mart consumption. |
| `zendesk_tickets` | `subject`, `description` | **Restricted PII** | Potential freeform PII; excluded from general BI marts. |

---

## 5. Storage Lifecycle & Data Retention Policies [IMPLEMENTED]

Data lifecycle rules are enforced across all S3 storage tiers:

| Tier | Storage Path | Retention Policy | Compaction / Cleanup | Compliance Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **Raw** | `s3://<bucket>/raw/<source>/<entity>/` | **90 Days** (Transition to Glacier after 30 days) | Append-only raw JSON/Parquet | Replayability and historical audit |
| **Silver** | `s3://<bucket>/silver/<source>/<entity>/` | **180 Days** | Partitioned Snappy Parquet | Mid-tier reprocessing and reconciliation |
| **Quarantine** | `s3://<bucket>/quarantine/<source>/<entity>/` | **30 Days** | Partitioned Parquet with error metadata | Inspection by stewards; purge after triage |
| **Iceberg Gold** | `s3://<bucket>/iceberg/` | **Indefinite** | Automated weekly `OPTIMIZE` and snapshot expiration | ACID analytical lakehouse foundation |

### Snapshot Expiration Rule [IMPLEMENTED]
Athena/Iceberg snapshots are expired after **7 days** retaining a minimum of **3 snapshots**. This preserves time-travel capabilities for operational recovery while pruning obsolete metadata and orphan data files.

---

## 6. End-to-End Data Lineage [IMPLEMENTED]

```mermaid
flowchart TD
    subgraph Operational Sources
        HS[HubSpot CRM]
        OMS[OMS Postgres/API]
        PIM[PIM Catalog]
        LOY[Loyalty Engine]
        MKT[Marketing Email]
        ZEN[Zendesk Support]
    end

    subgraph Airflow Ingestion & Gates
        ING[Source Ingestors]
        VAL[Data Contract Validation Gate]
        QR[Quarantine S3]
        SIL[Silver Parquet S3]
        QCK[Quality & Reconciliation Gate]
    end

    subgraph Apache Iceberg Lakehouse
        IC_HS[(iceberg.hubspot_contacts)]
        IC_OM[(iceberg.oms_orders)]
        IC_PI[(iceberg.pim_products)]
        IC_LO[(iceberg.loyalty_loyalty)]
        IC_MK[(iceberg.marketing_campaign_events)]
        IC_ZE[(iceberg.zendesk_tickets)]
    end

    subgraph dbt Transformation Layer
        STG[Staging Models: stg_*]
        IDB[bridge_customer_identity]
        DIM[dim_customer, dim_product, dim_campaign, dim_date]
        FCT[fact_order, fact_support_case, fact_campaign_engagement, fact_loyalty_activity]
        MRT[customer_360, mart_customer_metrics, mart_executive_kpis]
    end

    subgraph Analytics & BI
        ATH[Amazon Athena v3]
        QS[Amazon QuickSight / Metabase / Looker]
        API[Pipeline Health & Ops API]
    end

    HS --> ING
    OMS --> ING
    PIM --> ING
    LOY --> ING
    MKT --> ING
    ZEN --> ING

    ING --> VAL
    VAL -- "Schema Invalid / Corrupt" --> QR
    VAL -- "Valid Records" --> SIL
    SIL --> QCK
    QCK -- "Reconciliation Passed" --> IC_HS & IC_OM & IC_PI & IC_LO & IC_MK & IC_ZE

    IC_HS & IC_OM & IC_PI & IC_LO & IC_MK & IC_ZE --> STG
    STG --> IDB
    IDB --> DIM
    STG --> FCT
    DIM & FCT --> MRT
    MRT --> ATH --> QS
    Airflow Ingestion & Gates --> API
```

---

## 7. Identity Resolution & Survivorship Governance [IMPLEMENTED]

### Identity Resolution Hierarchy
Identity linkage is executed deterministically with explicit match confidence levels:
1. **Rule 1 (1.00 Confidence)**: Authoritative CRM Primary Customer ID (`crm_id`).
2. **Rule 2 (0.95 Confidence)**: Normalized exact email match (`clean_email`).
3. **Rule 3 (0.90 Confidence)**: Normalized exact E.164 phone match (`clean_phone`).
4. **Rule 4 (0.50 Confidence)**: Unresolved singleton exception (isolated foreign key preserved without merging).

### Golden Record Survivorship Rules (`dim_customer`)
When synthesizing the unified golden record from conflicting sources, attributes survive according to domain authority:
- **Contact Profile (Name, Email, Phone, Lifecycle Stage)**: Survives exclusively from **HubSpot CRM**.
- **Purchase History (Total Spend, Order Count, Last Purchase Date)**: Survives exclusively from **OMS**.
- **Loyalty Metrics (Tier, Points Balance, Join Date)**: Survives exclusively from **Loyalty Engine**.
- **Customer Support (Ticket Count, Open Tickets, CSAT)**: Survives exclusively from **Zendesk**.

---

## 8. Business Glossary & KPI Definitions [IMPLEMENTED & DOCUMENTED]

| Metric / Term | Technical Definition | Business Formula |
| :--- | :--- | :--- |
| **Total Customers** | Count of distinct `master_customer_id` in `customer_360`. | `COUNT(DISTINCT master_customer_id)` |
| **Active Customer** | Customer with completed order within past 90 days. | `completed_orders_last_90d > 0` |
| **Churn Risk: High** | Days since last order > 180 days OR (ticket count > 3 AND CSAT < 3.0). | Conditional rule in `customer_360` |
| **Average Order Value (AOV)** | Net revenue divided by completed order count. | `total_net_revenue / NULLIF(total_completed_orders, 0)` |
| **Repeat Purchase Rate** | Percentage of customers with 2 or more completed orders. | `COUNT(IF total_completed_orders >= 2) / COUNT(IF total_completed_orders >= 1)` |
| **RFM Score** | Composite quintile score combining Recency, Frequency, and Monetary spend. | `CONCAT(recency_tier, frequency_tier, monetary_tier)` |
| **Customer Lifetime Value (Estimated CLV)** | Baseline empirical projection based on AOV, order frequency, and retention rate. | `(AOV * annual_orders) / (1 + discount_rate - retention_rate)` |

---

## 9. Consent-Aware Processing & Regulatory Compliance [DOCUMENTED & PLANNED]

### GDPR / CCPA Right to be Forgotten (Erasure)
1. **Iceberg In-Place Deletions**: Because Iceberg v2 supports row-level deletes (`DELETE FROM customer360_iceberg.hubspot_contacts WHERE customer_id = '...'`), deletion requests are executed without requiring full table rewrites.
2. **Quarantine & Raw Purging**: An operational script locates and deletes partition objects matching the requested customer identifier in Raw and Quarantine S3 prefixes.
3. **dbt Re-run**: Executing `dbt run --select dim_customer customer_360` immediately purges the erased identity from all gold dimensional marts.

### Opt-Out & Communication Flags [IMPLEMENTED]
The `marketing_opt_in` flag from HubSpot CRM is propagated directly to `dim_customer` and `customer_360`. Downstream reverse ETL / activation tools MUST filter on `marketing_opt_in = TRUE` before syncing to advertising audiences or marketing platforms.
