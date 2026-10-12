# Customer 360 — Comprehensive Test Results & Validation Evidence

**Test Execution Date:** 2026-10-12  
**Target Environment:** Staging (Isolated) & Production (Live)  
**Account:** `734143568343` (`us-east-1`)  

---

## 1. Unit & Integration Test Suite

* **Framework:** `pytest 8.3.3` on Python `3.11.9`
* **Test Count:** **211 passed, 0 failed, 0 skipped**
* **Duration:** 42.38s
* **Status:** **PASS**

---

## 2. Live Healthcheck Verification (Isolated Staging vs Production)

* **Callable:** `dags.customer360_healthcheck.test_aws_connectivity`
* **Status:** **PASS (100% across both tiers)**

### Staging Healthcheck Output
```json
{
  "s3": "OK: bucket='c360-bucket786', key_count=5",
  "glue": "OK: database='customer360_iceberg_stage'",
  "athena": "OK: workgroup='customer360-staging', state='ENABLED'",
  "secrets_manager": "OK: secret='c_360_secret_manager' (verified via describe_secret)"
}
```

### Production Healthcheck Output
```json
{
  "s3": "OK: bucket='c360-bucket786', key_count=5",
  "glue": "OK: database='customer360_iceberg'",
  "athena": "OK: workgroup='primary', state='ENABLED'",
  "secrets_manager": "OK: secret='c_360_secret_manager' (verified via describe_secret)"
}
```

---

## 3. Staging Bounded Ingestion & Lakehouse Pipeline Execution

### Step 1: Real HubSpot Contact Ingestion
* **Source:** HubSpot API via [HubSpotIngest](file:///d:/Data%20Engineer%20Projects/Customer%20360/customer360/src/ingestion/hubspot.py)
* **Run ID:** `7ba9be18-24bc-4e73-a656-5b2df260334f`
* **Status:** **PASS** (Raw JSON persisted to `s3://c360-bucket786/raw/hubspot/contacts/`)

### Step 2: Silver Gate Validation & Parquet Serialization
* **Processor:** [HubSpotSilverProcessor](file:///d:/Data%20Engineer%20Projects/Customer%20360/customer360/src/processing/silver_gate.py)
* **Input Records:** 27
* **Valid Records:** 27
* **Invalid / Quarantined:** 0 (Rejection Rate: 0.0%)
* **Status:** **PASS** (Parquet written to `s3://c360-bucket786/silver/hubspot/contacts/`)

### Step 3: Apache Iceberg Load into Staging
* **Loader:** [IcebergLoader](file:///d:/Data%20Engineer%20Projects/Customer%20360/customer360/src/lakehouse/iceberg_loader.py)
* **Target Table:** `customer360_iceberg_stage.hubspot_contacts`
* **Athena Workgroup:** `customer360-staging`
* **Silver Row Count:** 27
* **Deduped Row Count:** 27
* **Status:** **PASS**

---

## 4. Athena Query Verification & Strict Idempotency Evidence

### Initial Load Row Count
* **Query:** `SELECT COUNT(*) as cnt FROM customer360_iceberg_stage.hubspot_contacts`
* **Query Execution ID:** `041efc3d-f68c-4883-9710-eea4c5bae7e9`
* **Verified Count:** **27 rows**
* **Status:** **PASS**

### Second Load (Re-run with Same Data)
* **Action:** Re-executed `IcebergLoader.load_silver_run` with same `run_id` (`7ba9be18-24bc-4e73-a656-5b2df260334f`)
* **Loader Result:** `status="success", idempotent=True`
* **Verification Query:** `SELECT COUNT(*) as cnt FROM customer360_iceberg_stage.hubspot_contacts`
* **Query Execution ID:** `bcf9993c-c18b-453f-bda3-ac17a7bd1a4f`
* **Verified Count:** **27 rows (Zero duplicate rows introduced)**
* **Status:** **PASS**

---

## 5. Production Lakehouse Table Inventory (25 Tables / Views)

| Table Name | Layer | Verified Production Rows | Status |
| :--- | :--- | :--- | :--- |
| `bridge_customer_identity` | Identity Resolution | 27 | ✅ SUCCEEDED |
| `customer_360` | Marts | 21 | ✅ SUCCEEDED |
| `dim_campaign` | Dimensions | 1 | ✅ SUCCEEDED |
| `dim_customer` | Dimensions | 21 | ✅ SUCCEEDED |
| `dim_date` | Dimensions | 400 | ✅ SUCCEEDED |
| `dim_product` | Dimensions | 4 | ✅ SUCCEEDED |
| `fact_campaign_engagement` | Facts | 1 | ✅ SUCCEEDED |
| `fact_loyalty_activity` | Facts | 2 | ✅ SUCCEEDED |
| `fact_order` | Facts | 11 | ✅ SUCCEEDED |
| `fact_support_case` | Facts | 2 | ✅ SUCCEEDED |
| `hubspot_contacts` | Silver Iceberg | 8 | ✅ SUCCEEDED |
| `int_customer_identities_unioned` | Intermediate View | 28 | ✅ SUCCEEDED |
| `loyalty_loyalty` | Silver Iceberg | 3 | ✅ SUCCEEDED |
| `marketing_campaign_events` | Silver Iceberg | 1 | ✅ SUCCEEDED |
| `mart_customer_metrics` | Marts | 4 | ✅ SUCCEEDED |
| `mart_executive_kpis` | Marts | 1 | ✅ SUCCEEDED |
| `oms_orders` | Silver Iceberg | 14 | ✅ SUCCEEDED |
| `pim_products` | Silver Iceberg | 5 | ✅ SUCCEEDED |
| `stg_hubspot_contacts` | Staging View | 8 | ✅ SUCCEEDED |
| `stg_loyalty` | Staging View | 3 | ✅ SUCCEEDED |
| `stg_marketing_campaign_events` | Staging View | 1 | ✅ SUCCEEDED |
| `stg_oms_orders` | Staging View | 14 | ✅ SUCCEEDED |
| `stg_pim_products` | Staging View | 5 | ✅ SUCCEEDED |
| `stg_zendesk_tickets` | Staging View | 2 | ✅ SUCCEEDED |
| `zendesk_tickets` | Silver Iceberg | 2 | ✅ SUCCEEDED |
