# OMS and PIM API integration guide

## Direct answer: does the current OMS/PIM setup work in production?

**Before this change: no.** `src/ingestion/oms.py` and `pim.py` called
`GET http://localhost:8001/api/v1/orders` and `:8002/api/v1/products` (the
FastAPI mocks in `mock_sources/`) with no authentication. In a Lambda there is
nothing on localhost, so the call failed and, in staging,
`ALLOW_SYNTHETIC_FALLBACK=true` (set by `settings.py`) silently replaced the
data with 13 hard-coded orders / 8 products, reported as a successful run.
In prod the call failed loudly. There was no real OMS or PIM.

**After this change:** each source has an explicit mode and never falls back:

| Mode | OMS / PIM data comes from | Use it for | Status |
|---|---|---|---|
| `synthetic` | in-code generator (`_synthetic_records`) | dev, tests, first staging runs | implemented, tested |
| `mock_api` | the `mock_sources/` REST contract at `OMS_BASE_URL`/`PIM_BASE_URL` | proving HTTP/pagination code against a server you host | implemented, tested with mocked HTTP |
| `dataset` | JSON/JSONL files you upload to `s3://<bucket>/datasets/oms/orders/` and `datasets/pim/products/` (e.g. Olist converted to the contract) | real-shaped history without an API | implemented, tested with moto |
| `api` | **Shopify Admin GraphQL API** (`src/ingestion/shopify.py`) | staging with a dev store, prod | implemented, unit-tested with recorded responses; **not yet run against a live store** |

Every raw record is tagged `source_mode`, and prod sets
`C360_REQUIRED_SOURCE_MODE=api`, so prod refuses to run any source that is not
on a real API.

## Why Shopify

The project needs an order system and a product catalogue with a real,
authenticated, paginated, rate-limited API. A Shopify development store is
free, has both (orders = OMS, products = PIM) behind one app, and is a
realistic e-commerce source. The Olist dataset is real order/product history
but has no API; it fits `dataset` mode, or it can be loaded into the dev store.

## Extraction flow (`api` mode)

```
Step Functions Map item {source: oms, entity: orders, run_id}
 └─ ingest Lambda
     ├─ resolve_source_mode("oms") -> "api"            (C360_SOURCE_MODES)
     ├─ read checkpoint s3://<bucket>/checkpoints/oms/orders.json
     │     watermark = last_successful_watermark (minus 15 min lookback)
     ├─ load secret customer360/<env>/oms               (JSON, never logged)
     ├─ token: client-credentials grant  POST https://<shop>/admin/oauth/access_token
     │         (or a stored access_token)
     ├─ POST https://<shop>/admin/api/<version>/graphql.json
     │     orders(first: 100, after: <cursor>, sortKey: UPDATED_AT,
     │            query: "updated_at:>='<watermark>'")
     │     429/5xx/network -> backoff (Retry-After honoured), then TransientError
     │     GraphQL THROTTLED -> backoff, then TransientError
     │     401/403/400     -> SourceRequestError (no retry; fix the secret)
     ├─ map each order to the OMS contract (shopify.map_order)
     ├─ write raw/oms/orders/ingestion_date=<d>/run_id=<id>/data.json   (RawWriteError if it fails)
     ├─ write metadata, then move the checkpoint to the newest updated_at
     └─ return {status, records_ingested, s3_raw_key, source_mode: "api"}
 └─ silver gate validates that exact key -> silver/ + quarantine/
```

Field mapping (orders): `legacyResourceId → order_id "shopify-<id>"`,
`customer.legacyResourceId → customer_id`, `customer.email → customer_email`
(used by the identity bridge), money sets → `subtotal/tax/shipping/discount/net_total`,
financial/fulfilment status → `order_status` (`shopify.map_order_status`).
Products: first variant's `sku`, `price`, `inventoryItem.unitCost → cost`,
`productType → category`, `vendor → brand`, `status` lower-cased.

## Setting up the Shopify development store (manual)

Check these steps against Shopify's current documentation when you do them;
Shopify changed app creation in 2026 (new custom apps are created in the Dev
Dashboard and use the client-credentials grant).

1. Create a Shopify Partner account and a **development store**.
2. Dev Dashboard → create an app → configure Admin API scopes:
   `read_orders`, `read_all_orders` (history older than 60 days),
   `read_products`, `read_inventory` (unit cost), `read_customers` (customer email).
   Install the app on the development store.
3. Note the **client ID** and **client secret** (or the Admin API access token
   for an older custom app).
4. Load data: import products with Shopify's product CSV import; create test
   orders in the admin (or with the `orderCreate` mutation, which needs
   `write_orders`). A seeding script from Olist is **not** part of this repo.
5. Create the secrets (M8 in `MANUAL_AWS_SETUP_GUIDE.md`):
   ```json
   {"shop_domain": "your-store.myshopify.com", "client_id": "...", "client_secret": "...", "api_version": "2026-10"}
   ```
   as `customer360/staging/oms` and the same content as `customer360/staging/pim`.
6. Check the credentials from CloudShell before switching the mode (prints
   only the HTTP status and the shop name):
   ```bash
   SHOP=your-store.myshopify.com
   read -s -p "client id: " CID; echo; read -s -p "client secret: " CSEC; echo
   TOKEN=$(curl -s -X POST "https://$SHOP/admin/oauth/access_token" \
     -d grant_type=client_credentials -d client_id="$CID" -d client_secret="$CSEC" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("access_token",""))')
   curl -s -o /dev/null -w '%{http_code}\n' -X POST "https://$SHOP/admin/api/2026-10/graphql.json" \
     -H "X-Shopify-Access-Token: $TOKEN" -H 'Content-Type: application/json' -d '{"query":"{ shop { name } }"}'
   unset CID CSEC TOKEN
   ```
   `200` means the app, scopes and secret are right.
7. In a PR set `oms = "api"` and `pim = "api"` in `source_modes`
   (`environments/staging/main.tf`), deploy, run once, then check the first raw
   record has `"source_mode": "api"` and `"source_type": "shopify"`.

## Using Olist without an API (`dataset` mode)

Convert the Olist CSVs to the OMS/PIM contracts (one JSON object per line,
fields as in `map_order` / `map_product`), upload to
`s3://c360-bucket786/datasets/oms/orders/` and `datasets/pim/products/`, and
set the mode to `dataset`. The watermark still applies, so the first run loads
everything and later runs only rows with a newer `updated_at`. A converter
script is not included.

## Tests

| Level | Where | Runs in CI |
|---|---|---|
| Unit: mapping, status rules, secret validation | `tests/test_source_connectors.py::TestShopify` | yes |
| Unit: retry/429/4xx behaviour | `tests/test_source_connectors.py::TestHttpClient` | yes |
| Mock API: REST contract + pagination | `tests/test_mock_sources.py` | yes |
| Flow: no fallback, checkpoint safety, idempotent retry, dataset mode | `tests/test_source_connectors.py::TestIngestFlow` | yes |
| End to end (all handlers, synthetic, moto) | `tests/test_lambda_handlers.py::test_synthetic_run_through_every_handler` | yes |
| Sandbox integration against a live dev store | step 6–7 above | no (manual, needs the secret) |

## Known limits

* Shopify returns orders older than 60 days only with `read_all_orders`.
* Customer fields on orders can be restricted for apps without approved
  protected-customer-data access; orders without a customer get an empty
  `customer_id` and are quarantined by the silver gate.
* Products without a unit cost (`read_inventory` missing or cost not set) are
  quarantined (`cost` is required by the PIM contract).
* One `MAX_PAGES = 500` safety cap per run (50,000 orders); a larger backfill
  needs several runs or `dataset` mode.
