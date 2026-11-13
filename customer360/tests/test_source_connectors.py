"""Source modes, HTTP retry behaviour, the Shopify adapter and the ingest flow."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import boto3
import pytest
import requests

from src.errors import (
    RawWriteError,
    SourceModeError,
    SourceRequestError,
    TransientError,
)
from src.ingestion.http_client import SourceHttpClient
from src.ingestion.oms import OmsIngest
from src.ingestion.shopify import (
    ShopifyClient,
    map_order,
    map_order_status,
    map_product,
)
from src.ingestion.source_mode import load_source_credentials, resolve_source_mode
from src.ingestion.support import map_zendesk_ticket

# ── source modes ────────────────────────────────────────────────────────────

class TestSourceMode:
    def test_dev_defaults_to_synthetic(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "dev")
        assert resolve_source_mode("oms") == "synthetic"

    def test_cloud_requires_explicit_mode(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "staging")
        with pytest.raises(SourceModeError):
            resolve_source_mode("oms")
        monkeypatch.setenv("C360_SOURCE_MODES", json.dumps({"oms": "mock_api"}))
        assert resolve_source_mode("oms") == "mock_api"

    def test_invalid_and_required(self, monkeypatch):
        monkeypatch.setenv("C360_SOURCE_MODES", json.dumps({"oms": "fake"}))
        with pytest.raises(SourceModeError):
            resolve_source_mode("oms")
        monkeypatch.setenv("C360_SOURCE_MODES", json.dumps({"oms": "synthetic"}))
        monkeypatch.setenv("C360_REQUIRED_SOURCE_MODE", "api")
        with pytest.raises(SourceModeError):
            resolve_source_mode("oms")

    def test_per_source_secret(self, monkeypatch):
        sm = boto3.client("secretsmanager", region_name="us-east-1")
        sm.create_secret(Name="customer360/staging/oms", SecretString=json.dumps({"shop_domain": "x.myshopify.com"}))
        monkeypatch.setenv("C360_SOURCE_SECRET_PREFIX", "customer360/staging/")
        assert load_source_credentials("oms")["shop_domain"] == "x.myshopify.com"
        assert load_source_credentials("pim") == {}  # missing secret -> empty, not an error


# ── HTTP client ─────────────────────────────────────────────────────────────

def _response(status, body=None, headers=None):
    r = requests.Response()
    r.status_code = status
    r._content = json.dumps(body or {}).encode()
    r.headers.update(headers or {})
    return r


class TestHttpClient:
    def _client(self, *responses):
        session = MagicMock()
        session.request.side_effect = list(responses)
        sleeps = []
        return SourceHttpClient("t", session=session, sleep=sleeps.append, max_attempts=3), sleeps

    def test_retries_429_with_retry_after_then_succeeds(self):
        client, sleeps = self._client(_response(429, headers={"Retry-After": "2"}), _response(200, {"ok": 1}))
        assert client.get_json("https://x") == {"ok": 1}
        assert sleeps == [2.0]

    def test_exhausted_5xx_raises_transient(self):
        client, sleeps = self._client(_response(503), _response(502), _response(500))
        with pytest.raises(TransientError):
            client.get_json("https://x")
        assert len(sleeps) == 2

    def test_4xx_is_permanent_and_not_retried(self):
        client, sleeps = self._client(_response(401))
        with pytest.raises(SourceRequestError):
            client.get_json("https://x?token=secret")
        assert sleeps == []

    def test_connection_error_is_retried(self):
        client, _ = self._client(requests.ConnectionError("down"), _response(200, {"a": 1}))
        assert client.get_json("https://x") == {"a": 1}


# ── Shopify adapter (mocked; not yet run against a live store) ──────────────

ORDER = {
    "legacyResourceId": "5001", "name": "#1001", "createdAt": "2026-10-01T10:00:00Z", "updatedAt": "2026-10-02T10:00:00Z",
    "cancelledAt": None, "displayFinancialStatus": "PAID", "displayFulfillmentStatus": "FULFILLED", "currencyCode": "BRL",
    "sourceName": "web", "customer": {"legacyResourceId": "77", "email": "a@b.com", "phone": None},
    "subtotalPriceSet": {"shopMoney": {"amount": "100.0"}}, "totalTaxSet": {"shopMoney": {"amount": "10.0"}},
    "totalShippingPriceSet": {"shopMoney": {"amount": "5.0"}}, "totalDiscountsSet": {"shopMoney": {"amount": "0.0"}},
    "totalPriceSet": {"shopMoney": {"amount": "115.0"}},
}


class TestShopify:
    def test_order_mapping_meets_oms_contract(self):
        from src.processing.silver_gate import HubSpotSilverProcessor

        row = map_order(ORDER)
        assert row["order_id"] == "shopify-5001" and row["customer_id"] == "shopify-77"
        assert row["order_status"] == "fulfilled" and row["net_total"] == 115.0
        result = HubSpotSilverProcessor().process_records("oms", "orders", "r", raw_records=[row])
        assert result["valid_count"] == 1, result["quarantine_records"]

    def test_status_mapping(self):
        assert map_order_status({"cancelledAt": "2026-01-01"}) == "cancelled"
        assert map_order_status({"displayFinancialStatus": "REFUNDED"}) == "refunded"
        assert map_order_status({"displayFinancialStatus": "PENDING"}) == "pending"
        assert map_order_status({"displayFinancialStatus": "PAID", "displayFulfillmentStatus": "UNFULFILLED"}) == "paid"

    def test_product_mapping(self):
        row = map_product({"legacyResourceId": "9", "title": "Lamp", "vendor": "Acme", "productType": "home",
                           "status": "ACTIVE", "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-02T00:00:00Z",
                           "variants": {"nodes": [{"sku": "L-1", "price": "20.00", "inventoryItem": {"unitCost": {"amount": "8.5"}}}]}})
        assert (row["product_id"], row["sku"], row["price"], row["cost"], row["status"]) == ("shopify-9", "L-1", 20.0, 8.5, "active")

    def test_client_credentials_pagination_and_throttle(self):
        http = MagicMock()
        http.headers = {}
        http.request_json.return_value = {"access_token": "tok"}
        page1 = {"data": {"orders": {"nodes": [ORDER], "pageInfo": {"hasNextPage": True, "endCursor": "c1"}}}}
        throttled = {"errors": [{"message": "Throttled", "extensions": {"code": "THROTTLED"}}]}
        page2 = {"data": {"orders": {"nodes": [{**ORDER, "legacyResourceId": "5002"}], "pageInfo": {"hasNextPage": False}}}}
        http.post_json.side_effect = [page1, throttled, page2]
        client = ShopifyClient({"shop_domain": "s.myshopify.com", "client_id": "id", "client_secret": "sec"}, http=http, sleep=lambda s: None)
        rows = client.fetch_orders("2026-10-01T00:00:00Z")
        assert [r["order_id"] for r in rows] == ["shopify-5001", "shopify-5002"]
        assert http.headers["X-Shopify-Access-Token"] == "tok"
        assert http.post_json.call_args_list[1].args[1]["variables"]["after"] == "c1"
        assert "updated_at:>=" in http.post_json.call_args_list[0].args[1]["variables"]["query"]

    def test_bad_secret_rejected(self):
        with pytest.raises(SourceModeError):
            ShopifyClient({"shop_domain": "example.com"})
        with pytest.raises(SourceModeError):
            ShopifyClient({"shop_domain": "s.myshopify.com"}, http=MagicMock(headers={})).authenticate()


def test_zendesk_mapping():
    row = map_zendesk_ticket({"id": 3, "requester_id": 44, "via": {"channel": "web"}, "type": "problem",
                              "priority": "high", "status": "solved", "created_at": "2026-01-01T00:00:00Z",
                              "updated_at": "2026-01-02T00:00:00Z", "satisfaction_rating": {"score": "good"}})
    assert (row["ticket_id"], row["customer_id"], row["channel"], row["csat_score"]) == ("zd-3", "zd-user-44", "web", 5.0)
    assert row["resolved_at"] == "2026-01-02T00:00:00Z"


# ── ingest flow ─────────────────────────────────────────────────────────────

class TestIngestFlow:
    def test_api_failure_never_falls_back_to_synthetic(self, monkeypatch):
        ingest = OmsIngest(run_id="r-api")
        ingest.source_mode = "api"
        monkeypatch.setattr(ingest, "_fetch_api", MagicMock(side_effect=TransientError("down")))
        with pytest.raises(TransientError):
            ingest.run()
        assert ingest.checkpoint_store.get_or_create("oms", "orders").get("last_successful_run_id") != "r-api"

    def test_raw_write_failure_keeps_checkpoint(self, monkeypatch):
        ingest = OmsIngest(run_id="r-fail")
        before = ingest.checkpoint_store.get_or_create("oms", "orders")["last_successful_watermark"]
        monkeypatch.setattr(ingest.s3, "upload_jsonlines", MagicMock(side_effect=RuntimeError("AccessDenied")))
        with pytest.raises(RawWriteError):
            ingest.run()
        assert ingest.checkpoint_store.get_or_create("oms", "orders")["last_successful_watermark"] == before

    def test_retry_of_completed_run_is_idempotent(self, monkeypatch):
        first = OmsIngest(run_id="r-same").run()
        assert first["records_ingested"] > 0
        again = OmsIngest(run_id="r-same")
        monkeypatch.setattr(again, "fetch_records", MagicMock(side_effect=AssertionError("must not refetch")))
        second = again.run()
        assert (second["s3_raw_key"], second["records_ingested"]) == (first["s3_raw_key"], first["records_ingested"])

    def test_records_are_tagged_with_mode(self):
        result = OmsIngest(run_id="r-tag").run()
        body = boto3.client("s3").get_object(Bucket="c360-bucket786", Key=result["s3_raw_key"])["Body"].read().decode()
        assert all(json.loads(line)["source_mode"] == "synthetic" for line in body.splitlines())

    def test_dataset_mode_reads_uploaded_files(self, monkeypatch):
        s3 = boto3.client("s3")
        s3.put_object(Bucket="c360-bucket786", Key="datasets/oms/orders/olist.jsonl",
                      Body=json.dumps({**map_order(ORDER), "source_type": "olist"}))
        ingest = OmsIngest(run_id="r-ds")
        ingest.source_mode = "dataset"
        assert ingest.run()["records_ingested"] == 1

    def test_dataset_mode_without_files_fails(self):
        ingest = OmsIngest(run_id="r-ds2")
        ingest.source_mode = "dataset"
        with pytest.raises(SourceModeError):
            ingest.run()
