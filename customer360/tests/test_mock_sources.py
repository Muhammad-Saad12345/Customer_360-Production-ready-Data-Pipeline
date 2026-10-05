import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from mock_sources.oms.main import app as oms_app
from mock_sources.pim.main import app as pim_app
from mock_sources.loyalty.main import app as loyalty_app
from src.ingestion.oms import OmsIngest
from src.ingestion.pim import PimIngest
from src.ingestion.loyalty import LoyaltyIngest
from src.processing.silver_gate import HubSpotSilverProcessor


class MockSourceApiTestCase(unittest.TestCase):
    def test_oms_api_endpoints_are_available(self):
        client = TestClient(oms_app)
        response = client.get("/api/v1/orders")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("items", body)
        self.assertTrue(len(body["items"]) > 0)

    def test_pim_api_endpoints_are_available(self):
        client = TestClient(pim_app)
        response = client.get("/api/v1/products")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("items", body)
        self.assertTrue(len(body["items"]) > 0)

    def test_loyalty_api_endpoints_are_available(self):
        client = TestClient(loyalty_app)
        response = client.get("/api/v1/loyalty")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("items", body)
        self.assertTrue(len(body["items"]) > 0)

    def test_oms_connector_extracts_incremental_page_data(self):
        ingest = OmsIngest()
        with patch.object(ingest, "_fetch_page", side_effect=[
            {"items": [{"order_id": "ORD-001", "customer_id": "C-001", "order_ts": "2026-01-02T00:10:00Z", "order_status": "paid", "subtotal": 100, "tax": 10, "shipping": 5, "discount": 0, "net_total": 115, "currency": "USD", "channel": "web"}], "next_cursor": "page-2"},
            {"items": [{"order_id": "ORD-002", "customer_id": "C-002", "order_ts": "2026-01-02T00:20:00Z", "order_status": "paid", "subtotal": 200, "tax": 20, "shipping": 10, "discount": 0, "net_total": 230, "currency": "USD", "channel": "web"}], "next_cursor": None},
        ]):
            records = ingest.fetch_records(watermark="2026-01-01T00:00:00Z")
            self.assertEqual(len(records), 2)
            self.assertEqual(records[0]["order_id"], "ORD-001")

    def test_pim_and_loyalty_connectors_read_and_persist(self):
        pim = PimIngest()
        loyalty = LoyaltyIngest()
        with patch.object(pim, "_fetch_page", return_value={"items": [{
            "product_id": "PROD-1",
            "sku": "SKU-1",
            "name": "Test Product",
            "category": "Phones",
            "brand": "Acme",
            "price": 499.0,
            "cost": 300.0,
            "attributes": {"color": "black"},
            "status": "active",
            "launch_date": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-02T00:00:00Z",
        }], "next_cursor": None}):
            self.assertEqual(len(pim.fetch_records(watermark="2026-01-01T00:00:00Z")), 1)

        with patch.object(loyalty, "_fetch_page", return_value={"items": [{
            "loyalty_id": "LOY-1",
            "customer_id": "C-005",
            "tier": "gold",
            "points_balance": 250,
            "earn_ts": "2026-01-02T00:10:00Z",
            "redeem_ts": "2026-01-02T00:11:00Z",
            "benefit_code": "VIP10",
            "updated_at": "2026-01-02T00:12:00Z",
        }], "next_cursor": None}):
            self.assertEqual(len(loyalty.fetch_records(watermark="2026-01-01T00:00:00Z")), 1)

    def test_dq_rejects_invalid_orders_products_and_loyalty(self):
        processor = HubSpotSilverProcessor(base_dir=Path("./tmp_mock_silver"))

        invalid_order = [{
            "order_id": "ORD-INVALID-1",
            "customer_id": "",
            "order_ts": "2026-01-02T00:10:00Z",
            "order_status": "unknown",
            "subtotal": "bad",
            "tax": 0,
            "shipping": 0,
            "discount": 0,
            "net_total": 0,
            "currency": "USD",
            "channel": "web",
        }]

        invalid_product = [{
            "product_id": "PROD-INVALID-1",
            "sku": "SKU-1",
            "name": "Bad Product",
            "category": "Phones",
            "brand": "Acme",
            "price": -10,
            "cost": 50,
            "attributes": {"color": "black"},
            "status": "invalid-status",
            "launch_date": "not-a-date",
        }]

        invalid_loyalty = [{
            "loyalty_id": "LOY-INVALID-1",
            "customer_id": "",
            "tier": "diamond",
            "points_balance": -5,
            "earn_ts": "2026-01-02T00:10:00Z",
            "redeem_ts": "not-a-date",
            "benefit_code": "VIP",
        }]

        result_order = processor.process_records(source="oms", entity="orders", run_id="dq-order", raw_records=invalid_order)
        result_product = processor.process_records(source="pim", entity="products", run_id="dq-product", raw_records=invalid_product)
        result_loyalty = processor.process_records(source="loyalty", entity="loyalty", run_id="dq-loyalty", raw_records=invalid_loyalty)

        self.assertTrue(result_order["invalid_count"] >= 1)
        self.assertTrue(result_product["invalid_count"] >= 1)
        self.assertTrue(result_loyalty["invalid_count"] >= 1)


if __name__ == "__main__":
    unittest.main()
