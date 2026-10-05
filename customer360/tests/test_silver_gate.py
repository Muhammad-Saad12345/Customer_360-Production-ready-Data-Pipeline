import json
import tempfile
import unittest
from pathlib import Path

from src.processing.silver_gate import HubSpotSilverProcessor


class HubSpotSilverGateTestCase(unittest.TestCase):
    def test_valid_record_passes_schema_and_dq(self):
        processor = HubSpotSilverProcessor(base_dir=Path(tempfile.gettempdir()) / "customer360_test_silver")
        raw = [{
            "id": "contact-001",
            "properties": {
                "firstname": "Saad",
                "lastname": "Khan",
                "email": "saad@example.com",
                "createdate": "2026-01-01T00:00:00Z",
                "lastmodifieddate": "2026-01-02T00:00:00Z",
                "lifecyclestage": "customer",
            },
        }]

        result = processor.process_records(
            source="hubspot",
            entity="contacts",
            run_id="run-valid-001",
            raw_records=raw,
        )

        self.assertEqual(result["input_record_count"], 1)
        self.assertEqual(result["valid_count"], 1)
        self.assertEqual(result["invalid_count"], 0)
        self.assertTrue(result["silver_path"].startswith("s3://"))
        self.assertTrue(result["silver_path"].endswith("data.parquet"))

    def test_missing_required_key_is_invalid(self):
        processor = HubSpotSilverProcessor(base_dir=Path(tempfile.gettempdir()) / "customer360_test_silver")
        raw = [{
            "properties": {
                "firstname": "Saad",
                "email": "saad@example.com",
            }
        }]

        result = processor.process_records(
            source="hubspot",
            entity="contacts",
            run_id="run-missing-key",
            raw_records=raw,
        )

        self.assertEqual(result["valid_count"], 0)
        self.assertEqual(result["invalid_count"], 1)
        self.assertEqual(result["quarantine_records"][0]["validation_rule"], "required_key")

    def test_invalid_email_is_invalid(self):
        processor = HubSpotSilverProcessor(base_dir=Path(tempfile.gettempdir()) / "customer360_test_silver")
        raw = [{
            "id": "contact-002",
            "properties": {
                "firstname": "Saad",
                "lastname": "Khan",
                "email": "not-an-email",
                "createdate": "2026-01-01T00:00:00Z",
                "lastmodifieddate": "2026-01-02T00:00:00Z",
            },
        }]

        result = processor.process_records(
            source="hubspot",
            entity="contacts",
            run_id="run-invalid-email",
            raw_records=raw,
        )

        self.assertEqual(result["invalid_count"], 1)
        self.assertEqual(result["quarantine_records"][0]["validation_rule"], "email_format")

    def test_duplicate_key_is_invalid(self):
        processor = HubSpotSilverProcessor(base_dir=Path(tempfile.gettempdir()) / "customer360_test_silver")
        raw = [
            {"id": "contact-dup", "properties": {"firstname": "A", "email": "a@example.com", "createdate": "2026-01-01T00:00:00Z", "lastmodifieddate": "2026-01-02T00:00:00Z"}},
            {"id": "contact-dup", "properties": {"firstname": "B", "email": "b@example.com", "createdate": "2026-01-01T00:00:00Z", "lastmodifieddate": "2026-01-02T00:00:00Z"}},
        ]

        result = processor.process_records(
            source="hubspot",
            entity="contacts",
            run_id="run-duplicate",
            raw_records=raw,
        )

        self.assertEqual(result["invalid_count"], 1)
        self.assertEqual(result["quarantine_records"][0]["validation_rule"], "duplicate_key")

    def test_malformed_timestamp_is_invalid(self):
        processor = HubSpotSilverProcessor(base_dir=Path(tempfile.gettempdir()) / "customer360_test_silver")
        raw = [{
            "id": "contact-003",
            "properties": {
                "firstname": "Saad",
                "lastname": "Khan",
                "email": "saad@example.com",
                "createdate": "not-a-date",
                "lastmodifieddate": "2026-01-02T00:00:00Z",
            },
        }]

        result = processor.process_records(
            source="hubspot",
            entity="contacts",
            run_id="run-bad-ts",
            raw_records=raw,
        )

        self.assertEqual(result["invalid_count"], 1)
        self.assertEqual(result["quarantine_records"][0]["validation_rule"], "timestamp_format")

    def test_schema_drift_is_invalid(self):
        processor = HubSpotSilverProcessor(base_dir=Path(tempfile.gettempdir()) / "customer360_test_silver")
        raw = [{
            "id": "contact-004",
            "not_properties": {
                "firstname": "Saad",
                "email": "saad@example.com",
                "createdate": "2026-01-01T00:00:00Z",
            },
        }]

        result = processor.process_records(
            source="hubspot",
            entity="contacts",
            run_id="run-schema-drift",
            raw_records=raw,
        )

        self.assertEqual(result["invalid_count"], 1)
        self.assertEqual(result["quarantine_records"][0]["validation_rule"], "schema")

    def test_quarantine_record_contains_required_metadata(self):
        processor = HubSpotSilverProcessor(base_dir=Path(tempfile.gettempdir()) / "customer360_test_silver")
        raw = [{
            "id": "contact-005",
            "properties": {
                "firstname": "Saad",
                "lastname": "Khan",
                "email": "bad-email",
                "createdate": "2026-01-01T00:00:00Z",
                "lastmodifieddate": "2026-01-02T00:00:00Z",
            },
        }]

        result = processor.process_records(
            source="hubspot",
            entity="contacts",
            run_id="run-quarantine",
            raw_records=raw,
        )

        self.assertEqual(result["quarantine_records"][0]["run_id"], "run-quarantine")
        self.assertEqual(result["quarantine_records"][0]["source"], "hubspot")
        self.assertEqual(result["quarantine_records"][0]["entity"], "contacts")
        self.assertIn("failure_reason", result["quarantine_records"][0])
        self.assertIn("ingestion_timestamp", result["quarantine_records"][0])

    def test_successful_valid_record_promotion_writes_parquet(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            processor = HubSpotSilverProcessor(base_dir=Path(tmpdir))
            raw = [{
                "id": "contact-006",
                "properties": {
                    "firstname": "Ali",
                    "lastname": "Raza",
                    "email": "ali@example.com",
                    "createdate": "2026-01-01T00:00:00Z",
                    "lastmodifieddate": "2026-01-02T00:00:00Z",
                    "lifecyclestage": "lead",
                },
            }]

            result = processor.process_records(
                source="hubspot",
                entity="contacts",
                run_id="run-promote",
                raw_records=raw,
            )

            self.assertTrue(result["silver_path"].startswith("s3://"))
            self.assertTrue(result["silver_path"].endswith("data.parquet"))
            self.assertIn("/silver/hubspot/contacts/", result["silver_path"])
            self.assertEqual(result["valid_count"], 1)

    def test_idempotent_reprocessing_does_not_duplicate_silver_records(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            processor = HubSpotSilverProcessor(base_dir=Path(tmpdir))
            raw = [{
                "id": "contact-007",
                "properties": {
                    "firstname": "Sara",
                    "lastname": "Ali",
                    "email": "sara@example.com",
                    "createdate": "2026-01-01T00:00:00Z",
                    "lastmodifieddate": "2026-01-02T00:00:00Z",
                    "lifecyclestage": "customer",
                },
            }]

            first = processor.process_records(source="hubspot", entity="contacts", run_id="run-idempotent", raw_records=raw)
            second = processor.process_records(source="hubspot", entity="contacts", run_id="run-idempotent", raw_records=raw)

            self.assertEqual(first["valid_count"], 1)
            self.assertEqual(second["valid_count"], 1)
            self.assertEqual(first["silver_path"], second["silver_path"])

    def test_oms_valid_record_without_properties_passes(self):
        processor = HubSpotSilverProcessor(base_dir=Path(tempfile.gettempdir()) / "customer360_test_silver")
        raw = [{
            "order_id": "ORD-1001",
            "customer_id": "C-1001",
            "order_ts": "2026-09-20T10:00:00Z",
            "order_status": "paid",
            "subtotal": 100.0,
            "tax": 10.0,
            "shipping": 5.0,
            "discount": 0.0,
            "net_total": 115.0,
            "currency": "USD",
            "channel": "web",
            "updated_at": "2026-09-20T10:05:00Z",
        }]

        result = processor.process_records(source="oms", entity="orders", run_id="oms-valid-001", raw_records=raw)

        self.assertEqual(result["valid_count"], 1)
        self.assertEqual(result["invalid_count"], 0)
        self.assertTrue(result["silver_path"].startswith("s3://"))
        self.assertEqual(result["valid_records"][0]["record_id"], "ORD-1001")

    def test_pim_valid_record_without_properties_passes(self):
        processor = HubSpotSilverProcessor(base_dir=Path(tempfile.gettempdir()) / "customer360_test_silver")
        raw = [{
            "product_id": "PIM-1001",
            "sku": "SKU-1001",
            "name": "Aero Smartwatch",
            "category": "Wearables",
            "brand": "Aero",
            "price": 299.0,
            "cost": 180.0,
            "attributes": {"color": "black"},
            "status": "active",
            "launch_date": "2026-09-20T00:00:00Z",
            "updated_at": "2026-09-20T10:05:00Z",
        }]

        result = processor.process_records(source="pim", entity="products", run_id="pim-valid-001", raw_records=raw)

        self.assertEqual(result["valid_count"], 1)
        self.assertEqual(result["invalid_count"], 0)
        self.assertEqual(result["valid_records"][0]["record_id"], "PIM-1001")

    def test_loyalty_valid_record_without_properties_passes(self):
        processor = HubSpotSilverProcessor(base_dir=Path(tempfile.gettempdir()) / "customer360_test_silver")
        raw = [{
            "loyalty_id": "LOY-1001",
            "customer_id": "C-1001",
            "tier": "gold",
            "points_balance": 3400,
            "earn_ts": "2026-09-20T09:00:00Z",
            "redeem_ts": "2026-09-20T09:30:00Z",
            "benefit_code": "VIP10",
            "updated_at": "2026-09-20T10:05:00Z",
        }]

        result = processor.process_records(source="loyalty", entity="loyalty", run_id="loyal-valid-001", raw_records=raw)

        self.assertEqual(result["valid_count"], 1)
        self.assertEqual(result["invalid_count"], 0)
        self.assertEqual(result["valid_records"][0]["record_id"], "LOY-1001")

    def test_mailchimp_valid_record_without_properties_passes(self):
        processor = HubSpotSilverProcessor(base_dir=Path(tempfile.gettempdir()) / "customer360_test_silver")
        raw = [{
            "campaign_id": "camp_001",
            "customer_id": "cust_100",
            "channel": "email",
            "send_ts": "2026-09-20T11:00:00Z",
            "open_ts": "2026-09-20T11:00:00Z",
            "click_ts": None,
            "audience_id": "aud_001",
            "offer_id": None,
            "member_id": "member_001",
            "email_address": "user@example.com",
            "status": "subscribed",
            "source_type": "mailchimp",
        }]

        result = processor.process_records(source="mailchimp", entity="campaign_events", run_id="mail-valid-001", raw_records=raw)

        self.assertEqual(result["valid_count"], 1)
        self.assertEqual(result["invalid_count"], 0)
        self.assertEqual(result["valid_records"][0]["record_id"], "member_001")

    def test_oms_invalid_enum_and_numeric_are_quarantined(self):
        processor = HubSpotSilverProcessor(base_dir=Path(tempfile.gettempdir()) / "customer360_test_silver")
        raw = [{
            "order_id": "ORD-INVALID-ENUM",
            "customer_id": "C-1001",
            "order_ts": "2026-09-20T10:00:00Z",
            "order_status": "unknown",
            "subtotal": -10.0,
            "tax": 10.0,
            "shipping": 5.0,
            "discount": 0.0,
            "net_total": 15.0,
            "currency": "USD",
            "channel": "web",
            "updated_at": "2026-09-20T10:05:00Z",
        }]

        result = processor.process_records(source="oms", entity="orders", run_id="oms-invalid-enum", raw_records=raw)

        self.assertEqual(result["valid_count"], 0)
        self.assertEqual(result["invalid_count"], 1)
        self.assertIn(result["quarantine_records"][0]["validation_rule"], {"enum", "numeric"})

    def test_repeated_processing_with_same_run_id_is_idempotent_for_non_hubspot(self):
        processor = HubSpotSilverProcessor(base_dir=Path(tempfile.gettempdir()) / "customer360_test_silver")
        raw = [{
            "loyalty_id": "LOY-1040",
            "customer_id": "C-1040",
            "tier": "silver",
            "points_balance": 1200,
            "earn_ts": "2026-09-20T09:00:00Z",
            "redeem_ts": "2026-09-20T09:30:00Z",
            "benefit_code": "STANDARD",
            "updated_at": "2026-09-20T10:05:00Z",
        }]

        first = processor.process_records(source="loyalty", entity="loyalty", run_id="loyal-idempotent", raw_records=raw)
        second = processor.process_records(source="loyalty", entity="loyalty", run_id="loyal-idempotent", raw_records=raw)

        self.assertEqual(first["valid_count"], 1)
        self.assertEqual(second["valid_count"], 1)
        self.assertEqual(first["silver_path"], second["silver_path"])


if __name__ == "__main__":
    unittest.main()
