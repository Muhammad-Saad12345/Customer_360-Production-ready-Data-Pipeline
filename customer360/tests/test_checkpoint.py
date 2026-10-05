import os
import tempfile
import unittest
from unittest.mock import patch

from src.checkpoint import CheckpointStore
from src.ingestion.base import BaseIngest
from src.ingestion.hubspot import HubSpotIngest
from src.ingestion.loyalty import LoyaltyIngest
from src.ingestion.marketing import MarketingIngest
from src.ingestion.support import ZendeskIngest
from src.orchestration.customer360_pipeline import Customer360PipelineRunner


class CheckpointStoreTestCase(unittest.TestCase):
    def test_checkpoint_round_trip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = CheckpointStore(base_dir=tmpdir)
            checkpoint = store.get_or_create(
                source="hubspot",
                entity="contacts",
                default_watermark="2024-01-01T00:00:00Z",
            )

            self.assertEqual(checkpoint["source"], "hubspot")
            self.assertEqual(checkpoint["entity"], "contacts")
            self.assertEqual(checkpoint["last_successful_watermark"], "2024-01-01T00:00:00Z")

            store.update(
                source="hubspot",
                entity="contacts",
                watermark="2024-01-02T00:00:00Z",
                run_id="run-123",
            )

            updated = store.get_or_create(source="hubspot", entity="contacts")
            self.assertEqual(updated["last_successful_watermark"], "2024-01-02T00:00:00Z")
            self.assertEqual(updated["last_successful_run_id"], "run-123")
            self.assertEqual(updated["status"], "success")

    def test_incremental_lookback_window_uses_latest_watermark(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = CheckpointStore(base_dir=tmpdir)
            store.update(
                source="hubspot",
                entity="contacts",
                watermark="2024-01-02T01:00:00Z",
                run_id="run-100",
            )

            ingest = BaseIngest.__new__(BaseIngest)
            ingest.source = "hubspot"
            ingest.object_name = "contacts"
            ingest.checkpoint_store = store

            watermark = ingest.resolve_watermark(
                default_watermark="2024-01-01T00:00:00Z",
                lookback_minutes=30,
                now_utc="2024-01-02T02:00:00Z",
            )

            self.assertEqual(watermark, "2024-01-02T01:00:00Z")

            window_start = ingest.compute_window_start(
                watermark=watermark,
                lookback_minutes=30,
                now_utc="2024-01-02T02:00:00Z",
            )
            self.assertEqual(window_start, "2024-01-02T00:30:00Z")

    def test_synthetic_source_connectors_return_records(self):
        marketing = MarketingIngest()
        marketing.provider = "synthetic"
        marketing.settings = type("Settings", (), {
            "MARKETING_PROVIDER": "synthetic",
            "MARKETING_API_KEY": None,
            "MAILCHIMP_API_KEY": None,
            "MAILCHIMP_SERVER_PREFIX": "",
            "MAILCHIMP_AUDIENCE_ID": "",
        })()
        marketing_records = marketing.fetch_records("1970-01-01T00:00:00Z")
        self.assertTrue(marketing_records)
        self.assertEqual(marketing_records[0]["source_type"], "synthetic")

        support = ZendeskIngest()
        support_records = support.fetch_records("1970-01-01T00:00:00Z")
        self.assertTrue(support_records)
        self.assertEqual(support_records[0]["source_type"], "synthetic")

        loyalty = LoyaltyIngest()
        loyalty_records = loyalty.fetch_records("1970-01-01T00:00:00Z")
        self.assertTrue(loyalty_records)
        self.assertEqual(loyalty_records[0]["source_type"], "synthetic")

    def test_mailchimp_fetch_uses_real_api_when_configured(self):
        marketing = MarketingIngest()
        marketing.provider = "mailchimp"
        marketing.settings = type("Settings", (), {
            "MARKETING_PROVIDER": "mailchimp",
            "MARKETING_API_KEY": "mailchimp-api-key",
            "MAILCHIMP_API_KEY": "mailchimp-api-key",
            "MAILCHIMP_SERVER_PREFIX": "us18",
            "MAILCHIMP_AUDIENCE_ID": "abc123",
        })()

        class FakeResponse:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "members": [{
                        "id": "member-1",
                        "email_address": "person@example.com",
                        "status": "subscribed",
                        "timestamp_opt": "2024-01-02T00:00:00Z",
                        "last_changed": "2024-01-02T00:05:00Z",
                        "merge_fields": {"customer_id": "cust-1"},
                    }],
                    "total_items": 1,
                }

        with patch("src.ingestion.marketing.requests.get", return_value=FakeResponse()) as mocked_get:
            records = marketing.fetch_records("1970-01-01T00:00:00Z")

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["source_type"], "mailchimp")
        self.assertEqual(records[0]["audience_id"], "abc123")
        self.assertEqual(records[0]["customer_id"], "cust-1")
        mocked_get.assert_called_once()

    def test_pipeline_runner_executes_hubspot_initial_load_only(self):
        runner = Customer360PipelineRunner()
        tasks = runner._tasks()
        self.assertEqual(len(tasks), 1)
        self.assertEqual([task_name for task_name, _ in tasks], [
            "hubspot_contacts",
        ])
        # HubSpot is CONTACTS ONLY. Orders belong to OMS; Products belong to PIM.
        runner.run_hourly()

    def test_hubspot_cannot_ingest_orders_or_products(self):
        """Regression test: prove HubSpot connector cannot extract orders or products."""
        ingest = HubSpotIngest()
        self.assertEqual(ingest.source, "hubspot")
        self.assertEqual(ingest.object_name, "contacts")
        self.assertFalse(hasattr(ingest, "run_orders"), "HubSpot must NOT have run_orders()")
        self.assertFalse(hasattr(ingest, "run_products"), "HubSpot must NOT have run_products()")
        self.assertFalse(hasattr(ingest, "_synthetic_order_records"), "HubSpot must NOT have synthetic order generator")
        self.assertFalse(hasattr(ingest, "_synthetic_product_records"), "HubSpot must NOT have synthetic product generator")

    def test_watermark_boundary_and_late_updates_are_handled_by_window_logic(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = CheckpointStore(base_dir=tmpdir)
            store.update(
                source="hubspot",
                entity="contacts",
                watermark="2024-01-02T01:00:00Z",
                run_id="run-200",
            )
            ingest = HubSpotIngest()
            ingest.checkpoint_store = store
            record_on_boundary = {"id": "c-1", "properties": {"lastmodifieddate": "2024-01-02T01:30:00Z"}}
            record_before_watermark = {"id": "c-2", "properties": {"lastmodifieddate": "2024-01-02T00:59:00Z"}}
            record_late_update = {"id": "c-3", "properties": {"lastmodifieddate": "2024-01-01T12:00:00Z"}}

            filtered = ingest._filter_records_since(
                [record_on_boundary, record_before_watermark, record_late_update],
                field_name="lastmodifieddate",
                watermark="2024-01-02T01:00:00Z",
                lookback_minutes=30,
                now_utc="2024-01-02T02:00:00Z",
            )
            self.assertEqual([item["id"] for item in filtered], ["c-1", "c-2"])

    def test_empty_response_keeps_checkpoint_unchanged(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = CheckpointStore(base_dir=tmpdir)
            store.update(
                source="hubspot",
                entity="contacts",
                watermark="2024-01-02T01:00:00Z",
                run_id="run-300",
            )
            ingest = HubSpotIngest()
            ingest.checkpoint_store = store
            filtered = ingest._filter_records_since([], watermark="2024-01-02T01:00:00Z", lookback_minutes=30, now_utc="2024-01-02T02:00:00Z")
            self.assertEqual(filtered, [])
            checkpoint = store.get_or_create(source="hubspot", entity="contacts")
            self.assertEqual(checkpoint["last_successful_watermark"], "2024-01-02T01:00:00Z")

    def test_duplicate_records_are_deduplicated_before_s3_write(self):
        ingest = HubSpotIngest()
        duplicates = [
            {"id": "dup-1", "properties": {"lastmodifieddate": "2024-01-02T01:50:00Z", "email": "one@example.com"}},
            {"id": "dup-1", "properties": {"lastmodifieddate": "2024-01-02T02:00:00Z", "email": "two@example.com"}},
        ]
        deduped = ingest._deduplicate_records(duplicates)
        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0]["properties"]["email"], "two@example.com")

    def test_pagination_aggregates_rows_across_pages(self):
        ingest = HubSpotIngest()
        calls = []

        def fake_fetch_page(url, params):
            calls.append({"url": url, "params": params.copy()})
            if url.endswith("/crm/v3/properties/contacts"):
                return {"results": [{"name": "id"}]}
            if len([c for c in calls if c["url"].endswith("/crm/v3/objects/contacts")]) == 1:
                return {"results": [{"id": "a", "properties": {"lastmodifieddate": "2024-01-02T01:40:00Z"}}, {"id": "b", "properties": {"lastmodifieddate": "2024-01-02T01:50:00Z"}}], "paging": {"next": {"after": "page-2"}}}
            return {"results": [{"id": "c", "properties": {"lastmodifieddate": "2024-01-02T02:00:00Z"}}], "paging": None}

        ingest._fetch_page = fake_fetch_page  # type: ignore[assignment]
        records, selected = ingest._fetch_objects("contacts", {"customer_id": ["id"]})
        self.assertEqual(len(records), 3)
        self.assertEqual([item["id"] for item in records], ["a", "b", "c"])

    def test_failed_s3_write_returns_empty_path_without_raising(self):
        ingest = HubSpotIngest()
        with patch.object(ingest.s3, "upload_jsonlines", side_effect=RuntimeError("AccessDenied")):
            result = ingest.persist_raw([{"id": "x"}])
            self.assertEqual(result, "")

    def test_first_ever_extraction_uses_default_watermark_as_boundary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ingest = HubSpotIngest()
            ingest.checkpoint_store = CheckpointStore(base_dir=tmpdir)
            start = ingest.compute_window_start("1970-01-01T00:00:00Z", lookback_minutes=60, now_utc="2024-01-02T02:00:00Z")
            self.assertEqual(start, "1969-12-31T23:00:00Z")

    def test_existing_checkpoint_applies_lookback_to_watermark(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = CheckpointStore(base_dir=tmpdir)
            store.update(source="hubspot", entity="contacts", watermark="2024-01-02T01:00:00Z", run_id="run-checkpoint")
            ingest = HubSpotIngest()
            ingest.checkpoint_store = store
            start = ingest.compute_window_start("2024-01-02T01:00:00Z", lookback_minutes=30, now_utc="2024-01-02T02:00:00Z")
            self.assertEqual(start, "2024-01-02T00:30:00Z")

    def test_zero_records_does_not_advance_checkpoint(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = CheckpointStore(base_dir=tmpdir)
            store.update(source="hubspot", entity="contacts", watermark="2024-01-02T01:00:00Z", run_id="run-zero")
            ingest = HubSpotIngest()
            ingest.checkpoint_store = store
            self.assertEqual(ingest._filter_records_since([], watermark="2024-01-02T01:00:00Z", lookback_minutes=30, now_utc="2024-01-02T02:00:00Z"), [])
            checkpoint = store.get_or_create(source="hubspot", entity="contacts")
            self.assertEqual(checkpoint["last_successful_watermark"], "2024-01-02T01:00:00Z")

    def test_record_inside_lookback_is_included(self):
        ingest = HubSpotIngest()
        record = {"id": "inside", "properties": {"lastmodifieddate": "2024-01-02T00:45:00Z"}}
        filtered = ingest._filter_records_since([record], watermark="2024-01-02T01:00:00Z", lookback_minutes=30, now_utc="2024-01-02T02:00:00Z")
        self.assertEqual([item["id"] for item in filtered], ["inside"])

    def test_record_outside_lookback_is_excluded(self):
        ingest = HubSpotIngest()
        record = {"id": "outside", "properties": {"lastmodifieddate": "2024-01-01T23:59:00Z"}}
        filtered = ingest._filter_records_since([record], watermark="2024-01-02T01:00:00Z", lookback_minutes=30, now_utc="2024-01-02T02:00:00Z")
        self.assertEqual(filtered, [])

    def test_pipeline_not_running_for_hours_uses_watermark_minus_lookback(self):
        ingest = HubSpotIngest()
        start = ingest.compute_window_start("2024-01-10T00:00:00Z", lookback_minutes=60, now_utc="2024-01-10T05:00:00Z")
        self.assertEqual(start, "2024-01-09T23:00:00Z")

    def test_late_update_inside_lookback_is_detected(self):
        ingest = HubSpotIngest()
        records = [
            {"id": "late", "properties": {"lastmodifieddate": "2024-01-02T00:50:00Z"}},
            {"id": "late", "properties": {"lastmodifieddate": "2024-01-02T00:55:00Z"}},
        ]
        filtered = ingest._filter_records_since(records, watermark="2024-01-02T01:00:00Z", lookback_minutes=30, now_utc="2024-01-02T02:00:00Z")
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["properties"]["lastmodifieddate"], "2024-01-02T00:55:00Z")

    def test_duplicate_record_is_deduplicated_before_checkpoint(self):
        ingest = HubSpotIngest()
        records = [
            {"id": "dup", "properties": {"lastmodifieddate": "2024-01-02T00:50:00Z", "email": "old@example.com"}},
            {"id": "dup", "properties": {"lastmodifieddate": "2024-01-02T00:55:00Z", "email": "new@example.com"}},
        ]
        deduped = ingest._deduplicate_records(records)
        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0]["properties"]["email"], "new@example.com")

    def test_failed_s3_write_does_not_advance_checkpoint(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = CheckpointStore(base_dir=tmpdir)
            store.update(source="hubspot", entity="contacts", watermark="2024-01-02T01:00:00Z", run_id="run-before")
            ingest = HubSpotIngest()
            ingest.checkpoint_store = store
            ingest.object_name = "contacts"
            ingest.s3 = type("DummyS3", (), {"make_raw_key": lambda self, *args, **kwargs: "raw/test/data.json", "upload_jsonlines": lambda self, key, items: (_ for _ in ()).throw(RuntimeError("AccessDenied"))})()
            result = ingest.persist_raw([{"id": "x"}])
            self.assertEqual(result, "")
            checkpoint = store.get_or_create(source="hubspot", entity="contacts")
            self.assertEqual(checkpoint["last_successful_watermark"], "2024-01-02T01:00:00Z")

    def test_successful_s3_write_advances_checkpoint(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = CheckpointStore(base_dir=tmpdir)
            store.update(source="hubspot", entity="contacts", watermark="2024-01-02T01:00:00Z", run_id="run-before")
            ingest = HubSpotIngest()
            ingest.checkpoint_store = store
            ingest.object_name = "contacts"
            ingest.s3 = type("DummyS3", (), {"make_raw_key": lambda self, *args, **kwargs: "raw/test/data.json", "upload_jsonlines": lambda self, key, items: None})()
            checkpoint = ingest.update_checkpoint("2024-01-02T02:00:00Z", run_id="run-after")
            self.assertEqual(checkpoint["last_successful_watermark"], "2024-01-02T02:00:00Z")


if __name__ == "__main__":
    unittest.main()
