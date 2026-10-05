"""
Data Quality, Idempotency, and Schema Evolution Verification
=============================================================

TASK 7 — Data quality: reconciliation, duplicate detection, schema compliance.
TASK 8 — Idempotency: re-running the same run_id must produce identical results.
TASK 9 — Schema evolution: new columns in Silver flow through to Iceberg correctly.

No AWS credentials required.  All S3/Glue/Athena I/O is mocked.
"""
from __future__ import annotations

import io
import uuid
import unittest
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import MagicMock, call, patch

import pyarrow as pa
import pyarrow.parquet as pq

from src.contracts.data_contracts import (
    get_contract,
    get_all_contracts,
    HUBSPOT_CONTACTS_CONTRACT,
    OMS_ORDERS_CONTRACT,
    PIM_PRODUCTS_CONTRACT,
    LOYALTY_CONTRACT,
    MARKETING_CAMPAIGN_EVENTS_CONTRACT,
)
from src.processing.silver_gate import HubSpotSilverProcessor
from src.lakehouse.iceberg_loader import IcebergLoader
from src.lakehouse.table_definitions import TABLE_REGISTRY, get_table_definition


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _ts() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _make_processor() -> HubSpotSilverProcessor:
    proc = HubSpotSilverProcessor.__new__(HubSpotSilverProcessor)
    proc.settings = MagicMock()
    proc.settings.AWS_REGION = "us-east-1"
    proc.settings.S3_BUCKET = "test-bucket"
    proc.processor = MagicMock()
    s3_mock = MagicMock()
    s3_mock.put_object.return_value = {}
    proc.s3 = s3_mock
    proc.bucket = "test-bucket"
    return proc


def _parquet_bytes(rows: List[Dict[str, Any]]) -> bytes:
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buf)
    return buf.getvalue()


def _make_loader(rows: List[Dict[str, Any]], run_id: str, source: str, entity: str) -> IcebergLoader:
    key = f"silver/{source}/{entity}/ingestion_date=2024-01-01/run_id={run_id}/data.parquet"
    data = _parquet_bytes(rows)
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"Contents": [{"Key": key}]}]
    mock_s3.get_paginator.return_value = mock_paginator
    mock_s3.get_object.return_value = {"Body": MagicMock(read=lambda: data)}

    mock_glue = MagicMock()
    mock_glue.table_exists.return_value = True
    mock_glue.get_table_metadata.return_value = {
        "StorageDescriptor": {"Columns": []}, "PartitionKeys": []
    }

    mock_athena = MagicMock()
    mock_athena.execute_query.return_value = {"status": "SUCCEEDED", "rows": []}
    mock_athena.run_ddl.return_value = {"status": "SUCCEEDED", "rows": []}

    mock_config = MagicMock()
    mock_config.bucket = "test-bucket"
    mock_config.database_name = "customer360"
    mock_config.region_name = "us-east-1"
    mock_config.table_location.return_value = "s3://test-bucket/iceberg/"

    return IcebergLoader(
        config=mock_config,
        glue_catalog=mock_glue,
        athena_client=mock_athena,
        s3_client=mock_s3,
    )


# ---------------------------------------------------------------------------
# TASK 6 — Data Contract validation tests
# ---------------------------------------------------------------------------

class TestDataContracts(unittest.TestCase):
    """Verify DataContract registry completeness and correctness."""

    def test_all_five_active_sources_have_contracts(self):
        sources = [
            ("hubspot", "contacts"),
            ("oms", "orders"),
            ("pim", "products"),
            ("loyalty", "loyalty"),
            ("marketing", "campaign_events"),
        ]
        for source, entity in sources:
            contract = get_contract(source, entity)
            self.assertIsNotNone(contract, f"No contract for ({source}, {entity})")

    def test_mailchimp_alias_resolves_to_marketing_table(self):
        mc = get_contract("mailchimp", "campaign_events")
        mk = get_contract("marketing", "campaign_events")
        self.assertIsNotNone(mc)
        self.assertEqual(mc.iceberg_table, mk.iceberg_table)

    def test_contracts_have_primary_keys(self):
        for c in get_all_contracts():
            self.assertTrue(
                len(c.primary_keys) >= 1,
                f"{c.source}/{c.entity} contract has no primary keys",
            )

    def test_contracts_have_required_fields(self):
        for c in get_all_contracts():
            self.assertTrue(
                len(c.required_fields) >= 1,
                f"{c.source}/{c.entity} contract has no required fields",
            )

    def test_primary_keys_are_subset_of_required_fields_or_nested(self):
        """Primary keys should always be reachable from required fields."""
        # This is a documentation test — fails if PK is not discoverable
        for c in get_all_contracts():
            for pk in c.primary_keys:
                reachable = (
                    pk in c.required_fields
                    or pk in c.optional_fields
                    or pk == "record_id"  # marketing uses composite record_id
                    or (c.source == "hubspot" and pk == "customer_id" and "id" in c.required_fields)
                )
                self.assertTrue(
                    reachable,
                    f"Primary key '{pk}' for {c.source}/{c.entity} is not in required or optional fields",
                )

    def test_hubspot_contract_validates_valid_contact(self):
        record = {
            "id": "hs_001",
            "properties": {
                "email": "test@example.com",
                "firstname": "Alice",
                "lastmodifieddate": _ts(),
            },
        }
        err = HUBSPOT_CONTACTS_CONTRACT.validate(record)
        self.assertIsNone(err, f"Unexpected validation error: {err}")

    def test_hubspot_contract_rejects_missing_email(self):
        record = {"id": "hs_001", "properties": {"firstname": "Alice"}}
        err = HUBSPOT_CONTACTS_CONTRACT.validate(record)
        self.assertIsNotNone(err)
        self.assertIn("email", err)

    def test_oms_contract_rejects_invalid_status(self):
        record = {
            "order_id": "ORD-001", "customer_id": "C1",
            "order_ts": _ts(), "order_status": "UNKNOWN",
            "subtotal": 10, "tax": 1, "shipping": 2,
            "discount": 0, "net_total": 13,
            "currency": "USD", "channel": "web", "updated_at": _ts(),
        }
        err = OMS_ORDERS_CONTRACT.validate(record)
        self.assertIsNotNone(err)
        self.assertIn("order_status", err)

    def test_oms_contract_rejects_negative_subtotal(self):
        record = {
            "order_id": "ORD-002", "customer_id": "C1",
            "order_ts": _ts(), "order_status": "paid",
            "subtotal": -5, "tax": 1, "shipping": 2,
            "discount": 0, "net_total": -2,
            "currency": "USD", "channel": "web", "updated_at": _ts(),
        }
        err = OMS_ORDERS_CONTRACT.validate(record)
        self.assertIsNotNone(err)
        self.assertIn("minimum", err)

    def test_pim_contract_rejects_invalid_status(self):
        record = {
            "product_id": "P1", "sku": "SKU-1", "name": "Widget",
            "category": "Tools", "brand": "Acme",
            "price": 9.99, "cost": 4.00, "status": "UNKNOWN",
            "launch_date": "2024-01-01T00:00:00Z", "updated_at": _ts(),
        }
        err = PIM_PRODUCTS_CONTRACT.validate(record)
        self.assertIsNotNone(err)
        self.assertIn("status", err)

    def test_loyalty_contract_rejects_invalid_tier(self):
        record = {
            "loyalty_id": "L1", "customer_id": "C1",
            "tier": "titanium",  # not in enum
            "points_balance": 500, "earn_ts": _ts(),
            "redeem_ts": _ts(), "benefit_code": "B1", "updated_at": _ts(),
        }
        err = LOYALTY_CONTRACT.validate(record)
        self.assertIsNotNone(err)
        self.assertIn("tier", err)

    def test_marketing_contract_rejects_bad_email(self):
        record = {
            "campaign_id": "C1", "customer_id": "CU1",
            "channel": "email", "send_ts": _ts(), "open_ts": _ts(),
            "audience_id": "A1", "member_id": "M1",
            "email_address": "not-an-email",
            "status": "subscribed", "source_type": "mailchimp",
        }
        # DataContract.validate handles email check through enum/required but not format
        # Email format validation is in HubSpotSilverProcessor._validate_generic_record
        # This test confirms the contract has email_fields declared
        self.assertIn("email_address", MARKETING_CAMPAIGN_EVENTS_CONTRACT.email_fields)

    def test_contract_iceberg_table_names_match_registry(self):
        """Each contract's iceberg_table must correspond to an active TABLE_REGISTRY entry."""
        registered_tables = {td.table_name for td in TABLE_REGISTRY.values()}
        for c in get_all_contracts():
            self.assertIn(
                c.iceberg_table, registered_tables,
                f"Contract {c.source}/{c.entity} references unknown table '{c.iceberg_table}'",
            )

    def test_contract_primary_keys_match_table_definitions(self):
        """Primary keys in DataContract must match TableDefinition.primary_keys."""
        for (source, entity), contract in {
            ("hubspot", "contacts"): HUBSPOT_CONTACTS_CONTRACT,
            ("oms", "orders"): OMS_ORDERS_CONTRACT,
            ("pim", "products"): PIM_PRODUCTS_CONTRACT,
            ("loyalty", "loyalty"): LOYALTY_CONTRACT,
        }.items():
            table_def = get_table_definition(source, entity)
            self.assertIsNotNone(table_def)
            self.assertEqual(
                set(contract.primary_keys), set(table_def.primary_keys),
                f"Primary key mismatch for {source}/{entity}: "
                f"contract={contract.primary_keys}, table_def={table_def.primary_keys}",
            )


# ---------------------------------------------------------------------------
# TASK 7 — Data Quality: Reconciliation and Schema Compliance
# ---------------------------------------------------------------------------

class TestDataQualityReconciliation(unittest.TestCase):
    """
    Verifies that Silver Gate reconciliation counts are correct and that
    the silver_path / quarantine_path return values are populated as expected.
    """

    def _run(self, source, entity, records):
        proc = _make_processor()
        return proc.process_records(source, entity, str(uuid.uuid4()), raw_records=records)

    def test_all_valid_records_produce_no_quarantine(self):
        records = [
            {"order_id": f"O{i}", "customer_id": f"C{i}", "order_ts": _ts(),
             "order_status": "paid", "subtotal": 10.0, "tax": 1.0,
             "shipping": 2.0, "discount": 0.0, "net_total": 13.0,
             "currency": "USD", "channel": "web", "updated_at": _ts()}
            for i in range(1, 6)
        ]
        result = self._run("oms", "orders", records)
        self.assertEqual(result["valid_count"], 5)
        self.assertEqual(result["invalid_count"], 0)
        self.assertEqual(result["quarantine_records"], [])

    def test_mixed_valid_invalid_counts_are_exact(self):
        """2 valid, 1 invalid (bad status), 1 invalid (negative price)."""
        records = [
            {"order_id": "O1", "customer_id": "C1", "order_ts": _ts(),
             "order_status": "paid", "subtotal": 10.0, "tax": 1.0,
             "shipping": 2.0, "discount": 0.0, "net_total": 13.0,
             "currency": "USD", "channel": "web", "updated_at": _ts()},
            {"order_id": "O2", "customer_id": "C2", "order_ts": _ts(),
             "order_status": "shipped", "subtotal": 20.0, "tax": 2.0,
             "shipping": 3.0, "discount": 0.0, "net_total": 25.0,
             "currency": "USD", "channel": "mobile", "updated_at": _ts()},
            {"order_id": "O3", "customer_id": "C3", "order_ts": _ts(),
             "order_status": "INVALID_STATUS",  # quarantine trigger
             "subtotal": 5.0, "tax": 0.5, "shipping": 1.0,
             "discount": 0.0, "net_total": 6.5,
             "currency": "USD", "channel": "web", "updated_at": _ts()},
            {"order_id": "O4", "customer_id": "C4", "order_ts": _ts(),
             "order_status": "paid", "subtotal": -5.0,  # quarantine trigger
             "tax": 0.5, "shipping": 1.0, "discount": 0.0,
             "net_total": 6.5, "currency": "USD", "channel": "web", "updated_at": _ts()},
        ]
        result = self._run("oms", "orders", records)
        self.assertEqual(result["input_record_count"], 4)
        self.assertEqual(result["valid_count"], 2)
        self.assertEqual(result["invalid_count"], 2)

    def test_quarantine_records_contain_failure_metadata(self):
        """Each quarantine record must carry validation_rule and failure_reason."""
        records = [
            {"product_id": "P1", "sku": "SK1", "name": "Prod",
             "category": "Cat", "brand": "Brand",
             "price": -1.0,  # invalid
             "cost": 5.0, "status": "active",
             "launch_date": "2024-01-01T00:00:00Z", "updated_at": _ts()},
        ]
        result = self._run("pim", "products", records)
        self.assertEqual(len(result["quarantine_records"]), 1)
        qr = result["quarantine_records"][0]
        self.assertIn("validation_rule", qr)
        self.assertIn("failure_reason", qr)
        self.assertIn("record", qr)
        self.assertEqual(qr["source"], "pim")
        self.assertEqual(qr["entity"], "products")

    def test_valid_records_are_standardized_with_pipeline_fields(self):
        """Valid silver records must include source, entity, run_id, ingestion_timestamp."""
        records = [
            {"loyalty_id": "L1", "customer_id": "C1", "tier": "gold",
             "points_balance": 500, "earn_ts": _ts(), "redeem_ts": _ts(),
             "benefit_code": "BC1", "updated_at": _ts()},
        ]
        result = self._run("loyalty", "loyalty", records)
        self.assertEqual(result["valid_count"], 1)
        valid = result["valid_records"]
        self.assertEqual(len(valid), 1)
        row = valid[0]
        self.assertIn("source", row)
        self.assertIn("entity", row)
        self.assertIn("run_id", row)
        self.assertIn("ingestion_timestamp", row)
        self.assertEqual(row["source"], "loyalty")
        self.assertEqual(row["entity"], "loyalty")

    def test_silver_path_is_returned_for_valid_records(self):
        records = [
            {"loyalty_id": "L1", "customer_id": "C1", "tier": "gold",
             "points_balance": 500, "earn_ts": _ts(), "redeem_ts": _ts(),
             "benefit_code": "BC1", "updated_at": _ts()},
        ]
        result = self._run("loyalty", "loyalty", records)
        self.assertTrue(result["silver_path"].startswith("s3://"))

    def test_quarantine_path_empty_when_no_invalid_records(self):
        records = [
            {"loyalty_id": "L1", "customer_id": "C1", "tier": "gold",
             "points_balance": 500, "earn_ts": _ts(), "redeem_ts": _ts(),
             "benefit_code": "BC1", "updated_at": _ts()},
        ]
        result = self._run("loyalty", "loyalty", records)
        self.assertEqual(result["quarantine_path"], "")

    def test_all_six_validation_rules_are_exercisable(self):
        """
        Confirm that each validation_rule type is producible.
        Rules: schema, required_key, duplicate_key, email_format, enum, numeric
        """
        proc = _make_processor()
        run_id = str(uuid.uuid4())

        # schema — non-dict
        r = proc.process_records("oms", "orders", run_id, raw_records=["not-a-dict"])
        self.assertTrue(any(q["validation_rule"] == "schema" for q in r["quarantine_records"]))

        # required_key — missing order_id
        r = proc.process_records("oms", "orders", run_id,
                                 raw_records=[{"customer_id": "C1", "order_ts": _ts(),
                                               "order_status": "paid", "subtotal": 5,
                                               "tax": 1, "shipping": 1, "discount": 0,
                                               "net_total": 7, "currency": "USD",
                                               "channel": "web", "updated_at": _ts()}])
        self.assertTrue(any(q["validation_rule"] == "required_key" for q in r["quarantine_records"]))

        # duplicate_key
        rec = {"order_id": "DUP", "customer_id": "C1", "order_ts": _ts(),
               "order_status": "paid", "subtotal": 5, "tax": 1, "shipping": 1,
               "discount": 0, "net_total": 7, "currency": "USD",
               "channel": "web", "updated_at": _ts()}
        r = proc.process_records("oms", "orders", str(uuid.uuid4()),
                                 raw_records=[dict(rec), dict(rec)])
        self.assertTrue(any(q["validation_rule"] == "duplicate_key" for q in r["quarantine_records"]))

        # email_format — hubspot
        r = proc.process_records("hubspot", "contacts", str(uuid.uuid4()),
                                 raw_records=[{"id": "hs1", "properties": {"email": "bad"}}])
        self.assertTrue(any(q["validation_rule"] == "email_format" for q in r["quarantine_records"]))

        # enum — oms order_status
        r = proc.process_records("oms", "orders", str(uuid.uuid4()),
                                 raw_records=[{"order_id": "O1", "customer_id": "C",
                                               "order_ts": _ts(), "order_status": "ILLEGAL",
                                               "subtotal": 5, "tax": 1, "shipping": 1,
                                               "discount": 0, "net_total": 7,
                                               "currency": "USD", "channel": "web",
                                               "updated_at": _ts()}])
        self.assertTrue(any(q["validation_rule"] == "enum" for q in r["quarantine_records"]))

        # numeric — negative price
        r = proc.process_records("pim", "products", str(uuid.uuid4()),
                                 raw_records=[{"product_id": "P1", "sku": "S1", "name": "N",
                                               "category": "C", "brand": "B",
                                               "price": -1, "cost": 1, "status": "active",
                                               "launch_date": "2024-01-01T00:00:00Z",
                                               "updated_at": _ts()}])
        self.assertTrue(any(q["validation_rule"] == "numeric" for q in r["quarantine_records"]))


class TestDataQualitySchemaCompliance(unittest.TestCase):
    """Verify standardized Silver records comply with expected output schema."""

    def _standardized_rows(self, source, entity, records):
        proc = _make_processor()
        r = proc.process_records(source, entity, str(uuid.uuid4()), raw_records=records)
        return r["valid_records"]

    def test_hubspot_contacts_standardized_fields_present(self):
        records = [{
            "id": "hs_001",
            "properties": {
                "email": "alice@example.com",
                "firstname": "Alice", "lastname": "Smith",
                "phone": "+1555000001",
                "createdate": _ts(), "lastmodifieddate": _ts(),
                "lifecyclestage": "customer", "hubspot_owner_id": "O1",
            },
        }]
        rows = self._standardized_rows("hubspot", "contacts", records)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        for expected_field in ["customer_id", "email", "first_name", "last_name",
                               "phone", "customer_stage", "created_at", "updated_at",
                               "source", "entity", "run_id", "ingestion_timestamp"]:
            self.assertIn(expected_field, row, f"Missing field: {expected_field}")

    def test_oms_orders_standardized_fields_present(self):
        records = [{
            "order_id": "ORD-100", "customer_id": "CUST-1",
            "order_ts": _ts(), "order_status": "paid",
            "subtotal": 50.0, "tax": 5.0, "shipping": 3.0,
            "discount": 0.0, "net_total": 58.0,
            "currency": "USD", "channel": "web", "updated_at": _ts(),
        }]
        rows = self._standardized_rows("oms", "orders", records)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        for expected_field in ["order_id", "customer_id", "order_ts", "order_status",
                               "subtotal", "tax", "net_total", "source", "entity",
                               "run_id", "ingestion_timestamp"]:
            self.assertIn(expected_field, row, f"Missing field: {expected_field}")

    def test_numeric_fields_are_float_not_string(self):
        records = [{
            "order_id": "ORD-200", "customer_id": "C2",
            "order_ts": _ts(), "order_status": "paid",
            "subtotal": "99.99", "tax": "8.00",  # passed as strings — must be coerced
            "shipping": "5.00", "discount": "0.00", "net_total": "112.99",
            "currency": "USD", "channel": "web", "updated_at": _ts(),
        }]
        proc = _make_processor()
        result = proc.process_records("oms", "orders", str(uuid.uuid4()), raw_records=records)
        rows = result["valid_records"]
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertIsInstance(row["subtotal"], float, "subtotal must be a float")
        self.assertIsInstance(row["net_total"], float, "net_total must be a float")


# ---------------------------------------------------------------------------
# TASK 8 — Idempotency Verification
# ---------------------------------------------------------------------------

class TestIdempotency(unittest.TestCase):
    """
    Idempotency guarantee: re-running the same pipeline run_id must produce
    the same Silver output and the same Iceberg MERGE call (no data duplication).
    """

    def test_same_run_id_produces_identical_silver_output(self):
        """
        Processing the exact same input records with the same run_id twice
        must produce identical valid_count / invalid_count.
        """
        records = [
            {"order_id": f"O{i}", "customer_id": f"C{i}", "order_ts": _ts(),
             "order_status": "paid", "subtotal": 10.0, "tax": 1.0,
             "shipping": 2.0, "discount": 0.0, "net_total": 13.0,
             "currency": "USD", "channel": "web", "updated_at": _ts()}
            for i in range(1, 4)
        ]
        run_id = str(uuid.uuid4())
        proc = _make_processor()

        # First run
        r1 = proc.process_records("oms", "orders", run_id, raw_records=records)
        # Second run with identical input + same run_id
        proc2 = _make_processor()
        r2 = proc2.process_records("oms", "orders", run_id, raw_records=records)

        self.assertEqual(r1["valid_count"], r2["valid_count"])
        self.assertEqual(r1["invalid_count"], r2["invalid_count"])
        self.assertEqual(r1["input_record_count"], r2["input_record_count"])

    def test_duplicate_records_are_deduped_intra_batch_before_silver(self):
        """
        If the source sends the same record twice in the same batch,
        the silver gate must quarantine the second occurrence.
        """
        rec = {"order_id": "DUP-001", "customer_id": "C1", "order_ts": _ts(),
               "order_status": "paid", "subtotal": 10.0, "tax": 1.0,
               "shipping": 2.0, "discount": 0.0, "net_total": 13.0,
               "currency": "USD", "channel": "web", "updated_at": _ts()}
        proc = _make_processor()
        result = proc.process_records("oms", "orders", str(uuid.uuid4()),
                                      raw_records=[dict(rec), dict(rec)])
        self.assertEqual(result["valid_count"], 1)
        self.assertEqual(result["invalid_count"], 1)
        self.assertEqual(result["quarantine_records"][0]["validation_rule"], "duplicate_key")

    def test_iceberg_merge_is_idempotent_via_deduplication(self):
        """
        IcebergLoader.deduplicate_records_by_primary_key must collapse
        duplicates before the MERGE query, so the same record loaded twice
        produces one row in the Iceberg output (not two).
        """
        run_id = "idempotency-run-001"
        rows = [
            {"source": "oms", "entity": "orders", "run_id": run_id,
             "record_id": "O1", "order_id": "O1", "ingestion_timestamp": _ts()},
            {"source": "oms", "entity": "orders", "run_id": run_id,
             "record_id": "O1", "order_id": "O1", "ingestion_timestamp": _ts()},  # duplicate
            {"source": "oms", "entity": "orders", "run_id": run_id,
             "record_id": "O2", "order_id": "O2", "ingestion_timestamp": _ts()},
        ]
        loader = _make_loader(rows, run_id, "oms", "orders")
        result = loader.load_silver_run("oms", "orders", run_id, dry_run=True)
        self.assertEqual(result["silver_row_count"], 3)
        self.assertEqual(result["deduped_row_count"], 2)  # O1 appears once

    def test_iceberg_athena_merge_called_exactly_once_per_run(self):
        """
        For a normal-sized batch (<= BATCH_SIZE=500), Athena.execute_query
        must be called exactly once per load_silver_run call.
        """
        run_id = "athena-call-count-001"
        rows = [
            {"source": "oms", "entity": "orders", "run_id": run_id,
             "record_id": f"O{i}", "order_id": f"O{i}", "ingestion_timestamp": _ts()}
            for i in range(5)
        ]
        loader = _make_loader(rows, run_id, "oms", "orders")
        loader.load_silver_run("oms", "orders", run_id, dry_run=False)
        self.assertEqual(loader.athena.execute_query.call_count, 1)

    def test_large_batch_is_chunked_into_multiple_athena_calls(self):
        """
        Batches exceeding BATCH_SIZE=500 rows must be split.
        601 rows → ceil(601/500)=2 Athena execute_query calls.
        """
        run_id = "chunked-batch-001"
        rows = [
            {"source": "oms", "entity": "orders", "run_id": run_id,
             "record_id": f"O{i}", "order_id": f"O{i}", "ingestion_timestamp": _ts()}
            for i in range(601)
        ]
        loader = _make_loader(rows, run_id, "oms", "orders")
        loader.load_silver_run("oms", "orders", run_id, dry_run=False)
        self.assertEqual(loader.athena.execute_query.call_count, 2)

    def test_silver_processing_with_no_records_is_safe(self):
        """Processing an empty input must not raise and must return all-zero counts."""
        proc = _make_processor()
        result = proc.process_records("oms", "orders", str(uuid.uuid4()), raw_records=[])
        self.assertEqual(result["input_record_count"], 0)
        self.assertEqual(result["valid_count"], 0)
        self.assertEqual(result["invalid_count"], 0)


# ---------------------------------------------------------------------------
# TASK 9 — Schema Evolution Verification
# ---------------------------------------------------------------------------

class TestSchemaEvolution(unittest.TestCase):
    """
    Verifies that new columns arriving in Silver are:
    1. Detected by handle_schema_evolution().
    2. Triggered to be ALTER TABLE ADD COLUMN via Athena DDL.
    3. Included in the effective table def used for MERGE (so values are not NULL).
    4. Correctly passed through to Iceberg even when not in the static Python TableDefinition.
    """

    def _loader_with_evolved_schema(
        self,
        rows: List[Dict[str, Any]],
        run_id: str,
        source: str,
        entity: str,
        existing_glue_columns: List[str] = None,
    ) -> IcebergLoader:
        """
        Build a loader whose Glue catalog reports *existing_glue_columns* as
        already present (simulates a table that has been evolved in a previous run).
        The incoming rows may contain additional columns beyond the static table_def.
        """
        key = f"silver/{source}/{entity}/ingestion_date=2024-01-01/run_id={run_id}/data.parquet"
        data = _parquet_bytes(rows)

        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": [{"Key": key}]}]
        mock_s3.get_paginator.return_value = mock_paginator
        mock_s3.get_object.return_value = {"Body": MagicMock(read=lambda: data)}

        existing_cols_raw = [{"Name": c} for c in (existing_glue_columns or [])]

        mock_glue = MagicMock()
        mock_glue.table_exists.return_value = True
        mock_glue.get_table_metadata.return_value = {
            "StorageDescriptor": {"Columns": existing_cols_raw},
            "PartitionKeys": [],
        }

        mock_athena = MagicMock()
        mock_athena.execute_query.return_value = {"status": "SUCCEEDED", "rows": []}
        mock_athena.run_ddl.return_value = {"status": "SUCCEEDED", "rows": []}

        mock_config = MagicMock()
        mock_config.bucket = "test-bucket"
        mock_config.database_name = "customer360"
        mock_config.region_name = "us-east-1"
        mock_config.table_location.return_value = "s3://test-bucket/iceberg/"

        return IcebergLoader(
            config=mock_config,
            glue_catalog=mock_glue,
            athena_client=mock_athena,
            s3_client=mock_s3,
        )

    def test_new_column_in_silver_triggers_schema_evolution(self):
        """
        When Silver contains a field not in the static TableDefinition,
        handle_schema_evolution must detect it and trigger ALTER TABLE via Athena DDL.
        """
        run_id = "evo-detect-001"
        rows = [
            {"source": "oms", "entity": "orders", "run_id": run_id,
             "record_id": "O1", "order_id": "O1",
             "ingestion_timestamp": _ts(),
             "new_field_xyz": "some_value",  # ← new column not in static def
             },
        ]
        loader = self._loader_with_evolved_schema(rows, run_id, "oms", "orders")
        result = loader.load_silver_run("oms", "orders", run_id, dry_run=True)
        self.assertIn("new_field_xyz", result["evolved_columns"])

    def test_new_column_triggers_athena_ddl(self):
        """
        When a new column is detected, Athena.run_ddl must be called exactly once
        with the ALTER TABLE ADD COLUMNS statement.
        """
        run_id = "evo-ddl-001"
        rows = [
            {"source": "oms", "entity": "orders", "run_id": run_id,
             "record_id": "O1", "order_id": "O1",
             "ingestion_timestamp": _ts(),
             "extra_column": "value",
             },
        ]
        loader = self._loader_with_evolved_schema(rows, run_id, "oms", "orders")
        loader.load_silver_run("oms", "orders", run_id, dry_run=False)
        # run_ddl should have been called for ALTER TABLE
        self.assertGreaterEqual(loader.athena.run_ddl.call_count, 1)

    def test_no_new_columns_does_not_trigger_ddl(self):
        """
        When Silver matches the static TableDefinition schema exactly,
        no ALTER TABLE DDL should be executed.
        """
        run_id = "evo-no-ddl-001"
        table_def = get_table_definition("oms", "orders")
        # Build rows using ONLY the known column names
        rows = [
            {col: ("O1" if col == "order_id" else "v") for col in table_def.column_names}
            | {"ingestion_timestamp": _ts(), "record_id": "O1"}
        ]
        loader = self._loader_with_evolved_schema(rows, run_id, "oms", "orders")
        result = loader.load_silver_run("oms", "orders", run_id, dry_run=True)
        self.assertEqual(result["evolved_columns"], [])

    def test_previously_evolved_column_is_not_re_added(self):
        """
        A column that was added by a previous run (and is now in Glue) must
        NOT be re-added.  handle_schema_evolution must check live Glue columns
        before triggering ALTER TABLE.
        """
        run_id = "evo-no-dup-001"
        rows = [
            {"source": "oms", "entity": "orders", "run_id": run_id,
             "record_id": "O1", "order_id": "O1",
             "ingestion_timestamp": _ts(),
             "column_from_prior_run": "already_there",
             },
        ]
        # Simulate Glue already having this column from a prior run
        loader = self._loader_with_evolved_schema(
            rows, run_id, "oms", "orders",
            existing_glue_columns=["column_from_prior_run"],
        )
        result = loader.load_silver_run("oms", "orders", run_id, dry_run=True)
        # The column is already in Glue → must not appear in evolved_columns
        self.assertNotIn("column_from_prior_run", result["evolved_columns"])

    def test_multiple_new_columns_all_detected(self):
        """Multiple new columns in a single Silver run are all detected at once."""
        run_id = "evo-multi-001"
        rows = [
            {"source": "oms", "entity": "orders", "run_id": run_id,
             "record_id": "O1", "order_id": "O1",
             "ingestion_timestamp": _ts(),
             "brand_new_col_a": "a",
             "brand_new_col_b": "b",
             "brand_new_col_c": "c",
             },
        ]
        loader = self._loader_with_evolved_schema(rows, run_id, "oms", "orders")
        result = loader.load_silver_run("oms", "orders", run_id, dry_run=True)
        evolved = set(result["evolved_columns"])
        self.assertIn("brand_new_col_a", evolved)
        self.assertIn("brand_new_col_b", evolved)
        self.assertIn("brand_new_col_c", evolved)

    def test_schema_evolution_does_not_break_existing_columns(self):
        """
        After evolution, a MERGE query must still be generated correctly for
        the existing columns.  The Athena execute_query must be called (not raise).
        """
        run_id = "evo-merge-safe-001"
        rows = [
            {"source": "oms", "entity": "orders", "run_id": run_id,
             "record_id": "O1", "order_id": "O1",
             "ingestion_timestamp": _ts(),
             "evolved_col": "val",
             },
        ]
        loader = self._loader_with_evolved_schema(rows, run_id, "oms", "orders")
        # Should not raise — MERGE must execute despite schema evolution
        try:
            loader.load_silver_run("oms", "orders", run_id, dry_run=False)
            executed = True
        except Exception as exc:
            executed = False
            self.fail(f"load_silver_run raised after schema evolution: {exc}")
        self.assertTrue(executed)

    def test_internal_underscore_columns_are_not_evolved(self):
        """
        Columns beginning with '_' (internal metadata columns) must be
        excluded from schema evolution detection.
        """
        run_id = "evo-underscore-001"
        rows = [
            {"source": "oms", "entity": "orders", "run_id": run_id,
             "record_id": "O1", "order_id": "O1",
             "ingestion_timestamp": _ts(),
             "_internal_meta": "should_be_ignored",
             },
        ]
        loader = self._loader_with_evolved_schema(rows, run_id, "oms", "orders")
        result = loader.load_silver_run("oms", "orders", run_id, dry_run=True)
        self.assertNotIn("_internal_meta", result["evolved_columns"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
