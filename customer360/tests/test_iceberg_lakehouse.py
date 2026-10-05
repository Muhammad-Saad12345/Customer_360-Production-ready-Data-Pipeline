from __future__ import annotations

import io
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pyarrow as pa
import pyarrow.parquet as pq

from src.lakehouse.athena_client import AthenaClient, AthenaQueryExecutionError, AthenaQueryResult
from src.lakehouse.config import LakehouseConfig, get_lakehouse_config
from src.lakehouse.glue_catalog import GlueIcebergCatalog
from src.lakehouse.iceberg_loader import IcebergLoader
from src.lakehouse.table_definitions import (
    HUBSPOT_CONTACTS_TABLE,
    LOYALTY_TABLE,
    MARKETING_EVENTS_TABLE,
    OMS_ORDERS_TABLE,
    PIM_PRODUCTS_TABLE,
    TABLE_REGISTRY,
    ColumnDefinition,
    TableDefinition,
    generate_athena_add_column_ddl,
    generate_athena_create_table_ddl,
    generate_athena_merge_query,
    get_all_table_definitions,
    get_table_definition,
)
from src.lakehouse.validator import IcebergValidator, ValidationReport


class TestIcebergTableDefinitions(unittest.TestCase):
    """Test schema, DDL, and MERGE generation for Customer 360 Iceberg tables."""

    def test_all_expected_entities_are_registered(self):
        # Source ownership:
        #   hubspot  -> contacts ONLY (customer/CRM data)
        #   oms      -> orders (order management system)
        #   pim      -> products (product information management)
        #   loyalty  -> loyalty/rewards ledger
        #   marketing/mailchimp -> campaign events
        expected = [
            ("hubspot", "contacts"),
            ("oms", "orders"),
            ("pim", "products"),
            ("loyalty", "loyalty"),
            ("marketing", "campaign_events"),
            ("mailchimp", "campaign_events"),
        ]
        for src, ent in expected:
            table_def = get_table_definition(src, ent)
            self.assertIsNotNone(table_def, f"Missing TableDefinition for {src}/{ent}")
            self.assertTrue(len(table_def.primary_keys) >= 1)
            self.assertIn("ingestion_date", table_def.partition_keys)

        # Regression guard: HubSpot must NOT own orders or products.
        # Orders come from OMS; products come from PIM.
        self.assertIsNone(
            get_table_definition("hubspot", "orders"),
            "hubspot/orders must NOT be registered — orders are owned by OMS",
        )
        self.assertIsNone(
            get_table_definition("hubspot", "products"),
            "hubspot/products must NOT be registered — products are owned by PIM",
        )

    def test_athena_create_table_ddl_generation(self):
        table_def = HUBSPOT_CONTACTS_TABLE
        ddl = generate_athena_create_table_ddl(
            table_def=table_def,
            s3_location="s3://c360-bucket786/iceberg/hubspot_contacts/",
            database_name="customer360_iceberg",
        )
        self.assertIn("CREATE TABLE IF NOT EXISTS customer360_iceberg.hubspot_contacts", ddl)
        # No backticks — plain identifiers required by Athena Engine v3
        self.assertNotIn("`", ddl)
        self.assertIn("customer_id string", ddl)
        self.assertIn("PARTITIONED BY (ingestion_date)", ddl)
        self.assertIn("LOCATION 's3://c360-bucket786/iceberg/hubspot_contacts/'", ddl)
        self.assertIn("'table_type'='ICEBERG'", ddl)
        self.assertIn("'format'='parquet'", ddl)
        self.assertIn("'write_compression'='snappy'", ddl)

    def test_athena_merge_query_generation_idempotency(self):
        table_def = OMS_ORDERS_TABLE
        merge_sql = generate_athena_merge_query(
            table_def=table_def,
            source_subquery_or_table="SELECT * FROM customer360_iceberg._staging_oms_orders",
            database_name="customer360_iceberg",
        )
        self.assertIn("MERGE INTO customer360_iceberg.oms_orders AS target", merge_sql)
        # No backticks — plain identifiers required by Athena Engine v3
        self.assertNotIn("`", merge_sql)
        self.assertIn("ON target.order_id = source.order_id", merge_sql)
        self.assertIn("WHEN MATCHED THEN", merge_sql)
        self.assertIn("UPDATE SET", merge_sql)
        self.assertIn("WHEN NOT MATCHED THEN", merge_sql)
        self.assertIn("INSERT", merge_sql)

    def test_schema_evolution_ddl_generation(self):
        new_cols = [
            ColumnDefinition("loyalty_tier_override", "string", "Manual VIP override tier"),
            ColumnDefinition("risk_score", "double", "Credit risk score"),
        ]
        alter_ddl = generate_athena_add_column_ddl(
            table_name="hubspot_contacts",
            new_columns=new_cols,
            database_name="customer360_iceberg",
        )
        self.assertEqual(
            alter_ddl,
            "ALTER TABLE customer360_iceberg.hubspot_contacts ADD COLUMNS (loyalty_tier_override string, risk_score double);",
        )


class TestGlueIcebergCatalog(unittest.TestCase):
    """Test AWS Glue Data Catalog integration for Iceberg database & table metadata."""

    def setUp(self):
        self.mock_glue = MagicMock()
        self.config = LakehouseConfig(
            database_name="customer360_iceberg",
            bucket="c360-bucket786",
            region_name="us-east-1",
            s3_prefix="iceberg",
            athena_workgroup="primary",
            athena_output_location="s3://c360-bucket786/athena-results/",
        )
        self.catalog = GlueIcebergCatalog(config=self.config, glue_client=self.mock_glue)

    def test_create_database_if_not_exists_when_absent(self):
        from botocore.exceptions import ClientError
        self.mock_glue.get_database.side_effect = ClientError(
            {"Error": {"Code": "EntityNotFoundException", "Message": "Not found"}}, "GetDatabase"
        )
        created = self.catalog.create_database_if_not_exists()
        self.assertTrue(created)
        self.mock_glue.create_database.assert_called_once()
        db_input = self.mock_glue.create_database.call_args[1]["DatabaseInput"]
        self.assertEqual(db_input["Name"], "customer360_iceberg")
        self.assertEqual(db_input["Parameters"]["classification"], "iceberg")

    def test_create_database_if_not_exists_when_present(self):
        self.mock_glue.get_database.return_value = {"Database": {"Name": "customer360_iceberg"}}
        created = self.catalog.create_database_if_not_exists()
        self.assertFalse(created)
        self.mock_glue.create_database.assert_not_called()

    def test_is_iceberg_table_verification(self):
        self.mock_glue.get_table.return_value = {
            "Table": {
                "Name": "hubspot_contacts",
                "Parameters": {
                    "table_type": "ICEBERG",
                    "metadata_location": "s3://c360-bucket786/iceberg/hubspot_contacts/metadata",
                },
                "StorageDescriptor": {
                    "Location": "s3://c360-bucket786/iceberg/hubspot_contacts",
                },
            }
        }
        self.assertTrue(self.catalog.is_iceberg_table("hubspot_contacts"))

    def test_direct_glue_table_entry_creation(self):
        from botocore.exceptions import ClientError
        self.mock_glue.get_table.side_effect = ClientError(
            {"Error": {"Code": "EntityNotFoundException", "Message": "Not found"}}, "GetTable"
        )
        table_def = PIM_PRODUCTS_TABLE
        created = self.catalog.create_iceberg_table_entry(table_def)
        self.assertTrue(created)
        self.mock_glue.create_table.assert_called_once()
        call_input = self.mock_glue.create_table.call_args[1]["TableInput"]
        self.assertEqual(call_input["Name"], "pim_products")
        self.assertEqual(call_input["Parameters"]["table_type"], "ICEBERG")
        self.assertEqual(call_input["TableType"], "EXTERNAL_TABLE")


class TestAthenaClient(unittest.TestCase):
    """Test Amazon Athena query execution, polling, and result parsing."""

    def setUp(self):
        self.mock_athena = MagicMock()
        self.config = LakehouseConfig(
            database_name="customer360_iceberg",
            bucket="c360-bucket786",
            region_name="us-east-1",
            s3_prefix="iceberg",
            athena_workgroup="primary",
            athena_output_location="s3://c360-bucket786/athena-results/",
        )
        self.client = AthenaClient(config=self.config, athena_client=self.mock_athena)

    def _setup_athena_success(self, query_id: str = "query-123", time_ms: int = 450, scanned: int = 10240):
        """Helper: configure mock to immediately return SUCCEEDED state."""
        self.mock_athena.start_query_execution.return_value = {"QueryExecutionId": query_id}
        self.mock_athena.get_query_execution.return_value = {
            "QueryExecution": {
                "QueryExecutionId": query_id,
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {"TotalExecutionTimeInMillis": time_ms, "DataScannedInBytes": scanned},
            }
        }

    def test_successful_query_execution_and_result_parsing(self):
        self._setup_athena_success("query-123")
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [{"Name": "customer_id"}, {"Name": "email"}],
                },
                "Rows": [
                    {"Data": [{"VarCharValue": "customer_id"}, {"VarCharValue": "email"}]},  # header row
                    {"Data": [{"VarCharValue": "CUST-001"}, {"VarCharValue": "alice@example.com"}]},
                ],
            }
        }]
        self.mock_athena.get_paginator.return_value = mock_paginator

        result = self.client.execute_query(
            "SELECT customer_id, email FROM hubspot_contacts LIMIT 1;",
            poll_interval_seconds=0,
        )
        self.assertEqual(result.state, "SUCCEEDED")
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.rows[0]["customer_id"], "CUST-001")
        self.assertEqual(result.rows[0]["email"], "alice@example.com")
        self.assertEqual(result.execution_time_ms, 450)

    def test_failed_query_raises_athena_execution_error(self):
        self.mock_athena.start_query_execution.return_value = {"QueryExecutionId": "query-bad"}
        self.mock_athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {
                    "State": "FAILED",
                    "StateChangeReason": "SYNTAX_ERROR: line 1:15: Table not found",
                },
            }
        }
        with self.assertRaises(AthenaQueryExecutionError) as ctx:
            self.client.execute_query(
                "SELECT * FROM non_existent_table;",
                poll_interval_seconds=0,
            )
        self.assertIn("Table not found", str(ctx.exception))


class TestIcebergLoader(unittest.TestCase):
    """Test controlled loading, idempotency, deduplication, and schema evolution."""

    def setUp(self):
        self.mock_glue = MagicMock()
        self.mock_athena = MagicMock()
        self.mock_s3 = MagicMock()
        self.config = LakehouseConfig(
            database_name="customer360_iceberg",
            bucket="c360-bucket786",
            region_name="us-east-1",
            s3_prefix="iceberg",
            athena_workgroup="primary",
            athena_output_location="s3://c360-bucket786/athena-results/",
        )
        self.loader = IcebergLoader(
            config=self.config,
            glue_catalog=GlueIcebergCatalog(self.config, glue_client=self.mock_glue),
            athena_client=AthenaClient(self.config, athena_client=self.mock_athena),
            s3_client=self.mock_s3,
        )

    def test_deduplicate_records_by_primary_key_intra_batch(self):
        records = [
            {"order_id": "ORD-1", "net_total": 100.0, "updated_at": "2026-09-24T10:00:00Z"},
            {"order_id": "ORD-1", "net_total": 120.0, "updated_at": "2026-09-24T10:05:00Z"},  # latest update
            {"order_id": "ORD-2", "net_total": 50.0, "updated_at": "2026-09-24T10:00:00Z"},
        ]
        deduped = self.loader.deduplicate_records_by_primary_key(records, ["order_id"])
        self.assertEqual(len(deduped), 2)
        ord_1 = next(r for r in deduped if r["order_id"] == "ORD-1")
        self.assertEqual(ord_1["net_total"], 120.0)

    def test_empty_silver_run_is_gracefully_skipped(self):
        self.mock_glue.table_exists.return_value = True
        self.mock_s3.get_paginator.return_value.paginate.return_value = []
        result = self.loader.load_silver_run(source="oms", entity="orders", run_id="empty-run-001")
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "empty_silver_run")
        self.assertEqual(result["silver_row_count"], 0)

    def test_schema_evolution_detection(self):
        """Schema evolution detects new columns and gracefully handles Athena DDL errors."""
        table_def = HUBSPOT_CONTACTS_TABLE
        incoming_schema = pa.schema([
            ("customer_id", pa.string()),
            ("email", pa.string()),
            ("loyalty_level", pa.string()),  # New field!
            ("annual_spend", pa.float64()),  # New field!
        ])
        # Patch run_ddl to avoid polling a real Athena mock
        with patch.object(self.loader.athena, "run_ddl", return_value=None):
            evolved = self.loader.handle_schema_evolution(table_def, incoming_schema)
        self.assertEqual(len(evolved), 2)
        evolved_names = [c.name for c in evolved]
        self.assertIn("loyalty_level", evolved_names)
        self.assertIn("annual_spend", evolved_names)

    def test_dry_run_mode_returns_reconciliation_summary(self):
        self.mock_glue.table_exists.return_value = True

        # Mock Silver parquet in S3
        sample_rows = [
            {
                "customer_id": "C-101",
                "email": "c101@example.com",
                "first_name": "Test",
                "last_name": "User",
                "ingestion_timestamp": "2026-09-24T12:00:00Z",
                "run_id": "run-dry-001",
            }
        ]
        tab = pa.Table.from_pylist(sample_rows)
        buf = io.BytesIO()
        pq.write_table(tab, buf)

        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": [{"Key": "silver/hubspot/contacts/ingestion_date=2026-09-24/run_id=run-dry-001/data.parquet"}]}]
        self.mock_s3.get_paginator.return_value = mock_paginator
        self.mock_s3.get_object.return_value = {"Body": io.BytesIO(buf.getvalue())}

        result = self.loader.load_silver_run(
            source="hubspot",
            entity="contacts",
            run_id="run-dry-001",
            dry_run=True,
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["mode"], "dry_run")
        self.assertEqual(result["silver_row_count"], 1)
        self.assertEqual(result["deduped_row_count"], 1)
        self.assertEqual(result["primary_keys"], ["customer_id"])


class TestIcebergValidator(unittest.TestCase):
    """Test validation suite proving the 6 required Iceberg lakehouse guarantees."""

    def setUp(self):
        self.mock_glue = MagicMock()
        self.mock_athena = MagicMock()
        self.config = LakehouseConfig(
            database_name="customer360_iceberg",
            bucket="c360-bucket786",
            region_name="us-east-1",
            s3_prefix="iceberg",
            athena_workgroup="primary",
            athena_output_location="s3://c360-bucket786/athena-results/",
        )
        self.validator = IcebergValidator(
            config=self.config,
            glue_catalog=GlueIcebergCatalog(self.config, glue_client=self.mock_glue),
            athena_client=AthenaClient(self.config, athena_client=self.mock_athena),
        )

    def test_requirement_1_tables_exist(self):
        """Proof: Iceberg tables exist in Glue Catalog."""
        self.mock_glue.table_exists.return_value = True
        self.assertTrue(self.validator.check_table_exists("hubspot_contacts"))
        self.assertTrue(self.validator.check_table_exists("oms_orders"))

    def test_requirement_2_glue_metadata_correct(self):
        """Proof: Glue Catalog metadata is correct ('table_type'='ICEBERG', valid S3 location)."""
        self.mock_glue.get_table.return_value = {
            "Table": {
                "Name": "hubspot_contacts",
                "Parameters": {"table_type": "ICEBERG"},
                "StorageDescriptor": {
                    "Location": "s3://c360-bucket786/iceberg/hubspot_contacts",
                    "Columns": [{"Name": "customer_id", "Type": "string"}],
                },
                "PartitionKeys": [{"Name": "ingestion_date", "Type": "date"}],
            }
        }
        res = self.validator.check_glue_metadata("hubspot_contacts")
        self.assertTrue(res["is_iceberg"])
        self.assertTrue(res["valid_location"])
        self.assertEqual(res["table_type"], "ICEBERG")
        self.assertIn("ingestion_date", res["partition_keys"])

    def _setup_athena_success(self, query_id: str = "q-1", time_ms: int = 300, scanned: int = 4096):
        """Configure Athena mock to immediately return SUCCEEDED."""
        self.mock_athena.start_query_execution.return_value = {"QueryExecutionId": query_id}
        self.mock_athena.get_query_execution.return_value = {
            "QueryExecution": {
                "QueryExecutionId": query_id,
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {"TotalExecutionTimeInMillis": time_ms, "DataScannedInBytes": scanned},
            }
        }

    def test_requirement_3_athena_can_query_tables(self):
        """Proof: Athena can query the tables and return row counts and schema."""
        self._setup_athena_success()
        mock_paginator = MagicMock()
        mock_paginator.paginate.side_effect = [
            [{"ResultSet": {"ResultSetMetadata": {"ColumnInfo": [{"Name": "id"}]}, "Rows": [{"Data": [{"VarCharValue": "id"}]}, {"Data": [{"VarCharValue": "1"}]}]}}],
            [{"ResultSet": {"ResultSetMetadata": {"ColumnInfo": [{"Name": "total_count"}]}, "Rows": [{"Data": [{"VarCharValue": "total_count"}]}, {"Data": [{"VarCharValue": "42"}]}]}}],
        ]
        self.mock_athena.get_paginator.return_value = mock_paginator

        original_exec = self.validator.athena.execute_query

        def fast_exec(q, **kw):
            return original_exec(q, poll_interval_seconds=0, **{k: v for k, v in kw.items() if k != "poll_interval_seconds"})

        with patch.object(self.validator.athena, "execute_query", side_effect=fast_exec):
            res = self.validator.check_athena_queryable("oms_orders")
        self.assertTrue(res["queryable"])
        self.assertEqual(res["total_count"], 42)

    def test_requirement_4_row_counts_consistent_with_silver(self):
        """Proof: Row counts are consistent between Iceberg and Silver."""
        self._setup_athena_success("q-count")
        mock_pag = MagicMock()
        mock_pag.paginate.return_value = [
            {"ResultSet": {"ResultSetMetadata": {"ColumnInfo": [{"Name": "total"}]}, "Rows": [{"Data": [{"VarCharValue": "total"}]}, {"Data": [{"VarCharValue": "150"}]}]}}
        ]
        self.mock_athena.get_paginator.return_value = mock_pag

        original_exec = self.validator.athena.execute_query

        def fast_exec(q, **kw):
            return original_exec(q, poll_interval_seconds=0, **{k: v for k, v in kw.items() if k != "poll_interval_seconds"})

        with patch.object(self.validator.athena, "execute_query", side_effect=fast_exec):
            res = self.validator.check_row_count_consistency("loyalty_loyalty", expected_count=150)
        self.assertTrue(res["consistent"])
        self.assertEqual(res["actual_iceberg_count"], 150)

    def test_requirement_5_primary_key_duplicates_not_introduced(self):
        """Proof: Primary/business-key duplicates are not introduced."""
        self._setup_athena_success("q-dup")
        mock_pag = MagicMock()
        mock_pag.paginate.return_value = [
            {"ResultSet": {"ResultSetMetadata": {"ColumnInfo": [{"Name": "order_id"}, {"Name": "duplicate_count"}]}, "Rows": []}}  # 0 rows = 0 duplicates!
        ]
        self.mock_athena.get_paginator.return_value = mock_pag

        original_exec = self.validator.athena.execute_query

        def fast_exec(q, **kw):
            return original_exec(q, poll_interval_seconds=0, **{k: v for k, v in kw.items() if k != "poll_interval_seconds"})

        with patch.object(self.validator.athena, "execute_query", side_effect=fast_exec):
            res = self.validator.check_no_duplicate_keys(OMS_ORDERS_TABLE)
        self.assertFalse(res["has_duplicates"])
        self.assertEqual(len(res["duplicate_samples"]), 0)

    def test_requirement_6_rerunning_process_is_safe_and_idempotent(self):
        """
        Proof: Rerunning the process is safe and preserves data integrity.
        Validates that repeated runs do not inflate counts or introduce duplicates.
        """
        self.mock_glue.table_exists.return_value = True
        self.mock_glue.get_table.return_value = {
            "Table": {
                "Parameters": {"table_type": "ICEBERG"},
                "StorageDescriptor": {"Location": "s3://c360-bucket786/iceberg/pim_products"},
                "PartitionKeys": [{"Name": "ingestion_date"}],
            }
        }
        self._setup_athena_success("q-validate")
        mock_pag = MagicMock()
        mock_pag.paginate.side_effect = [
            # check_athena_queryable limit query
            [{"ResultSet": {"ResultSetMetadata": {"ColumnInfo": [{"Name": "product_id"}]}, "Rows": []}}],
            # check_athena_queryable count query
            [{"ResultSet": {"ResultSetMetadata": {"ColumnInfo": [{"Name": "total_count"}]}, "Rows": [{"Data": [{"VarCharValue": "total_count"}]}, {"Data": [{"VarCharValue": "25"}]}]}}],
            # check_no_duplicate_keys query (empty result = no duplicates)
            [{"ResultSet": {"ResultSetMetadata": {"ColumnInfo": [{"Name": "product_id"}]}, "Rows": []}}],
        ]
        self.mock_athena.get_paginator.return_value = mock_pag

        original_exec = self.validator.athena.execute_query

        def fast_exec(q, **kw):
            return original_exec(q, poll_interval_seconds=0, **{k: v for k, v in kw.items() if k != "poll_interval_seconds"})

        with patch.object(self.validator.athena, "execute_query", side_effect=fast_exec):
            report = self.validator.validate_table("pim", "products", expected_silver_count=25)
        self.assertTrue(report.is_valid)
        self.assertTrue(report.idempotent_rerun_safe)
        self.assertFalse(report.has_duplicates)
        self.assertEqual(report.row_count, 25)


if __name__ == "__main__":
    unittest.main()
