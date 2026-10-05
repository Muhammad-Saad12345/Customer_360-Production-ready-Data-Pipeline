from __future__ import annotations

import unittest
from unittest.mock import MagicMock, call, patch

from src.lakehouse.config import LakehouseConfig
from src.lakehouse.iceberg_maintenance import IcebergMaintenanceManager, MaintenanceResult


class TestIcebergMaintenance(unittest.TestCase):
    """Unit tests for Athena/Iceberg-native operational maintenance."""

    def setUp(self):
        self.config = LakehouseConfig(
            database_name="customer360_iceberg",
            bucket="c360-bucket786",
            region_name="us-east-1",
            s3_prefix="iceberg",
            athena_workgroup="primary",
            athena_output_location="s3://c360-bucket786/athena-results/",
        )
        self.mock_athena = MagicMock()
        self.mock_glue = MagicMock()
        self.manager = IcebergMaintenanceManager(
            config=self.config,
            athena_client=self.mock_athena,
            glue_catalog=self.mock_glue,
        )

    def test_optimize_table_generates_correct_sql(self):
        self.mock_athena.run_ddl.return_value = "query_opt_123"
        res = self.manager.optimize_table("oms_orders")

        self.assertEqual(res["status"], "success")
        self.assertEqual(res["query_id"], "query_opt_123")
        expected_sql = "OPTIMIZE customer360_iceberg.oms_orders REWRITE DATA USING BIN_PACK;"
        self.mock_athena.run_ddl.assert_called_once_with(expected_sql)

    def test_optimize_table_with_partition_filter(self):
        self.mock_athena.run_ddl.return_value = "query_opt_456"
        res = self.manager.optimize_table("oms_orders", partition_filter="ingestion_date = DATE '2026-09-30'")

        self.assertEqual(res["status"], "success")
        expected_sql = "OPTIMIZE customer360_iceberg.oms_orders REWRITE DATA USING BIN_PACK WHERE ingestion_date = DATE '2026-09-30';"
        self.mock_athena.run_ddl.assert_called_once_with(expected_sql)

    def test_vacuum_table_sets_retention_and_vacuums(self):
        self.mock_athena.run_ddl.side_effect = ["query_alter_1", "query_vac_2"]
        res = self.manager.vacuum_table("hubspot_contacts", retention_days=14)

        self.assertEqual(res["status"], "success")
        self.assertEqual(res["retention_days"], 14)

        # 14 days = 14 * 86400 = 1209600 seconds
        expected_alter = (
            "ALTER TABLE customer360_iceberg.hubspot_contacts "
            "SET TBLPROPERTIES ('vacuum_max_snapshot_age_seconds' = '1209600');"
        )
        expected_vacuum = "VACUUM customer360_iceberg.hubspot_contacts;"

        calls = [call(expected_alter), call(expected_vacuum)]
        self.mock_athena.run_ddl.assert_has_calls(calls)

    def test_maintain_table_when_absent_in_catalog(self):
        self.mock_glue.table_exists.return_value = False
        res = self.manager.maintain_table("nonexistent_table")

        self.assertFalse(res.is_success)
        self.assertIn("does not exist in Glue catalog", res.errors[0])
        self.assertEqual(res.optimize_status, "skipped")
        self.assertEqual(res.vacuum_status, "skipped")
        self.mock_athena.run_ddl.assert_not_called()

    def test_maintain_table_full_success(self):
        self.mock_glue.table_exists.return_value = True
        self.mock_athena.run_ddl.side_effect = ["query_opt", "query_alter", "query_vac"]

        res = self.manager.maintain_table("pim_products", retention_days=7)

        self.assertTrue(res.is_success)
        self.assertEqual(res.optimize_status, "success")
        self.assertEqual(res.vacuum_status, "success")
        self.assertEqual(len(res.query_execution_ids), 2)
        self.assertEqual(res.query_execution_ids, ["query_opt", "query_vac"])

    def test_run_maintenance_all_executes_all_unique_tables(self):
        self.mock_glue.table_exists.return_value = True
        self.mock_athena.run_ddl.return_value = "query_success"

        summary = self.manager.run_maintenance_all(retention_days=7)

        self.assertEqual(summary["database"], "customer360_iceberg")
        self.assertGreaterEqual(summary["total_tables"], 5)
        self.assertEqual(summary["failed"], 0)
        self.assertEqual(summary["succeeded"], summary["total_tables"])
        self.assertIn("hubspot_contacts", summary["tables"])
        self.assertIn("oms_orders", summary["tables"])
        self.assertIn("pim_products", summary["tables"])
        self.assertIn("loyalty_loyalty", summary["tables"])
        self.assertIn("marketing_campaign_events", summary["tables"])
        self.assertIn("zendesk_tickets", summary["tables"])

    def test_maintenance_never_calls_arbitrary_s3_delete(self):
        """CRITICAL ARCHITECTURAL RULE: Never use boto3 or S3 delete against active Iceberg table paths."""
        mock_s3 = MagicMock()
        with patch("boto3.client", return_value=mock_s3):
            self.mock_glue.table_exists.return_value = True
            self.mock_athena.run_ddl.return_value = "query_id"
            self.manager.run_maintenance_all()

            mock_s3.delete_object.assert_not_called()
            mock_s3.delete_objects.assert_not_called()


if __name__ == "__main__":
    unittest.main()
