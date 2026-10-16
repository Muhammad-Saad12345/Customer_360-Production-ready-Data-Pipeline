"""
Unit tests for Customer 360 Step Functions Lambda Handlers.
Validates input/output contracts, error handling, dry-run modes, and edge cases
for all 8 pipeline Lambda handlers.
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch
import pytest

from lambda_handlers.initialize.handler import lambda_handler as init_handler
from lambda_handlers.ingest.handler import lambda_handler as ingest_handler, _get_ingester
from lambda_handlers.silver_gate.handler import lambda_handler as silver_handler
from lambda_handlers.quality_check.handler import lambda_handler as quality_handler
from lambda_handlers.iceberg_load.handler import lambda_handler as iceberg_handler
from lambda_handlers.reconcile.handler import lambda_handler as reconcile_handler
from lambda_handlers.notify.handler import lambda_handler as notify_handler
from lambda_handlers.iceberg_maintenance.handler import lambda_handler as maintenance_handler


# ─────────────────────────────────────────────────────────────────────────────
# 1. Initialize Handler Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestInitializeHandler:
    def test_initialize_defaults(self):
        result = init_handler({}, None)
        assert "run_id" in result
        assert result["run_id"].startswith("c360-")
        assert result["environment"] == "staging"
        assert len(result["sources"]) == 6
        assert result["dry_run"] is False
        assert "execution_start" in result

    def test_initialize_custom_params(self):
        event = {
            "environment": "prod",
            "sources": ["hubspot", "oms"],
            "dry_run": True,
        }
        result = init_handler(event, None)
        assert result["environment"] == "prod"
        assert result["sources"] == ["hubspot", "oms"]
        assert result["dry_run"] is True


# ─────────────────────────────────────────────────────────────────────────────
# 2. Ingest Handler Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestIngestHandler:
    def test_get_ingester_all_sources(self):
        sources = ["hubspot", "oms", "pim", "loyalty", "marketing", "support", "zendesk"]
        for s in sources:
            ingester = _get_ingester(s)
            assert ingester is not None

    def test_get_ingester_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown source"):
            _get_ingester("unknown_source")

    def test_ingest_dry_run(self):
        event = {
            "source": "hubspot",
            "entity": "contacts",
            "run_id": "c360-test-run-001",
            "dry_run": True,
        }
        result = ingest_handler(event, None)
        assert result["status"] == "dry_run"
        assert result["records_ingested"] == 0

    @patch("lambda_handlers.ingest.handler._get_ingester")
    def test_ingest_execution_success(self, mock_get_ingester):
        mock_instance = MagicMock()
        mock_instance._last_metadata = {"records_uploaded": 50, "file_path": "raw/hubspot/data.json"}
        mock_get_ingester.return_value = mock_instance

        event = {
            "source": "hubspot",
            "entity": "contacts",
            "run_id": "c360-test-run-001",
            "dry_run": False,
        }
        result = ingest_handler(event, None)
        mock_instance.run.assert_called_once()
        assert result["status"] == "success"
        assert result["records_ingested"] == 50
        assert result["s3_raw_key"] == "raw/hubspot/data.json"


# ─────────────────────────────────────────────────────────────────────────────
# 3. Silver Gate Handler Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSilverGateHandler:
    @patch("src.processing.silver_gate.HubSpotSilverProcessor")
    def test_silver_gate_success(self, mock_processor_cls):
        mock_processor = MagicMock()
        mock_processor.process_records.return_value = {
            "input_record_count": 50,
            "valid_count": 48,
            "invalid_count": 2,
            "silver_path": "s3://lakehouse/silver/hubspot/data.parquet",
            "quarantine_path": "s3://lakehouse/quarantine/hubspot/rejected.json",
        }
        mock_processor_cls.return_value = mock_processor

        event = {
            "source": "hubspot",
            "entity": "contacts",
            "run_id": "c360-test-run-001",
        }
        result = silver_handler(event, None)
        assert result["status"] == "success"
        assert result["input_record_count"] == 50
        assert result["valid_count"] == 48
        assert result["invalid_count"] == 2
        assert "s3://" in result["silver_path"]


# ─────────────────────────────────────────────────────────────────────────────
# 4. Quality Check Handler Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestQualityCheckHandler:
    def test_quality_check_dry_run(self):
        event = {
            "source": "hubspot",
            "entity": "contacts",
            "run_id": "c360-test-run-001",
            "dry_run": True,
        }
        result = quality_handler(event, None)
        assert result["status"] == "dry_run"

    def test_quality_check_missing_silver_path(self):
        event = {
            "source": "hubspot",
            "entity": "contacts",
            "run_id": "c360-test-run-001",
            "input_record_count": 10,
            "valid_count": 10,
            "invalid_count": 0,
            "silver_path": "",
        }
        with pytest.raises(ValueError, match="Silver path is empty"):
            quality_handler(event, None)

    def test_quality_check_count_mismatch(self):
        event = {
            "source": "hubspot",
            "entity": "contacts",
            "run_id": "c360-test-run-001",
            "input_record_count": 100,
            "valid_count": 90,
            "invalid_count": 5, # 95 != 100
            "silver_path": "s3://bucket/silver/data.parquet",
        }
        with pytest.raises(ValueError, match="count mismatch"):
            quality_handler(event, None)

    def test_quality_check_hard_threshold_failure(self):
        event = {
            "source": "hubspot",
            "entity": "contacts",
            "run_id": "c360-test-run-001",
            "input_record_count": 100,
            "valid_count": 85,
            "invalid_count": 15, # 15% >= 10% hard threshold
            "silver_path": "s3://bucket/silver/data.parquet",
        }
        with pytest.raises(ValueError, match="DQ gate FAILED: rejection rate 15.00%"):
            quality_handler(event, None)

    def test_quality_check_success(self):
        event = {
            "source": "hubspot",
            "entity": "contacts",
            "run_id": "c360-test-run-001",
            "input_record_count": 100,
            "valid_count": 98,
            "invalid_count": 2, # 2% < 5% warn threshold
            "silver_path": "s3://bucket/silver/data.parquet",
        }
        result = quality_handler(event, None)
        assert result["status"] == "success"
        assert result["rejection_rate_pct"] == 2.0
        assert result["valid_count"] == 98


# ─────────────────────────────────────────────────────────────────────────────
# 5. Iceberg Load Handler Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestIcebergLoadHandler:
    def test_iceberg_load_zero_records_skipped(self):
        event = {
            "source": "hubspot",
            "entity": "contacts",
            "run_id": "c360-test-run-001",
            "valid_count": 0,
        }
        result = iceberg_handler(event, None)
        assert result["status"] == "skipped"
        assert result["reason"] == "zero_valid_records"

    @patch("src.lakehouse.iceberg_loader.IcebergLoader")
    def test_iceberg_load_success(self, mock_loader_cls):
        mock_loader = MagicMock()
        mock_loader.load_silver_run.return_value = {
            "status": "success",
            "table_name": "contacts",
            "silver_row_count": 50,
            "deduped_row_count": 50,
            "athena_executed": True,
        }
        mock_loader_cls.return_value = mock_loader

        event = {
            "source": "hubspot",
            "entity": "contacts",
            "run_id": "c360-test-run-001",
            "valid_count": 50,
        }
        result = iceberg_handler(event, None)
        assert result["status"] == "success"
        assert result["silver_row_count"] == 50
        assert result["athena_executed"] is True


# ─────────────────────────────────────────────────────────────────────────────
# 6. Reconcile Handler Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestReconcileHandler:
    def test_reconcile_all_branches_passed(self):
        event = {
            "run_id": "c360-test-run-001",
            "branch_results": [
                {"source": "hubspot", "entity": "contacts", "status": "success", "silver_row_count": 50, "deduped_row_count": 50},
                {"source": "oms", "entity": "orders", "status": "success", "silver_row_count": 100, "deduped_row_count": 100},
                {"source": "pim", "entity": "products", "status": "skipped", "silver_row_count": 0, "deduped_row_count": 0},
            ]
        }
        result = reconcile_handler(event, None)
        assert result["reconciliation_status"] == "PASSED"
        assert result["status"] == "success"
        assert result["total_silver_records"] == 150
        assert result["total_sources_evaluated"] == 3

    def test_reconcile_branch_failure(self):
        event = {
            "run_id": "c360-test-run-001",
            "branch_results": [
                {"source": "hubspot", "entity": "contacts", "status": "success", "silver_row_count": 50},
                {"source": "oms", "entity": "orders", "status": "failed", "silver_row_count": 0},
            ]
        }
        result = reconcile_handler(event, None)
        assert result["reconciliation_status"] == "FAILED"
        assert result["status"] == "failed"
        assert len(result["discrepancies"]) == 1


# ─────────────────────────────────────────────────────────────────────────────
# 7. Notify Handler Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestNotifyHandler:
    @patch("boto3.client")
    def test_notify_handler_sns_published(self, mock_boto_client):
        mock_sns = MagicMock()
        mock_sns.publish.return_value = {"MessageId": "msg-12345"}
        mock_boto_client.return_value = mock_sns

        with patch.dict(os.environ, {"C360_SNS_ALERT_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:alerts"}):
            event = {
                "run_id": "c360-test-run-001",
                "status": "failed",
                "failed_state": "Ingest_HubSpot",
                "error": {"Cause": "Connection timed out"},
                "execution_id": "arn:aws:states:us-east-1:123456789012:execution:c360:exec-1",
            }
            result = notify_handler(event, None)
            assert result["notification_sent"] is True
            assert result["sns_message_id"] == "msg-12345"
            mock_sns.publish.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# 8. Iceberg Maintenance Handler Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestIcebergMaintenanceHandler:
    def test_maintenance_dry_run(self):
        event = {
            "run_id": "c360-test-run-001",
            "dry_run": True,
        }
        result = maintenance_handler(event, None)
        assert result["status"] == "dry_run"
        assert result["tables_processed"] == 6

    @patch("src.lakehouse.iceberg_maintenance.IcebergMaintenanceManager")
    def test_maintenance_execution(self, mock_mgr_cls):
        mock_mgr = MagicMock()
        res_mock = MagicMock()
        res_mock.is_success = True
        res_mock.optimize_status = "success"
        res_mock.vacuum_status = "success"
        res_mock.errors = []
        mock_mgr.run_full_maintenance.return_value = res_mock
        mock_mgr_cls.return_value = mock_mgr

        event = {
            "run_id": "c360-test-run-001",
            "tables": ["contacts", "orders"],
            "dry_run": False,
        }
        result = maintenance_handler(event, None)
        assert result["status"] == "complete"
        assert result["tables_processed"] == 2
        assert result["maintenance_summary"]["contacts"]["status"] == "success"
