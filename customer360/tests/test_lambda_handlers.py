"""
Unit and contract tests for the Step Functions Lambda handlers.

The contract tests read step_functions/customer360_batch.asl.json and check
that every "$.Payload.<field>" a ResultSelector picks is present in the
handler output, in normal, no-data and dry-run modes. A missing field raises
States.Runtime in Step Functions, which no Catch can handle, so this is the
check that keeps the state machine and the handlers in step.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import boto3
import pytest

from lambda_handlers.iceberg_load.handler import lambda_handler as iceberg_handler
from lambda_handlers.iceberg_maintenance.handler import (
    lambda_handler as maintenance_handler,
)
from lambda_handlers.ingest.handler import lambda_handler as ingest_handler
from lambda_handlers.initialize.handler import ConcurrentRunError
from lambda_handlers.initialize.handler import lambda_handler as init_handler
from lambda_handlers.notify.handler import lambda_handler as notify_handler
from lambda_handlers.quality_check.handler import DataQualityGateFailed
from lambda_handlers.quality_check.handler import lambda_handler as quality_handler
from lambda_handlers.reconcile.handler import lambda_handler as reconcile_handler
from lambda_handlers.silver_gate.handler import lambda_handler as silver_handler
from src.errors import ReconciliationFailed, SourceModeError
from src.pipeline_contract import SOURCE_ENTITIES, get_ingester

ASL_PATH = Path(__file__).resolve().parent.parent / "step_functions" / "customer360_batch.asl.json"
BASE = {"run_id": "c360-test-run", "environment": "staging", "dry_run": False}


def _asl_states() -> dict:
    text = re.sub(r"\$\{[a-z0-9_]+\}", "arn:aws:lambda:us-east-1:123456789012:function:x", ASL_PATH.read_text())
    asl = json.loads(text)
    states = dict(asl["States"])
    for state in asl["States"].values():
        if state["Type"] == "Map":
            states.update(state["ItemProcessor"]["States"])
    return states


def _selected_payload_fields(state_name: str) -> set:
    selector = _asl_states()[state_name].get("ResultSelector", {})
    return {v.split(".", 2)[2] for v in selector.values() if isinstance(v, str) and v.startswith("$.Payload.")}


# ── Initialize ──────────────────────────────────────────────────────────────

class TestInitialize:
    def test_defaults_all_sources_and_stable_run_id(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "staging")
        event = {"input": {}, "execution_name": "manual-2026-11-13", "execution_arn": "", "state_machine_arn": ""}
        first = init_handler(event, None)
        second = init_handler(event, None)
        assert first["run_id"] == second["run_id"] == "c360-manual-2026-11-13"
        assert [s["source"] for s in first["sources"]] == list(SOURCE_ENTITIES)
        assert first["sources"][1] == {"source": "oms", "entity": "orders"}
        assert first["mandatory_sources"] == ["hubspot", "oms"]
        assert first["dry_run"] is False

    def test_subset_and_mandatory_intersection(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "staging")
        out = init_handler({"input": {"sources": ["pim", "oms"], "dry_run": True}, "execution_name": "x"}, None)
        assert [s["source"] for s in out["sources"]] == ["pim", "oms"]
        assert out["mandatory_sources"] == ["oms"]
        assert out["dry_run"] is True

    def test_unknown_source_rejected(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "staging")
        with pytest.raises(ValueError):
            init_handler({"input": {"sources": ["crm"]}}, None)

    def test_environment_mismatch_rejected(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "prod")
        with pytest.raises(ValueError):
            init_handler({"input": {"environment": "staging"}}, None)

    def test_refuses_to_overlap_running_execution(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "staging")
        sfn = MagicMock()
        sfn.list_executions.return_value = {"executions": [{"executionArn": "arn:other"}, {"executionArn": "arn:me"}]}
        event = {"input": {}, "execution_name": "me", "execution_arn": "arn:me", "state_machine_arn": "arn:sm"}
        with pytest.raises(ConcurrentRunError):
            init_handler(event, None, sfn_client=sfn)
        sfn.list_executions.return_value = {"executions": [{"executionArn": "arn:me"}]}
        assert init_handler(event, None, sfn_client=sfn)["run_id"] == "c360-me"

    def test_output_matches_state_machine(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "staging")
        out = init_handler({"input": {}, "execution_name": "x"}, None)
        assert _selected_payload_fields("InitializePipeline") <= set(out)


# ── Ingest ──────────────────────────────────────────────────────────────────

class TestIngest:
    def test_factory_covers_every_source(self):
        for source, entity in SOURCE_ENTITIES.items():
            assert get_ingester(source, "r1").object_name == entity
        with pytest.raises(ValueError):
            get_ingester("crm", "r1")

    def test_dry_run_has_every_selected_field(self):
        out = ingest_handler({**BASE, "dry_run": True, "source": "oms", "entity": "orders"}, None)
        assert out["status"] == "dry_run" and out["s3_raw_key"] == "" and out["source_mode"] == "synthetic"
        assert _selected_payload_fields("Ingest") <= set(out)

    def test_synthetic_run_writes_raw_and_moves_checkpoint(self, tmp_path, monkeypatch):
        monkeypatch.setenv("C360_SOURCE_MODES", json.dumps({"oms": "synthetic"}))
        out = ingest_handler({**BASE, "source": "oms", "entity": "orders"}, None)
        assert out["status"] == "success" and out["records_ingested"] > 0
        assert "run_id=c360-test-run" in out["s3_raw_key"]
        body = boto3.client("s3").get_object(Bucket="c360-bucket786", Key=out["s3_raw_key"])["Body"].read()
        assert len(body.decode().splitlines()) == out["records_ingested"]
        assert _selected_payload_fields("Ingest") <= set(out)

    def test_required_mode_blocks_synthetic(self, monkeypatch):
        monkeypatch.setenv("C360_SOURCE_MODES", json.dumps({"oms": "synthetic"}))
        monkeypatch.setenv("C360_REQUIRED_SOURCE_MODE", "api")
        with pytest.raises(SourceModeError):
            ingest_handler({**BASE, "source": "oms", "entity": "orders"}, None)

    def test_entity_mismatch_rejected(self):
        with pytest.raises(ValueError):
            ingest_handler({**BASE, "source": "oms", "entity": "products"}, None)


# ── Silver gate ─────────────────────────────────────────────────────────────

class TestSilverGate:
    def test_no_raw_key_means_no_data(self):
        out = silver_handler({**BASE, "source": "oms", "entity": "orders", "s3_raw_key": ""}, None)
        assert out["status"] == "no_data" and out["valid_count"] == 0
        assert _selected_payload_fields("SilverGate") <= set(out)

    def test_reads_exact_raw_key(self):
        rows = [
            {"order_id": "O-1", "customer_id": "C-1", "order_ts": "2026-01-01T00:00:00Z", "order_status": "paid",
             "subtotal": 10, "tax": 1, "shipping": 1, "discount": 0, "net_total": 12, "currency": "USD",
             "channel": "web", "updated_at": "2026-01-01T00:00:00Z"},
            {"order_id": "O-2", "customer_id": "", "order_ts": "bad", "order_status": "x", "subtotal": -1, "tax": 0,
             "shipping": 0, "discount": 0, "net_total": 0, "currency": "USD", "channel": "web", "updated_at": "2026-01-01T00:00:00Z"},
        ]
        key = "raw/oms/orders/ingestion_date=2026-01-01/run_id=c360-test-run/data.json"
        boto3.client("s3").put_object(Bucket="c360-bucket786", Key=key, Body="\n".join(json.dumps(r) for r in rows))
        out = silver_handler({**BASE, "source": "oms", "entity": "orders", "s3_raw_key": key}, None)
        assert (out["valid_count"], out["invalid_count"]) == (1, 1)
        assert out["silver_path"].endswith("data.parquet") and out["quarantine_path"]
        assert _selected_payload_fields("SilverGate") <= set(out)


# ── Quality ─────────────────────────────────────────────────────────────────

class TestQualityCheck:
    def _event(self, valid, invalid, path="s3://b/silver/x.parquet", **kw):
        return {**BASE, "source": "oms", "entity": "orders", "valid_count": valid, "invalid_count": invalid, "silver_path": path, **kw}

    def test_verdicts(self):
        assert quality_handler(self._event(100, 0), None)["verdict"] == "PASS"
        assert quality_handler(self._event(94, 6), None)["verdict"] == "WARN"
        assert quality_handler(self._event(0, 0, ""), None)["verdict"] == "NO_DATA"
        assert quality_handler(self._event(0, 0, "", dry_run=True), None)["verdict"] == "SKIPPED"

    def test_hard_threshold_raises(self):
        with pytest.raises(DataQualityGateFailed):
            quality_handler(self._event(89, 11), None)

    def test_valid_rows_without_silver_path_raise(self):
        with pytest.raises(DataQualityGateFailed):
            quality_handler(self._event(5, 0, ""), None)

    def test_output_matches_state_machine(self):
        assert _selected_payload_fields("QualityCheck") <= set(quality_handler(self._event(10, 0), None))


# ── Iceberg load ────────────────────────────────────────────────────────────

class TestIcebergLoad:
    def test_skip_paths_have_every_field(self):
        for event in ({**BASE, "valid_count": 0}, {**BASE, "valid_count": 5, "dry_run": True}):
            out = iceberg_handler({**event, "source": "oms", "entity": "orders", "silver_path": ""}, None)
            assert out["status"] == "skipped" and out["athena_executed"] is False
            assert _selected_payload_fields("IcebergLoad") <= set(out)

    @patch("src.lakehouse.iceberg_loader.IcebergLoader")
    def test_success_passes_silver_key(self, loader_cls):
        loader_cls.return_value.load_silver_run.return_value = {
            "status": "success", "athena_executed": True, "loaded_row_count": 7, "table_name": "oms_orders"}
        out = iceberg_handler({**BASE, "source": "oms", "entity": "orders", "valid_count": 7,
                               "silver_path": "s3://c360-bucket786/silver/oms/orders/x/data.parquet"}, None)
        assert out["loaded_row_count"] == 7 and out["athena_executed"] is True
        assert loader_cls.return_value.load_silver_run.call_args.kwargs["silver_key"] == "silver/oms/orders/x/data.parquet"

    @patch("src.lakehouse.iceberg_loader.IcebergLoader")
    def test_success_without_merge_is_an_error(self, loader_cls):
        loader_cls.return_value.load_silver_run.return_value = {"status": "success", "athena_executed": False}
        with pytest.raises(RuntimeError):
            iceberg_handler({**BASE, "source": "oms", "entity": "orders", "valid_count": 3, "silver_path": "s3://b/k"}, None)


# ── Reconcile ───────────────────────────────────────────────────────────────

def _ok(source, valid=10, **kw):
    return {"source": source, "entity": SOURCE_ENTITIES[source], "status": "succeeded", "source_mode": "synthetic",
            "records_ingested": valid, "valid_count": valid, "invalid_count": 0, "rejection_rate_pct": 0,
            "load_status": "success", "loaded_row_count": valid, "athena_executed": True, **kw}


def _failed(source):
    return {"source": source, "entity": SOURCE_ENTITIES[source], "status": "failed", "error": "States.TaskFailed", "cause": "boom"}


class TestReconcile:
    def test_all_passed(self):
        out = reconcile_handler({**BASE, "mandatory_sources": ["hubspot", "oms"], "branch_results": [_ok("hubspot"), _ok("oms")]}, None)
        assert out["reconciliation_status"] == "PASSED" and out["failed_sources"] == []
        assert _selected_payload_fields("ReconcileRun") <= set(out)

    def test_optional_failure_is_warning(self):
        out = reconcile_handler({**BASE, "mandatory_sources": ["oms"], "branch_results": [_ok("oms"), _failed("pim")]}, None)
        assert out["reconciliation_status"] == "PASSED_WITH_WARNINGS" and out["failed_sources"] == ["pim"]

    def test_mandatory_failure_raises(self):
        with pytest.raises(ReconciliationFailed):
            reconcile_handler({**BASE, "mandatory_sources": ["oms"], "branch_results": [_failed("oms"), _ok("pim")]}, None)

    def test_success_without_merge_counts_as_failure(self):
        with pytest.raises(ReconciliationFailed):
            reconcile_handler({**BASE, "mandatory_sources": ["oms"], "branch_results": [_ok("oms", athena_executed=False)]}, None)

    def test_missing_mandatory_branch_raises(self):
        with pytest.raises(ReconciliationFailed):
            reconcile_handler({**BASE, "mandatory_sources": ["oms"], "branch_results": [_ok("pim")]}, None)


# ── Notify ──────────────────────────────────────────────────────────────────

class TestNotify:
    def test_publishes_to_topic(self, monkeypatch):
        sns = boto3.client("sns")
        topic = sns.create_topic(Name="c360-staging-pipeline-alerts")["TopicArn"]
        monkeypatch.setenv("C360_SNS_ALERT_TOPIC_ARN", topic)
        out = notify_handler({**BASE, "severity": "WARNING", "failed_sources": ["pim"], "warnings": ["pim failed"]}, None)
        assert out["sns_message_id"]
        assert _selected_payload_fields("NotifyWarnings") <= set(out)

    def test_no_topic_configured(self, monkeypatch):
        monkeypatch.delenv("C360_SNS_ALERT_TOPIC_ARN", raising=False)
        assert notify_handler({**BASE}, None) == {"sns_message_id": ""}


# ── Maintenance ─────────────────────────────────────────────────────────────

class TestMaintenance:
    def test_dry_run_lists_registered_tables(self):
        out = maintenance_handler({"dry_run": True}, None)
        assert "oms_orders" in out["maintenance_summary"] and out["status"] == "dry_run"

    @patch("src.lakehouse.iceberg_maintenance.IcebergMaintenanceManager")
    def test_missing_table_is_not_a_failure_but_real_failure_raises(self, mgr_cls):
        mgr_cls.return_value.DEFAULT_RETENTION_DAYS = 7
        mgr_cls.return_value.run_maintenance_all.return_value = {"tables": {
            "oms_orders": {"success": True, "errors": []},
            "pim_products": {"success": False, "errors": ["Table x.pim_products does not exist in Glue catalog"]},
        }}
        assert maintenance_handler({}, None)["status"] == "complete"
        mgr_cls.return_value.run_maintenance_all.return_value["tables"]["oms_orders"] = {"success": False, "errors": ["OPTIMIZE failed"]}
        with pytest.raises(RuntimeError):
            maintenance_handler({}, None)


# ── End to end (local, synthetic, Athena mocked) ────────────────────────────

@patch("src.lakehouse.iceberg_loader.IcebergLoader")
def test_synthetic_run_through_every_handler(loader_cls, monkeypatch):
    """Walks the same path as the state machine for every source."""
    monkeypatch.setenv("ENVIRONMENT", "staging")
    monkeypatch.setenv("C360_SOURCE_MODES", json.dumps({s: "synthetic" for s in SOURCE_ENTITIES}))
    monkeypatch.setenv("C360_SYNTHETIC_INCLUDE_INVALID", "false")
    loader_cls.return_value.load_silver_run.side_effect = lambda **kw: {
        "status": "success", "athena_executed": True, "loaded_row_count": loaded[kw["source"]]}

    init = init_handler({"input": {}, "execution_name": "e2e"}, None)
    branches, loaded = [], {}
    for item in init["sources"]:
        ev = {**item, "run_id": init["run_id"], "environment": init["environment"], "dry_run": False}
        ing = ingest_handler(ev, None)
        sil = silver_handler({**ev, "s3_raw_key": ing["s3_raw_key"]}, None)
        q = quality_handler({**ev, "valid_count": sil["valid_count"], "invalid_count": sil["invalid_count"], "silver_path": sil["silver_path"]}, None)
        loaded[item["source"]] = sil["valid_count"]
        ld = iceberg_handler({**ev, "silver_path": sil["silver_path"], "valid_count": sil["valid_count"]}, None)
        assert q["verdict"] in ("PASS", "WARN"), (item, sil)
        branches.append({"source": item["source"], "entity": item["entity"], "status": "succeeded",
                         "source_mode": ing["source_mode"], "records_ingested": ing["records_ingested"],
                         "valid_count": sil["valid_count"], "invalid_count": sil["invalid_count"],
                         "rejection_rate_pct": q["rejection_rate_pct"], "load_status": ld["status"],
                         "loaded_row_count": ld["loaded_row_count"], "athena_executed": ld["athena_executed"]})
    rec = reconcile_handler({**BASE, "run_id": init["run_id"], "mandatory_sources": init["mandatory_sources"], "branch_results": branches}, None)
    assert rec["reconciliation_status"] in ("PASSED", "PASSED_WITH_WARNINGS")
    assert rec["failed_sources"] == []
    assert all(v["mode"] == "synthetic" for v in rec["summary"].values())
