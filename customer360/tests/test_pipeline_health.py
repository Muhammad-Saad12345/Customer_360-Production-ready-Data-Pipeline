"""Tests for PipelineHealthMonitor (Phase 10 — Operational Observability)."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from src.monitoring.pipeline_health import (
    HealthStatus,
    PipelineHealthMonitor,
    RunResult,
    REJECTION_RATE_WARN,
    REJECTION_RATE_ERROR,
)


# ---------------------------------------------------------------------------
# RunResult unit tests
# ---------------------------------------------------------------------------

class TestRunResult:
    def test_rejection_rate_zero_input(self):
        r = RunResult(source="hubspot", entity="contacts", tier="silver")
        assert r.rejection_rate == 0.0

    def test_rejection_rate_correct(self):
        r = RunResult(
            source="oms", entity="orders", tier="silver",
            input_count=100, valid_count=80, invalid_count=20,
        )
        assert r.rejection_rate == pytest.approx(0.20)

    def test_to_dict_includes_rejection_rate(self):
        r = RunResult(
            source="pim", entity="products", tier="quality",
            input_count=50, valid_count=45, invalid_count=5,
        )
        d = r.to_dict()
        assert "rejection_rate" in d
        assert d["rejection_rate"] == pytest.approx(0.10, abs=0.001)

    def test_to_dict_status_is_string(self):
        r = RunResult(source="loyalty", entity="loyalty", tier="ingest")
        assert r.to_dict()["status"] == "ok"


# ---------------------------------------------------------------------------
# Health monitor — basic recording
# ---------------------------------------------------------------------------

class TestPipelineHealthMonitorRecording:
    def setup_method(self):
        self.monitor = PipelineHealthMonitor()

    def test_record_single_result(self):
        r = RunResult(source="hubspot", entity="contacts", tier="ingest")
        self.monitor.record(r)
        report = self.monitor.health_report()
        assert len(report.run_results) == 1

    def test_report_ok_status_on_clean_run(self):
        for source, entity in [("hubspot", "contacts"), ("oms", "orders")]:
            self.monitor.record(RunResult(
                source=source, entity=entity, tier="ingest",
                input_count=100, valid_count=100, invalid_count=0,
            ))
        report = self.monitor.health_report()
        assert report.overall_status == HealthStatus.OK
        assert report.errors == []
        assert report.warnings == []

    def test_reset_clears_results(self):
        self.monitor.record(RunResult(source="oms", entity="orders", tier="ingest"))
        self.monitor.reset()
        report = self.monitor.health_report()
        assert len(report.run_results) == 0
        assert report.overall_status == HealthStatus.UNKNOWN


# ---------------------------------------------------------------------------
# Health monitor — threshold escalation
# ---------------------------------------------------------------------------

class TestHealthStatusThresholds:
    def setup_method(self):
        self.monitor = PipelineHealthMonitor()

    def test_warning_on_elevated_rejection_rate(self):
        # 6% rejection rate (above REJECTION_RATE_WARN=5%)
        self.monitor.record(RunResult(
            source="pim", entity="products", tier="silver",
            input_count=100, valid_count=94, invalid_count=6,
        ))
        report = self.monitor.health_report()
        assert report.overall_status == HealthStatus.WARNING
        assert len(report.warnings) == 1

    def test_error_on_high_rejection_rate(self):
        # 25% rejection rate (above REJECTION_RATE_ERROR=20%)
        self.monitor.record(RunResult(
            source="marketing", entity="campaign_events", tier="silver",
            input_count=100, valid_count=75, invalid_count=25,
        ))
        report = self.monitor.health_report()
        assert report.overall_status == HealthStatus.ERROR
        assert len(report.errors) == 1

    def test_error_on_failed_run(self):
        self.monitor.record(RunResult(
            source="support", entity="tickets", tier="iceberg",
            status=HealthStatus.ERROR,
            error_message="ConnectionTimeout",
        ))
        report = self.monitor.health_report()
        assert report.overall_status == HealthStatus.ERROR
        assert "support/tickets" in report.summary_by_source
        assert report.summary_by_source["support/tickets"]["error_count"] == 1

    def test_worst_status_wins(self):
        # One clean, one error
        self.monitor.record(RunResult(
            source="hubspot", entity="contacts", tier="ingest",
            input_count=100, valid_count=100, invalid_count=0,
        ))
        self.monitor.record(RunResult(
            source="oms", entity="orders", tier="silver",
            status=HealthStatus.ERROR,
        ))
        assert self.monitor.health_report().overall_status == HealthStatus.ERROR


# ---------------------------------------------------------------------------
# Health monitor — track() context manager
# ---------------------------------------------------------------------------

class TestTrackContextManager:
    def setup_method(self):
        self.monitor = PipelineHealthMonitor()

    def test_track_records_success(self):
        with self.monitor.track("hubspot", "contacts", "ingest") as run:
            run.record_counts(input=200, valid=198, invalid=2)
        report = self.monitor.health_report()
        assert len(report.run_results) == 1
        r = report.run_results[0]
        assert r.status == HealthStatus.OK
        assert r.input_count == 200
        assert r.valid_count == 198
        assert r.invalid_count == 2

    def test_track_captures_exception(self):
        with pytest.raises(RuntimeError):
            with self.monitor.track("oms", "orders", "iceberg") as _:
                raise RuntimeError("Athena timeout")
        report = self.monitor.health_report()
        assert len(report.run_results) == 1
        r = report.run_results[0]
        assert r.status == HealthStatus.ERROR
        assert "Athena timeout" in r.error_message

    def test_track_records_timing(self):
        with self.monitor.track("pim", "products", "silver") as run:
            run.record_counts(input=10, valid=10, invalid=0)
        report = self.monitor.health_report()
        r = report.run_results[0]
        assert r.duration_seconds is not None
        assert r.duration_seconds >= 0.0
        assert r.started_at is not None
        assert r.finished_at is not None


# ---------------------------------------------------------------------------
# Health monitor — JSONL persistence
# ---------------------------------------------------------------------------

class TestJSONLPersistence:
    def test_results_persisted_to_jsonl(self, tmp_path):
        log_path = tmp_path / "run_log.jsonl"
        monitor = PipelineHealthMonitor(run_log_path=log_path)
        monitor.record(RunResult(
            source="loyalty", entity="loyalty", tier="ingest",
            input_count=50, valid_count=50, invalid_count=0,
        ))
        assert log_path.exists()
        lines = log_path.read_text().strip().splitlines()
        assert len(lines) == 1
        d = json.loads(lines[0])
        assert d["source"] == "loyalty"

    def test_load_run_log_round_trips(self, tmp_path):
        log_path = tmp_path / "run_log.jsonl"
        monitor = PipelineHealthMonitor(run_log_path=log_path)
        monitor.record(RunResult(
            source="hubspot", entity="contacts", tier="silver",
            input_count=100, valid_count=95, invalid_count=5,
            status=HealthStatus.OK,
        ))
        loaded = monitor.load_run_log()
        assert len(loaded) == 1
        assert loaded[0].source == "hubspot"
        assert loaded[0].valid_count == 95


# ---------------------------------------------------------------------------
# HealthReport.summary() formatting
# ---------------------------------------------------------------------------

class TestHealthReportSummary:
    def test_summary_contains_source_names(self):
        monitor = PipelineHealthMonitor()
        monitor.record(RunResult(
            source="hubspot", entity="contacts", tier="ingest",
            input_count=300, valid_count=295, invalid_count=5,
        ))
        report = monitor.health_report()
        summary_text = report.summary()
        assert "hubspot/contacts" in summary_text
        assert "overall_status" not in summary_text  # plain text, not JSON keys

    def test_to_json_is_valid_json(self):
        monitor = PipelineHealthMonitor()
        monitor.record(RunResult(
            source="oms", entity="orders", tier="quality",
            input_count=100, valid_count=100, invalid_count=0,
        ))
        json_str = monitor.health_report().to_json()
        parsed = json.loads(json_str)
        assert "overall_status" in parsed
        assert "summary_by_source" in parsed
