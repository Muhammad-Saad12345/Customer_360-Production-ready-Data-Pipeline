"""Tests for Customer 360 Pipeline Health API (Phase 12)."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.api.health_api import app, _monitor


@pytest.fixture(autouse=True)
def _reset_monitor():
    """Clear monitor state before each test."""
    _monitor.reset()
    yield
    _monitor.reset()


client = TestClient(app)


class TestRootEndpoint:
    def test_root_returns_service_info(self):
        resp = client.get("/")
        assert resp.status_code == 200
        data = resp.json()
        assert data["service"] == "customer360-pipeline-health"
        assert data["status"] == "running"
        assert "timestamp" in data


class TestHealthEndpoint:
    def test_health_empty_returns_unknown(self):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["overall_status"] == "unknown"
        assert data["error_count"] == 0

    def test_health_with_ok_run(self):
        # Record a clean run
        client.post("/runs", json={
            "source": "hubspot",
            "entity": "contacts",
            "tier": "ingest",
            "status": "ok",
            "input_count": 100,
            "valid_count": 100,
            "invalid_count": 0,
        })
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["overall_status"] == "ok"

    def test_health_with_error(self):
        client.post("/runs", json={
            "source": "oms",
            "entity": "orders",
            "tier": "iceberg",
            "status": "error",
            "error_message": "Athena timeout",
        })
        resp = client.get("/health")
        data = resp.json()
        assert data["overall_status"] == "error"
        assert data["error_count"] >= 1


class TestHealthDetailEndpoint:
    def test_detail_returns_full_report(self):
        client.post("/runs", json={
            "source": "pim",
            "entity": "products",
            "tier": "silver",
            "status": "ok",
            "input_count": 50,
            "valid_count": 50,
            "invalid_count": 0,
        })
        resp = client.get("/health/detail")
        assert resp.status_code == 200
        data = resp.json()
        assert "run_results" in data
        assert "summary_by_source" in data
        assert "pim/products" in data["summary_by_source"]


class TestRunsEndpoints:
    def test_post_and_get_runs(self):
        client.post("/runs", json={
            "source": "loyalty",
            "entity": "loyalty",
            "tier": "ingest",
            "status": "ok",
            "input_count": 200,
            "valid_count": 195,
            "invalid_count": 5,
        })
        resp = client.get("/runs")
        assert resp.status_code == 200
        runs = resp.json()
        assert len(runs) >= 1
        assert runs[-1]["source"] == "loyalty"
        assert runs[-1]["invalid_count"] == 5

    def test_filter_by_source(self):
        client.post("/runs", json={"source": "hubspot", "entity": "contacts", "tier": "ingest"})
        client.post("/runs", json={"source": "oms", "entity": "orders", "tier": "ingest"})
        resp = client.get("/runs", params={"source": "hubspot"})
        runs = resp.json()
        assert all(r["source"] == "hubspot" for r in runs)

    def test_filter_by_tier(self):
        client.post("/runs", json={"source": "hubspot", "entity": "contacts", "tier": "ingest"})
        client.post("/runs", json={"source": "hubspot", "entity": "contacts", "tier": "silver"})
        resp = client.get("/runs", params={"tier": "silver"})
        runs = resp.json()
        assert all(r["tier"] == "silver" for r in runs)


class TestSourcesEndpoint:
    def test_sources_lists_all_active(self):
        resp = client.get("/sources")
        assert resp.status_code == 200
        data = resp.json()
        sources = data["sources"]
        source_names = [s["source"] for s in sources]
        assert "hubspot" in source_names
        assert "oms" in source_names
        assert "pim" in source_names
        assert "loyalty" in source_names
        assert "marketing" in source_names
        assert "support" in source_names
        assert len(sources) == 6
