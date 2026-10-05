"""Customer 360 Pipeline Health API.

A lightweight FastAPI service exposing pipeline operational status, health
checks, and run history. Designed to run alongside Airflow for BI/ops
dashboards, alerting webhooks, and Slack/email integration.

Start:
    uvicorn src.api.health_api:app --port 8360
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.monitoring.pipeline_health import (
    HealthStatus,
    PipelineHealthMonitor,
    RunResult,
)

# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Customer 360 Pipeline Health",
    description="Operational observability endpoints for the Customer 360 Data Lakehouse",
    version="1.0.0",
)

# Singleton monitor — loads historical JSONL log if available
_LOG_PATH = Path(os.environ.get(
    "C360_RUN_LOG",
    str(Path(__file__).resolve().parents[2] / "logs" / "pipeline_run_log.jsonl"),
))
_monitor = PipelineHealthMonitor(run_log_path=_LOG_PATH)


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class HealthSummary(BaseModel):
    generated_at: str
    overall_status: str
    summary_by_source: Dict[str, Any]
    error_count: int
    warning_count: int


class RunResultSchema(BaseModel):
    source: str
    entity: str
    tier: str
    status: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    input_count: int = 0
    valid_count: int = 0
    invalid_count: int = 0
    rejection_rate: float = 0.0
    error_message: Optional[str] = None
    run_id: Optional[str] = None


class RecordRunRequest(BaseModel):
    source: str
    entity: str
    tier: str
    status: str = "ok"
    input_count: int = 0
    valid_count: int = 0
    invalid_count: int = 0
    error_message: Optional[str] = None
    run_id: Optional[str] = None
    duration_seconds: Optional[float] = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/", tags=["meta"])
def root():
    return {
        "service": "customer360-pipeline-health",
        "status": "running",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/health", response_model=HealthSummary, tags=["health"])
def health():
    """Return current pipeline health summary."""
    report = _monitor.health_report()
    return HealthSummary(
        generated_at=report.generated_at,
        overall_status=report.overall_status.value,
        summary_by_source=report.summary_by_source,
        error_count=len(report.errors),
        warning_count=len(report.warnings),
    )


@app.get("/health/detail", tags=["health"])
def health_detail():
    """Return full health report with per-run detail."""
    report = _monitor.health_report()
    return report.to_dict()


@app.get("/runs", response_model=List[RunResultSchema], tags=["runs"])
def list_runs(
    source: Optional[str] = None,
    tier: Optional[str] = None,
    limit: int = 100,
):
    """List recorded run results with optional filtering."""
    results = list(_monitor._results)
    if source:
        results = [r for r in results if r.source == source]
    if tier:
        results = [r for r in results if r.tier == tier]
    results = results[-limit:]
    return [
        RunResultSchema(
            source=r.source,
            entity=r.entity,
            tier=r.tier,
            status=r.status.value,
            started_at=r.started_at,
            finished_at=r.finished_at,
            duration_seconds=r.duration_seconds,
            input_count=r.input_count,
            valid_count=r.valid_count,
            invalid_count=r.invalid_count,
            rejection_rate=round(r.rejection_rate, 4),
            error_message=r.error_message,
            run_id=r.run_id,
        )
        for r in results
    ]


@app.post("/runs", tags=["runs"])
def record_run(req: RecordRunRequest):
    """Record a pipeline run result (called by Airflow callbacks)."""
    result = RunResult(
        source=req.source,
        entity=req.entity,
        tier=req.tier,
        status=HealthStatus(req.status),
        input_count=req.input_count,
        valid_count=req.valid_count,
        invalid_count=req.invalid_count,
        error_message=req.error_message,
        run_id=req.run_id,
        duration_seconds=req.duration_seconds,
        started_at=datetime.now(timezone.utc).isoformat(),
        finished_at=datetime.now(timezone.utc).isoformat(),
    )
    _monitor.record(result)
    return {"status": "recorded", "source": req.source, "entity": req.entity, "tier": req.tier}


@app.get("/runs/history", response_model=List[RunResultSchema], tags=["runs"])
def run_history(limit: int = 200):
    """Load historical results from the persisted JSONL log."""
    historical = _monitor.load_run_log()
    historical = historical[-limit:]
    return [
        RunResultSchema(
            source=r.source,
            entity=r.entity,
            tier=r.tier,
            status=r.status.value,
            started_at=r.started_at,
            finished_at=r.finished_at,
            duration_seconds=r.duration_seconds,
            input_count=r.input_count,
            valid_count=r.valid_count,
            invalid_count=r.invalid_count,
            rejection_rate=round(r.rejection_rate, 4),
            error_message=r.error_message,
            run_id=r.run_id,
        )
        for r in historical
    ]


@app.get("/sources", tags=["meta"])
def active_sources():
    """List all active source systems and their entities."""
    return {
        "sources": [
            {"source": s, "entity": e}
            for s, e in PipelineHealthMonitor.ACTIVE_SOURCES
        ]
    }
