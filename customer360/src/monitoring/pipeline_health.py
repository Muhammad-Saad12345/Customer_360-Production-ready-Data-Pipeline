"""Pipeline health monitoring for Customer 360.

Tracks per-run metrics across all 6 tiers (ingest, silver, quality, iceberg,
dbt, maintenance) and emits structured health reports suitable for alerting
or a dashboard.

Usage
-----
monitor = PipelineHealthMonitor()

# Record a successful run
with monitor.track("hubspot", "contacts", "ingest") as run:
    ingest()
    run.record_counts(input=500, valid=498, invalid=2)

# Or record manually
monitor.record(RunResult(source="oms", entity="orders", tier="silver", ...))

# Get health report
report = monitor.health_report()
print(report.summary())
"""
from __future__ import annotations

import json
import time
import traceback
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, Generator, List, Optional


# ---------------------------------------------------------------------------
# Enums & data classes
# ---------------------------------------------------------------------------

class HealthStatus(str, Enum):
    OK = "ok"
    WARNING = "warning"
    ERROR = "error"
    UNKNOWN = "unknown"


@dataclass
class RunResult:
    """Metrics captured for one pipeline tier/source execution."""

    source: str
    entity: str
    tier: str  # ingest | silver | quality | iceberg | dbt | maintenance
    status: HealthStatus = HealthStatus.OK
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    input_count: int = 0
    valid_count: int = 0
    invalid_count: int = 0
    error_message: Optional[str] = None
    run_id: Optional[str] = None
    extra: Dict = field(default_factory=dict)

    @property
    def rejection_rate(self) -> float:
        if self.input_count == 0:
            return 0.0
        return self.invalid_count / self.input_count

    def to_dict(self) -> Dict:
        d = asdict(self)
        d["status"] = self.status.value
        d["rejection_rate"] = round(self.rejection_rate, 4)
        return d


@dataclass
class HealthReport:
    """Aggregated health report across all active pipeline runs."""

    generated_at: str
    overall_status: HealthStatus
    run_results: List[RunResult]
    errors: List[Dict]
    warnings: List[Dict]
    summary_by_source: Dict[str, Dict]

    def to_dict(self) -> Dict:
        return {
            "generated_at": self.generated_at,
            "overall_status": self.overall_status.value,
            "run_results": [r.to_dict() for r in self.run_results],
            "errors": self.errors,
            "warnings": self.warnings,
            "summary_by_source": self.summary_by_source,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def summary(self) -> str:
        """Human-readable one-pager for logs/console output."""
        lines = [
            "=" * 60,
            f"PIPELINE HEALTH REPORT  [{self.generated_at}]",
            f"Overall status: {self.overall_status.value.upper()}",
            "-" * 60,
        ]
        for src, metrics in self.summary_by_source.items():
            lines.append(
                f"  {src:20s}  status={metrics.get('status', '?'):8s}  "
                f"runs={metrics.get('run_count', 0)}  "
                f"errors={metrics.get('error_count', 0)}  "
                f"rejections={metrics.get('rejection_rate_pct', 0):.1f}%"
            )
        if self.errors:
            lines.append("-" * 60)
            lines.append(f"Errors ({len(self.errors)}):")
            for e in self.errors:
                lines.append(f"  [{e.get('source')}/{e.get('entity')}] {e.get('error_message', '')}")
        lines.append("=" * 60)
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

# Rejection rate above this -> WARNING
REJECTION_RATE_WARN = 0.05   # 5 %
# Rejection rate above this -> ERROR
REJECTION_RATE_ERROR = 0.20  # 20 %
# Run duration above this (seconds) -> WARNING
DURATION_WARN_SECONDS = 600  # 10 min per tier


# ---------------------------------------------------------------------------
# Context manager helper
# ---------------------------------------------------------------------------

class _RunTracker:
    """Mutable in-flight run result; used as context manager."""

    def __init__(self, result: RunResult):
        self._result = result
        self._start_ts = time.monotonic()
        self._started_wall = datetime.now(timezone.utc).isoformat()

    def record_counts(
        self,
        *,
        input: int = 0,
        valid: int = 0,
        invalid: int = 0,
        run_id: Optional[str] = None,
    ) -> None:
        self._result.input_count = input
        self._result.valid_count = valid
        self._result.invalid_count = invalid
        if run_id is not None:
            self._result.run_id = run_id

    def set_extra(self, **kwargs) -> None:
        self._result.extra.update(kwargs)

    def _finalise(self, status: HealthStatus, error: Optional[str] = None):
        finished = datetime.now(timezone.utc).isoformat()
        self._result.started_at = self._started_wall
        self._result.finished_at = finished
        self._result.duration_seconds = round(time.monotonic() - self._start_ts, 3)
        self._result.status = status
        if error:
            self._result.error_message = error

    @property
    def result(self) -> RunResult:
        return self._result


# ---------------------------------------------------------------------------
# Main monitor
# ---------------------------------------------------------------------------

class PipelineHealthMonitor:
    """Collects run results and emits structured health reports.

    Thread-safe for single-process Airflow workers (appends to list and
    optionally persists to a JSONL file on disk).
    """

    ACTIVE_SOURCES = [
        ("hubspot", "contacts"),
        ("oms", "orders"),
        ("pim", "products"),
        ("loyalty", "loyalty"),
        ("marketing", "campaign_events"),
        ("support", "tickets"),
    ]

    def __init__(self, run_log_path: Optional[Path] = None):
        """
        Parameters
        ----------
        run_log_path:
            Optional path to a JSONL file where run results are appended.
            Useful for cross-run trend analysis without a database.
        """
        self._results: List[RunResult] = []
        self._run_log_path = run_log_path

    # ------------------------------------------------------------------
    # Recording API
    # ------------------------------------------------------------------

    def record(self, result: RunResult) -> None:
        """Append a completed RunResult."""
        self._results.append(result)
        if self._run_log_path:
            self._persist(result)

    @contextmanager
    def track(
        self,
        source: str,
        entity: str,
        tier: str,
        *,
        run_id: Optional[str] = None,
    ) -> Generator[_RunTracker, None, None]:
        """Context manager that captures timing and handles exceptions.

        Usage::

            with monitor.track("hubspot", "contacts", "ingest") as run:
                ingest()
                run.record_counts(input=500, valid=498, invalid=2)
        """
        result = RunResult(source=source, entity=entity, tier=tier, run_id=run_id)
        tracker = _RunTracker(result)
        try:
            yield tracker
            tracker._finalise(HealthStatus.OK)
        except Exception as exc:  # noqa: BLE001
            tracker._finalise(
                HealthStatus.ERROR,
                error=f"{type(exc).__name__}: {exc}",
            )
            self.record(tracker.result)
            raise
        finally:
            # Record if not already recorded (i.e. success path)
            if tracker.result not in self._results:
                self.record(tracker.result)

    # ------------------------------------------------------------------
    # Reporting API
    # ------------------------------------------------------------------

    def health_report(self) -> HealthReport:
        """Generate a health report from all recorded results."""
        now = datetime.now(timezone.utc).isoformat()
        errors: List[Dict] = []
        warnings: List[Dict] = []
        summary: Dict[str, Dict] = {}

        for r in self._results:
            key = f"{r.source}/{r.entity}"
            s = summary.setdefault(key, {
                "source": r.source,
                "entity": r.entity,
                "status": HealthStatus.OK.value,
                "run_count": 0,
                "error_count": 0,
                "total_input": 0,
                "total_invalid": 0,
                "rejection_rate_pct": 0.0,
                "last_duration_seconds": None,
            })
            s["run_count"] += 1
            s["total_input"] += r.input_count
            s["total_invalid"] += r.invalid_count
            if r.duration_seconds is not None:
                s["last_duration_seconds"] = r.duration_seconds

            if r.status == HealthStatus.ERROR:
                s["error_count"] += 1
                s["status"] = HealthStatus.ERROR.value
                errors.append(r.to_dict())
            elif r.rejection_rate >= REJECTION_RATE_ERROR:
                s["status"] = HealthStatus.ERROR.value
                errors.append({**r.to_dict(), "reason": "high_rejection_rate"})
            elif r.rejection_rate >= REJECTION_RATE_WARN:
                if s["status"] == HealthStatus.OK.value:
                    s["status"] = HealthStatus.WARNING.value
                warnings.append({**r.to_dict(), "reason": "elevated_rejection_rate"})
            elif r.duration_seconds and r.duration_seconds > DURATION_WARN_SECONDS:
                if s["status"] == HealthStatus.OK.value:
                    s["status"] = HealthStatus.WARNING.value
                warnings.append({**r.to_dict(), "reason": "slow_tier"})

        # Compute aggregate rejection rate per source/entity
        for s in summary.values():
            if s["total_input"] > 0:
                s["rejection_rate_pct"] = round(
                    s["total_invalid"] / s["total_input"] * 100, 2
                )

        # Overall status: worst across all entries
        if any(s["status"] == HealthStatus.ERROR.value for s in summary.values()):
            overall = HealthStatus.ERROR
        elif any(s["status"] == HealthStatus.WARNING.value for s in summary.values()):
            overall = HealthStatus.WARNING
        elif summary:
            overall = HealthStatus.OK
        else:
            overall = HealthStatus.UNKNOWN

        return HealthReport(
            generated_at=now,
            overall_status=overall,
            run_results=list(self._results),
            errors=errors,
            warnings=warnings,
            summary_by_source=summary,
        )

    def reset(self) -> None:
        """Clear in-memory results (typically called at start of each DAG run)."""
        self._results.clear()

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def _persist(self, result: RunResult) -> None:
        """Append result to JSONL log file."""
        try:
            self._run_log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._run_log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(result.to_dict(), default=str) + "\n")
        except OSError:
            # Non-fatal: monitoring must not block the pipeline.
            pass

    def load_run_log(self) -> List[RunResult]:
        """Load historical results from JSONL log (for trend analysis)."""
        if not self._run_log_path or not self._run_log_path.exists():
            return []
        results: List[RunResult] = []
        with self._run_log_path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                    d["status"] = HealthStatus(d.get("status", "unknown"))
                    d.pop("rejection_rate", None)
                    results.append(RunResult(**{k: v for k, v in d.items() if k in RunResult.__dataclass_fields__}))
                except (json.JSONDecodeError, TypeError, ValueError):
                    pass
        return results
