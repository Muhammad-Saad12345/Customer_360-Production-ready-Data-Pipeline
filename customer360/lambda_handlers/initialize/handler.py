"""
Lambda handler: initialize
Step Functions state: Initialize

Generates a unique run_id, captures the execution window, and returns
the execution context that flows through all downstream states as
Step Functions input/output.

Input event (from EventBridge Scheduler or manual):
{
    "sources": ["hubspot", "oms", "pim", "loyalty", "marketing", "support"],  # optional
    "environment": "staging",  # optional, defaults to env var
    "dry_run": false
}

Output:
{
    "run_id": "c360-20261016T020000Z-abc123",
    "execution_start": "2026-10-16T02:00:00Z",
    "environment": "staging",
    "sources": ["hubspot", "oms", "pim", "loyalty", "marketing", "support"],
    "dry_run": false
}
"""
from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_DEFAULT_SOURCES = ["hubspot", "oms", "pim", "loyalty", "marketing", "support"]


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Entry point for the Initialize Lambda."""
    logger.info("Initialize: event=%s", event)

    environment = (
        event.get("environment")
        or os.environ.get("ENVIRONMENT", "staging")
    )
    sources: List[str] = event.get("sources", _DEFAULT_SOURCES)
    dry_run: bool = bool(event.get("dry_run", False))

    now_utc = datetime.now(timezone.utc)
    timestamp_str = now_utc.strftime("%Y%m%dT%H%M%SZ")
    short_id = uuid.uuid4().hex[:8]
    run_id = f"c360-{timestamp_str}-{short_id}"

    result = {
        "run_id": run_id,
        "execution_start": now_utc.isoformat().replace("+00:00", "Z"),
        "environment": environment,
        "sources": sources,
        "dry_run": dry_run,
    }

    logger.info(
        "Initialize complete: run_id=%s environment=%s sources=%s dry_run=%s",
        run_id, environment, sources, dry_run,
    )
    return result
