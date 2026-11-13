"""
Lambda handler: initialize  (state InitializePipeline)

Input (built by the state machine):
  {"input": <execution input>, "execution_name": ..., "execution_arn": ..., "state_machine_arn": ...}

Execution input (all optional):
  {"sources": ["hubspot", "oms"], "mandatory_sources": ["hubspot"], "dry_run": false}

Output:
  {"run_id", "environment", "dry_run", "sources": [{"source", "entity"}...], "mandatory_sources": [...]}

run_id is derived from the execution name, so a retry or redrive of the same
execution keeps the same run_id (and therefore the same S3 partitions).
Refuses to start while another execution of this state machine is RUNNING.
"""
from __future__ import annotations

import logging
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

from src.pipeline_contract import DEFAULT_MANDATORY_SOURCES, SOURCE_ENTITIES

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ConcurrentRunError(Exception):
    """Another execution is already running; this one must not overlap it."""


def _run_id(execution_name: str) -> str:
    if execution_name:
        safe = re.sub(r"[^A-Za-z0-9_-]", "-", execution_name)[:64]
        return f"c360-{safe}"
    return f"c360-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def _check_no_other_running(state_machine_arn: str, execution_arn: str, client: Any = None) -> None:
    if not state_machine_arn or os.environ.get("C360_CONCURRENCY_GUARD", "true").lower() == "false":
        return
    if client is None:
        import boto3

        client = boto3.client("stepfunctions")
    response = client.list_executions(stateMachineArn=state_machine_arn, statusFilter="RUNNING", maxResults=10)
    others = [e["executionArn"] for e in response.get("executions", []) if e["executionArn"] != execution_arn]
    if others:
        raise ConcurrentRunError(f"Another execution is already running: {others[0]}")


def lambda_handler(event: Dict[str, Any], context: Any, sfn_client: Any = None) -> Dict[str, Any]:
    execution_input: Dict[str, Any] = event.get("input") or {}
    environment = os.environ.get("ENVIRONMENT", "staging")
    requested_env = execution_input.get("environment")
    if requested_env and requested_env != environment:
        raise ValueError(f"Execution input environment {requested_env!r} does not match this deployment ({environment!r})")

    enabled = [s.strip() for s in os.environ.get("C360_ENABLED_SOURCES", "").split(",") if s.strip()]
    names: List[str] = execution_input.get("sources") or enabled or list(SOURCE_ENTITIES)
    unknown = [n for n in names if n not in SOURCE_ENTITIES]
    if unknown:
        raise ValueError(f"Unknown sources {unknown}; valid: {sorted(SOURCE_ENTITIES)}")

    configured_mandatory = os.environ.get("C360_MANDATORY_SOURCES")
    mandatory = execution_input.get("mandatory_sources") or (
        [s.strip() for s in configured_mandatory.split(",") if s.strip()] if configured_mandatory else list(DEFAULT_MANDATORY_SOURCES)
    )
    mandatory = [s for s in mandatory if s in names]
    dry_run = bool(execution_input.get("dry_run", False))

    _check_no_other_running(event.get("state_machine_arn", ""), event.get("execution_arn", ""), sfn_client)

    result = {
        "run_id": _run_id(event.get("execution_name", "")),
        "environment": environment,
        "dry_run": dry_run,
        "sources": [{"source": n, "entity": SOURCE_ENTITIES[n]} for n in names],
        "mandatory_sources": mandatory,
    }
    logger.info("Initialize complete: %s", result)
    return result
