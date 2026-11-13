"""
Lambda handler: reconcile  (state ReconcileRun)

Input:  {"run_id", "environment", "dry_run", "mandatory_sources", "branch_results": [...]}
        Each branch result is either the SourceSucceeded summary
        {"source", "entity", "status": "succeeded", "source_mode", "records_ingested",
         "valid_count", "invalid_count", "rejection_rate_pct", "load_status",
         "loaded_row_count", "athena_executed"}
        or the SourceFailed summary {"source", "entity", "status": "failed", "error", "cause"}.

Output: {"reconciliation_status": PASSED | PASSED_WITH_WARNINGS, "failed_sources", "warnings", "summary"}

Raises ReconciliationFailed when a mandatory source failed: the execution
fails before dbt, and the EventBridge rule emails the alert.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

from src.errors import ReconciliationFailed

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _consistency_problem(branch: Dict[str, Any]) -> str:
    """A 'succeeded' branch whose numbers do not add up is treated as failed."""
    valid = int(branch.get("valid_count") or 0)
    loaded = int(branch.get("loaded_row_count") or 0)
    if valid > 0 and branch.get("load_status") == "success" and not branch.get("athena_executed"):
        return "load reported success but the Athena MERGE did not run"
    if valid > 0 and branch.get("load_status") == "skipped":
        return f"{valid} valid rows but the Iceberg load was skipped"
    if loaded > valid:
        return f"loaded {loaded} rows but only {valid} were valid"
    return ""


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    run_id = event["run_id"]
    dry_run = bool(event.get("dry_run", False))
    mandatory = set(event.get("mandatory_sources") or [])
    branches: List[Dict[str, Any]] = event.get("branch_results") or []

    failed: List[Dict[str, Any]] = []
    warnings: List[str] = []
    summary: Dict[str, Any] = {}
    records = 0

    for branch in branches:
        source = branch.get("source", "?")
        if branch.get("status") != "succeeded":
            failed.append({"source": source, "error": branch.get("error"), "cause": str(branch.get("cause"))[:500]})
            continue
        problem = _consistency_problem(branch)
        if problem:
            failed.append({"source": source, "error": "ReconciliationMismatch", "cause": problem})
            continue
        records += int(branch.get("records_ingested") or 0)
        summary[source] = {
            "mode": branch.get("source_mode"),
            "ingested": branch.get("records_ingested"),
            "valid": branch.get("valid_count"),
            "invalid": branch.get("invalid_count"),
            "loaded": branch.get("loaded_row_count"),
        }
        if not dry_run and int(branch.get("records_ingested") or 0) == 0:
            warnings.append(f"{source}: no new records in this run")
        if float(branch.get("rejection_rate_pct") or 0) > 0:
            warnings.append(f"{source}: {branch.get('rejection_rate_pct')}% of records quarantined")

    missing = mandatory - {b.get("source") for b in branches}
    for source in sorted(missing):
        failed.append({"source": source, "error": "MissingBranch", "cause": "no result for a mandatory source"})

    if not dry_run:
        from src.monitoring.cloudwatch import emit_run_summary

        emit_run_summary(len(summary), len(failed), records, run_id=run_id)

    mandatory_failed = [f for f in failed if f["source"] in mandatory]
    if mandatory_failed:
        names = ", ".join(f["source"] for f in mandatory_failed)
        raise ReconciliationFailed(f"run {run_id}: mandatory source(s) failed: {names}; details={mandatory_failed}")

    warnings.extend(f"{f['source']} failed (not mandatory): {f['error']}" for f in failed)
    status = "PASSED_WITH_WARNINGS" if failed or warnings else "PASSED"
    result = {
        "run_id": run_id,
        "reconciliation_status": status,
        "failed_sources": [f["source"] for f in failed],
        "warnings": warnings,
        "summary": summary,
    }
    logger.info("Reconcile complete: %s", result)
    return result
