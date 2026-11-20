"""
Lambda handler: iceberg_maintenance  (EventBridge Scheduler, weekly; not in the batch state machine)

Input:  {"environment": "staging", "dry_run": false, "retention_days": 7}  (all optional)
Output: {"status", "tables_processed", "failed_tables", "maintenance_summary"}

Runs OPTIMIZE + VACUUM on every table in TABLE_REGISTRY. Tables that do not
exist yet are reported, not failed. Raises if any existing table's
maintenance failed, so the Lambda error metric/alarm sees it.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from src.lakehouse.table_definitions import TABLE_REGISTRY

    tables = sorted({t.table_name for t in TABLE_REGISTRY.values()})
    if event.get("dry_run"):
        return {"status": "dry_run", "tables_processed": len(tables), "failed_tables": [],
                "maintenance_summary": {t: {"status": "dry_run"} for t in tables}}

    from src.lakehouse.iceberg_maintenance import IcebergMaintenanceManager

    manager = IcebergMaintenanceManager()
    retention = int(event.get("retention_days") or manager.DEFAULT_RETENTION_DAYS)
    summary = manager.run_maintenance_all(retention_days=retention)
    results = summary.get("tables", {})
    failed = [
        name for name, r in results.items()
        if isinstance(r, dict) and not r.get("success") and not any("does not exist" in e for e in r.get("errors", []))
    ]
    result = {"status": "failed" if failed else "complete", "tables_processed": len(tables),
              "failed_tables": failed, "maintenance_summary": results}
    logger.info("IcebergMaintenance: %s", result)
    if failed:
        raise RuntimeError(f"Iceberg maintenance failed for {failed}")
    return result
