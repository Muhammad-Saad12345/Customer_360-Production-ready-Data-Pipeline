"""
Lambda handler: iceberg_maintenance
Step Functions state: IcebergMaintenance

Runs OPTIMIZE (bin-packing compaction) and VACUUM (snapshot expiration)
on all active Customer 360 Iceberg tables via Athena SQL commands.
Extracted from customer360_batch._run_iceberg_maintenance().

Input event:
{
    "run_id": "c360-...",
    "environment": "staging",
    "tables": ["contacts", "orders", "products", "loyalty", "campaign_events", "support_tickets"],  # optional
    "dry_run": false
}

Output:
{
    "run_id": "c360-...",
    "status": "complete",
    "tables_processed": 6,
    "maintenance_summary": {...}
}
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_DEFAULT_TABLES = [
    "contacts",
    "orders",
    "products",
    "loyalty",
    "campaign_events",
    "support_tickets",
]


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Entry point for the Iceberg Maintenance Lambda."""
    run_id: str = event.get("run_id", "unknown-run")
    environment: str = event.get("environment", os.environ.get("ENVIRONMENT", "staging"))
    dry_run: bool = bool(event.get("dry_run", False))
    tables: List[str] = event.get("tables", _DEFAULT_TABLES)

    logger.info("IcebergMaintenance: run_id=%s tables=%s dry_run=%s", run_id, tables, dry_run)
    os.environ["ENVIRONMENT"] = environment

    if dry_run:
        logger.info("Dry run — skipping Athena OPTIMIZE/VACUUM operations")
        return {
            "run_id": run_id,
            "status": "dry_run",
            "tables_processed": len(tables),
            "maintenance_summary": {t: {"status": "dry_run"} for t in tables},
        }

    from src.lakehouse.iceberg_maintenance import IcebergMaintenanceManager

    manager = IcebergMaintenanceManager()
    results = {}

    for table in tables:
        try:
            res = manager.run_full_maintenance(table)
            results[table] = {
                "status": "success" if res.is_success else "error",
                "optimize_status": res.optimize_status,
                "vacuum_status": res.vacuum_status,
                "errors": res.errors,
            }
        except Exception as exc:  # noqa: BLE001
            # Non-fatal: single table maintenance failure should not fail entire batch
            logger.warning("Maintenance failed for table %s (non-fatal): %s", table, exc)
            results[table] = {"status": "error", "error": str(exc)}

    result = {
        "run_id": run_id,
        "status": "complete",
        "tables_processed": len(tables),
        "maintenance_summary": results,
    }
    logger.info("IcebergMaintenance complete: %s", result)
    return result
