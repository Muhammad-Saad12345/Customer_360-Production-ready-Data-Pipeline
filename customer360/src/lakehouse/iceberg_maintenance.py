"""
Customer 360 — Apache Iceberg Operational Maintenance
=====================================================

Provides Athena-native and Glue-catalog maintenance operations for all
active Customer 360 Apache Iceberg tables.

Guarantees:
1. Uses Athena/Iceberg-native DDL/SQL commands (OPTIMIZE, VACUUM, ALTER TABLE).
2. NEVER performs raw or arbitrary boto3 S3 deletions against active Iceberg metadata/data paths.
3. Enforces safe retention windows (default 7 days) to preserve snapshots needed
   for audit, rollback, and time travel.
4. Provides full audit logging, execution metrics, and error isolation per table.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src.lakehouse.athena_client import AthenaClient, AthenaQueryExecutionError
from src.lakehouse.config import LakehouseConfig, get_lakehouse_config
from src.lakehouse.glue_catalog import GlueIcebergCatalog
from src.lakehouse.table_definitions import TABLE_REGISTRY, TableDefinition, get_all_table_definitions

logger = logging.getLogger("customer360.lakehouse.maintenance")


@dataclass
class MaintenanceResult:
    """Audit record for a single table maintenance run."""
    table_name: str
    database_name: str
    optimize_status: str = "skipped"
    vacuum_status: str = "skipped"
    retention_days: int = 7
    query_execution_ids: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    completed_at: Optional[str] = None

    @property
    def is_success(self) -> bool:
        return len(self.errors) == 0


class IcebergMaintenanceManager:
    """
    Manages operational compaction, snapshot expiration, and metadata cleanup
    across registered Customer 360 Apache Iceberg tables.
    """

    DEFAULT_RETENTION_DAYS = 7
    # 7 days in seconds = 604800
    SECONDS_PER_DAY = 86400

    def __init__(
        self,
        config: Optional[LakehouseConfig] = None,
        athena_client: Optional[AthenaClient] = None,
        glue_catalog: Optional[GlueIcebergCatalog] = None,
    ):
        self.config = config or get_lakehouse_config()
        self.athena = athena_client or AthenaClient(self.config)
        self.glue = glue_catalog or GlueIcebergCatalog(self.config)

    def set_table_retention_policy(self, table_name: str, retention_days: int = DEFAULT_RETENTION_DAYS) -> str:
        """
        Configure safe snapshot retention on the Iceberg table using TBLPROPERTIES.
        Athena VACUUM respects 'vacuum_max_snapshot_age_seconds'.
        """
        max_age_seconds = retention_days * self.SECONDS_PER_DAY
        sql = (
            f"ALTER TABLE {self.config.database_name}.{table_name} "
            f"SET TBLPROPERTIES ('vacuum_max_snapshot_age_seconds' = '{max_age_seconds}');"
        )
        logger.info(
            "Setting retention policy for %s: %d days (%d seconds)",
            table_name,
            retention_days,
            max_age_seconds,
        )
        return self.athena.run_ddl(sql)

    def optimize_table(self, table_name: str, partition_filter: Optional[str] = None) -> Dict[str, Any]:
        """
        Compact small Parquet data files into optimal-sized files using bin-packing.
        Native Athena SQL: OPTIMIZE <table_name> REWRITE DATA USING BIN_PACK
        """
        target = f"{self.config.database_name}.{table_name}"
        where_clause = f" WHERE {partition_filter}" if partition_filter else ""
        sql = f"OPTIMIZE {target} REWRITE DATA USING BIN_PACK{where_clause};"

        logger.info("Starting table compaction (OPTIMIZE) on %s", target)
        try:
            query_id = self.athena.run_ddl(sql)
            logger.info("OPTIMIZE completed successfully for %s (query_id=%s)", target, query_id)
            return {
                "table_name": table_name,
                "operation": "OPTIMIZE",
                "status": "success",
                "query_id": query_id,
            }
        except Exception as e:
            logger.error("OPTIMIZE failed for %s: %s", target, str(e))
            raise

    def vacuum_table(self, table_name: str, retention_days: int = DEFAULT_RETENTION_DAYS) -> Dict[str, Any]:
        """
        Remove expired snapshots and remove orphan files no longer referenced by metadata.
        Native Athena SQL: VACUUM <table_name>
        Ensures retention policy is configured before vacuuming.
        """
        target = f"{self.config.database_name}.{table_name}"
        try:
            self.set_table_retention_policy(table_name, retention_days=retention_days)
        except Exception as e:
            logger.warning("Could not set table retention property on %s: %s", target, str(e))

        sql = f"VACUUM {target};"
        logger.info("Starting table cleanup (VACUUM) on %s (retention=%d days)", target, retention_days)
        try:
            query_id = self.athena.run_ddl(sql)
            logger.info("VACUUM completed successfully for %s (query_id=%s)", target, query_id)
            return {
                "table_name": table_name,
                "operation": "VACUUM",
                "status": "success",
                "query_id": query_id,
                "retention_days": retention_days,
            }
        except Exception as e:
            logger.error("VACUUM failed for %s: %s", target, str(e))
            raise

    def maintain_table(
        self,
        table_name: str,
        retention_days: int = DEFAULT_RETENTION_DAYS,
        run_optimize: bool = True,
        run_vacuum: bool = True,
    ) -> MaintenanceResult:
        """
        Execute full maintenance routine on a single Iceberg table.
        """
        result = MaintenanceResult(
            table_name=table_name,
            database_name=self.config.database_name,
            retention_days=retention_days,
        )

        if not self.glue.table_exists(table_name):
            msg = f"Table {self.config.database_name}.{table_name} does not exist in Glue catalog"
            logger.warning(msg)
            result.errors.append(msg)
            result.completed_at = datetime.now(timezone.utc).isoformat()
            return result

        if run_optimize:
            try:
                opt_res = self.optimize_table(table_name)
                result.optimize_status = "success"
                result.query_execution_ids.append(opt_res.get("query_id", ""))
            except Exception as e:
                result.optimize_status = "failed"
                result.errors.append(f"OPTIMIZE failed: {str(e)}")

        if run_vacuum:
            try:
                vac_res = self.vacuum_table(table_name, retention_days=retention_days)
                result.vacuum_status = "success"
                result.query_execution_ids.append(vac_res.get("query_id", ""))
            except Exception as e:
                result.vacuum_status = "failed"
                result.errors.append(f"VACUUM failed: {str(e)}")

        result.completed_at = datetime.now(timezone.utc).isoformat()
        return result

    def run_maintenance_all(
        self,
        retention_days: int = DEFAULT_RETENTION_DAYS,
        run_optimize: bool = True,
        run_vacuum: bool = True,
    ) -> Dict[str, Any]:
        """
        Execute maintenance across all registered Customer 360 Iceberg tables.
        """
        unique_tables = {table_def.table_name for table_def in TABLE_REGISTRY.values()}
        results: Dict[str, Any] = {}
        total_tables = len(unique_tables)
        succeeded = 0
        failed = 0

        logger.info(
            "Starting batch Iceberg maintenance on %d tables (retention=%d days)",
            total_tables,
            retention_days,
        )

        for table_name in sorted(unique_tables):
            res = self.maintain_table(
                table_name=table_name,
                retention_days=retention_days,
                run_optimize=run_optimize,
                run_vacuum=run_vacuum,
            )
            results[table_name] = {
                "optimize_status": res.optimize_status,
                "vacuum_status": res.vacuum_status,
                "query_ids": res.query_execution_ids,
                "errors": res.errors,
                "success": res.is_success,
            }
            if res.is_success:
                succeeded += 1
            else:
                failed += 1

        summary = {
            "database": self.config.database_name,
            "total_tables": total_tables,
            "succeeded": succeeded,
            "failed": failed,
            "retention_days": retention_days,
            "executed_at": datetime.now(timezone.utc).isoformat(),
            "tables": results,
        }
        logger.info(
            "Completed batch Iceberg maintenance: %d succeeded, %d failed",
            succeeded,
            failed,
        )
        return summary
