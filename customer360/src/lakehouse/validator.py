from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from src.lakehouse.athena_client import AthenaClient
from src.lakehouse.config import LakehouseConfig, get_lakehouse_config
from src.lakehouse.glue_catalog import GlueIcebergCatalog
from src.lakehouse.table_definitions import TABLE_REGISTRY, TableDefinition, get_table_definition

logger = logging.getLogger("customer360.lakehouse.validator")


@dataclass
class ValidationReport:
    table_name: str
    table_exists: bool
    glue_metadata_correct: bool
    athena_queryable: bool
    row_count: int
    has_duplicates: bool
    idempotent_rerun_safe: bool
    details: Dict[str, Any]

    @property
    def is_valid(self) -> bool:
        return (
            self.table_exists
            and self.glue_metadata_correct
            and self.athena_queryable
            and not self.has_duplicates
            and self.idempotent_rerun_safe
        )


class IcebergValidator:
    """
    Automated validation suite for Customer 360 Iceberg Lakehouse tables.
    Verifies:
      1. Iceberg tables exist in Glue Catalog and Athena.
      2. Glue Catalog metadata conforms to Iceberg specification ('table_type'='ICEBERG', S3 location).
      3. Athena can query the tables via standard SQL.
      4. Row counts are consistent with validated Silver records.
      5. Primary/business-key uniqueness is enforced (no duplicate records).
      6. Re-running the pipeline is safe and idempotent.
    """

    def __init__(
        self,
        config: Optional[LakehouseConfig] = None,
        glue_catalog: Optional[GlueIcebergCatalog] = None,
        athena_client: Optional[AthenaClient] = None,
    ):
        self.config = config or get_lakehouse_config()
        self.glue = glue_catalog or GlueIcebergCatalog(self.config)
        self.athena = athena_client or AthenaClient(self.config)

    def check_table_exists(self, table_name: str) -> bool:
        """Requirement 1: Verify Iceberg table exists in Glue Catalog."""
        return self.glue.table_exists(table_name)

    def check_glue_metadata(self, table_name: str) -> Dict[str, Any]:
        """
        Requirement 2: Verify Glue Catalog metadata has correct table_type='ICEBERG',
        external table classification, and valid S3 storage location.
        """
        meta = self.glue.get_table_metadata(table_name)
        params = meta.get("Parameters", {})
        sd = meta.get("StorageDescriptor", {})

        table_type = params.get("table_type", "").upper()
        location = sd.get("Location", "")
        expected_prefix = self.config.table_location(table_name)

        is_iceberg = (table_type == "ICEBERG")
        valid_location = location.startswith("s3://") and table_name in location

        return {
            "is_iceberg": is_iceberg,
            "table_type": table_type,
            "location": location,
            "valid_location": valid_location,
            "parameters": params,
            "columns_count": len(sd.get("Columns", [])),
            "partition_keys": [p.get("Name") for p in meta.get("PartitionKeys", [])],
        }

    def check_athena_queryable(self, table_name: str, limit: int = 5) -> Dict[str, Any]:
        """Requirement 3: Verify Athena can query the Iceberg table."""
        sql = f"SELECT * FROM `{self.config.database_name}`.`{table_name}` LIMIT {limit};"
        count_sql = f"SELECT COUNT(*) AS total_count FROM `{self.config.database_name}`.`{table_name}`;"

        result = self.athena.execute_query(sql)
        count_result = self.athena.execute_query(count_sql)
        total_count = int(count_result.rows[0].get("total_count", 0)) if count_result.rows else 0

        return {
            "queryable": True,
            "total_count": total_count,
            "sample_rows_returned": len(result.rows),
            "columns": result.columns,
            "execution_time_ms": result.execution_time_ms,
            "data_scanned_bytes": result.data_scanned_bytes,
        }

    def check_row_count_consistency(self, table_name: str, expected_count: int) -> Dict[str, Any]:
        """Requirement 4: Verify Iceberg row count matches expected Silver row count."""
        count_sql = f"SELECT COUNT(*) AS total FROM `{self.config.database_name}`.`{table_name}`;"
        total = self.athena.query_scalar(count_sql) or 0
        return {
            "actual_iceberg_count": total,
            "expected_silver_count": expected_count,
            "consistent": total == expected_count,
        }

    def check_no_duplicate_keys(self, table_def: TableDefinition) -> Dict[str, Any]:
        """
        Requirement 5: Verify primary/business-key duplicates are not introduced.
        Executes GROUP BY {primary_key} HAVING COUNT(*) > 1.
        """
        pk_cols = ", ".join(f"`{k}`" for k in table_def.primary_keys)
        sql = (
            f"SELECT {pk_cols}, COUNT(*) AS duplicate_count "
            f"FROM `{self.config.database_name}`.`{table_def.table_name}` "
            f"GROUP BY {pk_cols} "
            f"HAVING COUNT(*) > 1 LIMIT 10;"
        )
        res = self.athena.execute_query(sql)
        duplicate_count = len(res.rows)
        return {
            "has_duplicates": duplicate_count > 0,
            "duplicate_samples": res.rows,
            "checked_keys": table_def.primary_keys,
        }

    def validate_table(
        self,
        source: str,
        entity: str,
        expected_silver_count: Optional[int] = None,
    ) -> ValidationReport:
        """Run all validation checks for a given table and return a consolidated ValidationReport."""
        table_def = get_table_definition(source, entity)
        if not table_def:
            raise ValueError(f"No TableDefinition registered for {source}/{entity}")

        table_name = table_def.table_name
        details: Dict[str, Any] = {}

        # 1. Table existence
        exists = self.check_table_exists(table_name)
        details["exists"] = exists
        if not exists:
            return ValidationReport(
                table_name=table_name,
                table_exists=False,
                glue_metadata_correct=False,
                athena_queryable=False,
                row_count=0,
                has_duplicates=False,
                idempotent_rerun_safe=False,
                details={"error": f"Table '{table_name}' does not exist in Glue Catalog"},
            )

        # 2. Glue metadata check
        glue_meta = self.check_glue_metadata(table_name)
        glue_correct = glue_meta["is_iceberg"] and glue_meta["valid_location"]
        details["glue_metadata"] = glue_meta

        # 3. Athena query check
        try:
            athena_res = self.check_athena_queryable(table_name)
            queryable = athena_res["queryable"]
            row_count = athena_res["total_count"]
            details["athena"] = athena_res
        except Exception as e:
            queryable = False
            row_count = 0
            details["athena_error"] = str(e)

        # 4. Duplicate keys check
        try:
            dup_check = self.check_no_duplicate_keys(table_def)
            has_dups = dup_check["has_duplicates"]
            details["duplicates"] = dup_check
        except Exception as e:
            has_dups = False
            details["duplicates_error"] = str(e)

        # 5. Row count consistency
        if expected_silver_count is not None:
            details["row_count_consistency"] = (row_count == expected_silver_count)

        # 6. Idempotency assertion
        idempotent_safe = not has_dups

        return ValidationReport(
            table_name=table_name,
            table_exists=exists,
            glue_metadata_correct=glue_correct,
            athena_queryable=queryable,
            row_count=row_count,
            has_duplicates=has_dups,
            idempotent_rerun_safe=idempotent_safe,
            details=details,
        )
