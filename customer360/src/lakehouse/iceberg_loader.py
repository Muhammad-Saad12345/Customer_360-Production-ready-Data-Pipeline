from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import boto3

# pyarrow is packaged into the Lambda zip (scripts/package_artifacts.py).
# Installing packages at runtime from a Lambda is slow, needs internet access and
# is not reproducible, so a missing pyarrow is now an import error.
import pyarrow as pa
import pyarrow.parquet as pq

from src.lakehouse.athena_client import AthenaClient
from src.lakehouse.config import LakehouseConfig, get_lakehouse_config
from src.lakehouse.glue_catalog import GlueIcebergCatalog
from src.lakehouse.table_definitions import (
    TABLE_REGISTRY,
    ColumnDefinition,
    TableDefinition,
    generate_athena_add_column_ddl,
    generate_athena_create_table_ddl,
    generate_athena_merge_values_query,
    get_table_definition,
)

logger = logging.getLogger("customer360.lakehouse.loader")


class IcebergLoader:
    """
    Controlled conversion and loading process for transitioning validated Silver Parquet
    outputs into AWS-native Apache Iceberg tables registered in AWS Glue Data Catalog.

    Guarantees:
    1. Zero duplication of business keys on repeated runs (Strict Idempotency via MERGE).
    2. Automatic schema evolution handling for new validated attributes.
    3. Athena-accessible queryability via standard ANSI SQL.
    4. Row-count audit reconciliation against Silver parquet inputs.
    """

    def __init__(
        self,
        config: Optional[LakehouseConfig] = None,
        glue_catalog: Optional[GlueIcebergCatalog] = None,
        athena_client: Optional[AthenaClient] = None,
        s3_client: Optional[Any] = None,
    ):
        self.config = config or get_lakehouse_config()
        self.glue = glue_catalog or GlueIcebergCatalog(self.config)
        self.athena = athena_client or AthenaClient(self.config)
        self.s3 = s3_client or boto3.client("s3", region_name=self.config.region_name)

    def initialize_lakehouse(self) -> Dict[str, Any]:
        """
        Ensure Glue Database exists and all registered Customer 360 Iceberg tables
        are properly initialized with schema and metadata.
        """
        db_created = self.glue.create_database_if_not_exists()
        tables_created = []

        for (source, entity), table_def in TABLE_REGISTRY.items():
            if not self.glue.table_exists(table_def.table_name):
                created = self.ensure_iceberg_table(source, entity)
                if created:
                    tables_created.append(table_def.table_name)

        return {
            "database": self.config.database_name,
            "database_created": db_created,
            "tables_initialized": tables_created,
        }

    def ensure_iceberg_table(self, source: str, entity: str) -> bool:
        """
        Ensure the Iceberg table for the given source and entity exists in Glue and Athena.
        Uses Athena Engine v3 DDL to create the native Iceberg table on S3.
        """
        table_def = get_table_definition(source, entity)
        if not table_def:
            raise ValueError(f"No Iceberg TableDefinition registered for ({source}, {entity})")

        if self.glue.table_exists(table_def.table_name):
            logger.info("Iceberg table '%s' already exists.", table_def.table_name)
            return False

        loc = self.config.table_location(table_def.table_name)
        ddl = generate_athena_create_table_ddl(
            table_def=table_def,
            s3_location=loc,
            database_name=self.config.database_name,
        )

        try:
            self.athena.run_ddl(ddl)
            logger.info("Successfully created Iceberg table '%s' via Athena.", table_def.table_name)
            return True
        except Exception as e:
            logger.warning("Athena DDL execution failed: %s. Falling back to direct Glue catalog entry.", str(e))
            return self.glue.create_iceberg_table_entry(table_def=table_def, table_location=loc)

    def locate_silver_parquet_key(self, source: str, entity: str, run_id: str) -> Optional[str]:
        """Find S3 key for Silver parquet corresponding to a given run_id."""
        prefix = f"silver/{source}/{entity}/"
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.config.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item.get("Key", "")
                if f"run_id={run_id}" in key and key.endswith(".parquet"):
                    return key
        return None

    def read_silver_parquet_data(
        self, source: str, entity: str, run_id: str, silver_key: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], pa.Schema]:
        """Fetch and parse Silver parquet records and schema from S3.

        ``silver_key`` (passed from the SilverGate step) avoids listing the whole
        silver/<source>/<entity>/ history on every run.
        """
        key = silver_key or self.locate_silver_parquet_key(source, entity, run_id)
        if not key:
            logger.warning("No Silver parquet found for %s/%s with run_id=%s", source, entity, run_id)
            return [], pa.schema([])

        resp = self.s3.get_object(Bucket=self.config.bucket, Key=key)
        body = resp["Body"].read()
        table = pq.read_table(pa.BufferReader(body))
        rows = table.to_pylist()
        return rows, table.schema

    def handle_schema_evolution(
        self,
        table_def: TableDefinition,
        incoming_schema: pa.Schema,
    ) -> List[ColumnDefinition]:
        """
        Detect newly arrived columns in Silver that are not yet in the live Iceberg table.

        Queries the live Glue Data Catalog for existing column names (not the static
        Python table_def) to avoid ALTER TABLE ADD COLUMNS failures when a column was
        already added in a prior run but was not backported to the Python definition.
        """
        # Initialize live_cols with static table definition as base truth
        live_cols: Set[str] = set(table_def.column_names)
        try:
            glue_meta = self.glue.get_table_metadata(table_def.table_name)
            if isinstance(glue_meta, dict):
                for c in glue_meta.get("StorageDescriptor", {}).get("Columns", []):
                    if isinstance(c, dict) and "Name" in c:
                        live_cols.add(c["Name"])
                for k in glue_meta.get("PartitionKeys", []):
                    if isinstance(k, dict) and "Name" in k:
                        live_cols.add(k["Name"])
        except Exception as e:
            logger.debug("Could not fetch live Glue columns for %s, using static def: %s", table_def.table_name, e)

        new_cols: List[ColumnDefinition] = []

        type_mapping = {
            pa.string(): "string",
            pa.int64(): "bigint",
            pa.int32(): "int",
            pa.float64(): "double",
            pa.float32(): "float",
            pa.bool_(): "boolean",
            pa.date32(): "date",
            pa.timestamp("ms"): "timestamp",
            pa.timestamp("us"): "timestamp",
        }

        for field in incoming_schema:
            col_name = field.name
            if col_name not in live_cols and not col_name.startswith("_"):
                athena_type = type_mapping.get(field.type, "string")
                new_col = ColumnDefinition(
                    name=col_name,
                    data_type=athena_type,
                    comment="Evolved column detected from Silver ingestion",
                )
                new_cols.append(new_col)

        if new_cols:
            logger.info(
                "Schema evolution triggered for '%s': adding %d new columns: %s",
                table_def.table_name, len(new_cols), [c.name for c in new_cols],
            )
            try:
                alter_ddl = generate_athena_add_column_ddl(
                    table_name=table_def.table_name,
                    new_columns=new_cols,
                    database_name=self.config.database_name,
                )
                self.athena.run_ddl(alter_ddl)
                logger.info("Applied schema evolution DDL to '%s'", table_def.table_name)
            except Exception as e:
                logger.warning("Could not apply ALTER TABLE DDL via Athena: %s", str(e))

        return new_cols

    def deduplicate_records_by_primary_key(
        self,
        records: List[Dict[str, Any]],
        primary_keys: List[str],
    ) -> List[Dict[str, Any]]:
        """
        Deduplicate in-memory batch by primary key, preserving latest record.
        Ensures input to Iceberg MERGE / INSERT contains zero intra-batch duplicate keys.
        """
        seen: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        for r in records:
            key = tuple(str(r.get(pk, "")) for pk in primary_keys)
            seen[key] = r  # Overwrite with latest in batch
        return list(seen.values())

    def load_silver_run(
        self,
        source: str,
        entity: str,
        run_id: str,
        dry_run: bool = False,
        silver_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Controlled conversion and idempotent load of a validated Silver Parquet run into Iceberg.
        """
        table_def = get_table_definition(source, entity)
        if not table_def:
            raise ValueError(f"No Iceberg TableDefinition registered for ({source}, {entity})")

        # 1. Ensure Iceberg table exists
        self.ensure_iceberg_table(source, entity)

        # 2. Read Silver Parquet output
        silver_rows, parquet_schema = self.read_silver_parquet_data(source, entity, run_id, silver_key=silver_key)
        input_count = len(silver_rows)
        if input_count == 0:
            logger.info("No Silver records to promote for %s/%s run_id=%s", source, entity, run_id)
            return {
                "status": "skipped",
                "reason": "empty_silver_run",
                "source": source,
                "entity": entity,
                "table_name": table_def.table_name,
                "run_id": run_id,
                "silver_row_count": 0,
                "loaded_row_count": 0,
            }

        # 3. Detect and apply schema evolution
        evolved_cols = self.handle_schema_evolution(table_def, parquet_schema)

        # 4. Intra-batch deduplication on business key
        deduped_rows = self.deduplicate_records_by_primary_key(silver_rows, table_def.primary_keys)

        # 5. Extract partition date (ingestion_date)
        sample_timestamp = deduped_rows[0].get("ingestion_timestamp") or datetime.now(timezone.utc).isoformat()
        try:
            partition_date = datetime.fromisoformat(sample_timestamp.replace("Z", "+00:00")).date().isoformat()
        except Exception:
            partition_date = datetime.now(timezone.utc).date().isoformat()

        # Add ingestion_date to each row if missing
        for r in deduped_rows:
            if "ingestion_date" not in r or not r.get("ingestion_date"):
                r["ingestion_date"] = partition_date

        if dry_run:
            logger.info("Dry-run mode: successfully validated %d records for %s", len(deduped_rows), table_def.table_name)
            return {
                "status": "success",
                "mode": "dry_run",
                "table_name": table_def.table_name,
                "source": source,
                "entity": entity,
                "run_id": run_id,
                "silver_row_count": input_count,
                "deduped_row_count": len(deduped_rows),
                "evolved_columns": [c.name for c in evolved_cols],
                "primary_keys": table_def.primary_keys,
                "partition_date": partition_date,
            }

        # 6. Idempotent Load into Iceberg via inline VALUES MERGE INTO
        # Athena Engine v3 does not support CREATE EXTERNAL TABLE (Hive DDL), so we
        # use a VALUES-based MERGE which is fully self-contained and requires no
        # intermediate staging table or staging S3 object.
        #
        # SCHEMA EVOLUTION FIX: If new columns were detected and added via ALTER TABLE,
        # we must build an effective_table_def that includes those evolved columns so
        # their values from Silver are written to Iceberg (not silently dropped as NULL).
        # Without this fix, MERGE query is generated from original table_def.columns
        # only and new column values would always be NULL in Athena.
        #
        # Batch size limit: Athena SQL length limit is ~256 KB. For large Silver runs
        # we chunk the deduped rows into batches of at most BATCH_SIZE rows each.
        BATCH_SIZE = 500

        # Build effective column list using live Glue schema as the authoritative source.
        # This ensures Silver values for previously-evolved columns (added in prior runs
        # but absent from the static Python table_def) are also correctly written.
        static_col_names = {c.name for c in table_def.columns}
        extra_cols: List[ColumnDefinition] = []
        if evolved_cols:
            # evolved_cols are new columns detected AND added to Glue this run
            for c in evolved_cols:
                if c.name not in static_col_names:
                    extra_cols.append(c)
        # Also add any pre-existing Glue columns (from prior runs) that Silver data
        # has but the static Python table_def is missing.
        try:
            glue_meta_effective = self.glue.get_table_metadata(table_def.table_name)
            live_all_cols: Set[str] = set()
            if isinstance(glue_meta_effective, dict):
                cols_sd = glue_meta_effective.get("StorageDescriptor", {})
                if isinstance(cols_sd, dict):
                    cols_raw = cols_sd.get("Columns", [])
                    if isinstance(cols_raw, list):
                        for c in cols_raw:
                            if isinstance(c, dict) and "Name" in c:
                                live_all_cols.add(c["Name"])
            type_mapping_eff = {
                pa.string(): "string", pa.int64(): "bigint", pa.int32(): "int",
                pa.float64(): "double", pa.float32(): "float", pa.bool_(): "boolean",
                pa.date32(): "date", pa.timestamp("ms"): "timestamp",
                pa.timestamp("us"): "timestamp",
            }
            already_in_extra = {c.name for c in extra_cols}
            for field in parquet_schema:
                col_name = field.name
                if (col_name in live_all_cols
                        and col_name not in static_col_names
                        and col_name not in already_in_extra
                        and not col_name.startswith("_")):
                    athena_type = type_mapping_eff.get(field.type, "string")
                    extra_cols.append(ColumnDefinition(name=col_name, data_type=athena_type,
                                                       comment="Pre-existing evolved column in Glue"))
        except Exception as e:
            logger.warning("Could not query live Glue schema for effective table def: %s", str(e))

        if extra_cols:
            from dataclasses import replace as dc_replace
            effective_columns = list(table_def.columns) + extra_cols
            effective_table_def = dc_replace(table_def, columns=effective_columns)
            logger.info(
                "Effective schema includes %d extra columns beyond static def: %s",
                len(extra_cols), [c.name for c in extra_cols],
            )
        else:
            effective_table_def = table_def


        batches = [
            deduped_rows[i : i + BATCH_SIZE]
            for i in range(0, len(deduped_rows), BATCH_SIZE)
        ]

        completed_batches = 0
        try:
            for idx, batch in enumerate(batches):
                merge_query = generate_athena_merge_values_query(
                    table_def=effective_table_def,
                    records=batch,
                    database_name=self.config.database_name,
                )
                self.athena.execute_query(merge_query)
                completed_batches += 1
                logger.info(
                    "Iceberg MERGE batch %d/%d complete for '%s' run_id=%s (%d rows)",
                    idx + 1, len(batches), effective_table_def.table_name, run_id, len(batch),
                )
        except Exception as e:
            # Never report success when the MERGE failed (it used to be swallowed).
            # MERGE is idempotent on the primary key, so a retry of the whole step is safe.
            message = str(e)
            logger.error(
                "Athena MERGE INTO failed for '%s' run_id=%s after %d/%d batches: %s",
                effective_table_def.table_name, run_id, completed_batches, len(batches), message,
            )
            from src.errors import TransientError

            if any(token in message for token in ("TooManyRequests", "Throttl", "Rate exceeded", "SlowDown")):
                raise TransientError(f"Athena throttled during MERGE into {effective_table_def.table_name}: {message}") from e
            raise RuntimeError(f"Athena MERGE into {effective_table_def.table_name} failed: {message}") from e
        athena_success = True
        logger.info("Successfully loaded %d records into '%s' run_id=%s", len(deduped_rows), effective_table_def.table_name, run_id)

        return {
            "status": "success",
            "athena_executed": athena_success,
            "loaded_row_count": len(deduped_rows),
            "error_note": None,
            "table_name": table_def.table_name,
            "source": source,
            "entity": entity,
            "run_id": run_id,
            "silver_row_count": input_count,
            "deduped_row_count": len(deduped_rows),
            "evolved_columns": [c.name for c in evolved_cols],
            "primary_keys": table_def.primary_keys,
            "partition_date": partition_date,
            "idempotent": True,
        }
