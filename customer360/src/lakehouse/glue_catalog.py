from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from src.lakehouse.config import LakehouseConfig, get_lakehouse_config
from src.lakehouse.table_definitions import ColumnDefinition, TableDefinition

logger = logging.getLogger("customer360.lakehouse.glue")


class GlueIcebergCatalog:
    """
    AWS Glue Data Catalog manager for Apache Iceberg tables.
    Provides metadata creation, schema registration, and catalog verification.
    """

    def __init__(self, config: Optional[LakehouseConfig] = None, glue_client: Optional[Any] = None):
        self.config = config or get_lakehouse_config()
        self.client = glue_client or boto3.client("glue", region_name=self.config.region_name)

    def database_exists(self, database_name: Optional[str] = None) -> bool:
        """Check if Glue Catalog database exists."""
        db = database_name or self.config.database_name
        try:
            self.client.get_database(Name=db)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("EntityNotFoundException", "ResourceNotFoundException"):
                return False
            raise

    def create_database_if_not_exists(
        self,
        database_name: Optional[str] = None,
        description: str = "Customer 360 Lakehouse layer powered by Apache Iceberg",
    ) -> bool:
        """Create Glue Catalog database if it does not already exist. Returns True if created."""
        db = database_name or self.config.database_name
        if self.database_exists(db):
            logger.info("Glue database '%s' already exists.", db)
            return False

        try:
            self.client.create_database(
                DatabaseInput={
                    "Name": db,
                    "Description": description,
                    "LocationUri": self.config.warehouse_location,
                    "Parameters": {
                        "classification": "iceberg",
                        "owner": "customer360-platform",
                    },
                }
            )
            logger.info("Created Glue database '%s'.", db)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code == "AlreadyExistsException":
                return False
            raise

    def table_exists(self, table_name: str, database_name: Optional[str] = None) -> bool:
        """Check if table exists in Glue Catalog."""
        db = database_name or self.config.database_name
        try:
            self.client.get_table(DatabaseName=db, Name=table_name)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("EntityNotFoundException", "ResourceNotFoundException"):
                return False
            raise

    def get_table_metadata(self, table_name: str, database_name: Optional[str] = None) -> Dict[str, Any]:
        """Fetch table metadata from Glue Catalog."""
        db = database_name or self.config.database_name
        resp = self.client.get_table(DatabaseName=db, Name=table_name)
        return resp.get("Table", {})

    def is_iceberg_table(self, table_name: str, database_name: Optional[str] = None) -> bool:
        """Verify whether table has Iceberg table_type parameter in Glue."""
        meta = self.get_table_metadata(table_name, database_name=database_name)
        params = meta.get("Parameters", {})
        table_type = params.get("table_type", "").upper()
        return table_type == "ICEBERG"

    def list_tables(self, database_name: Optional[str] = None) -> List[str]:
        """List all table names in the Glue Catalog database."""
        db = database_name or self.config.database_name
        paginator = self.client.get_paginator("get_tables")
        table_names = []
        for page in paginator.paginate(DatabaseName=db):
            for t in page.get("TableList", []):
                table_names.append(t["Name"])
        return table_names

    def build_glue_columns(self, columns: List[ColumnDefinition]) -> List[Dict[str, str]]:
        """Map ColumnDefinition list to Glue Column format."""
        glue_cols = []
        for col in columns:
            glue_cols.append({
                "Name": col.name,
                "Type": col.data_type,
                "Comment": col.comment or "",
            })
        return glue_cols

    def create_iceberg_table_entry(
        self,
        table_def: TableDefinition,
        table_location: Optional[str] = None,
        database_name: Optional[str] = None,
    ) -> bool:
        """
        Register Iceberg table in Glue Catalog directly.
        Note: When created via Athena Engine v3 DDL, Athena registers this automatically.
        This method allows direct Glue Catalog registration when desired.
        """
        db = database_name or self.config.database_name
        loc = table_location or self.config.table_location(table_def.table_name)

        if self.table_exists(table_def.table_name, database_name=db):
            logger.info("Glue table '%s.%s' already exists.", db, table_def.table_name)
            return False

        table_input: Dict[str, Any] = {
            "Name": table_def.table_name,
            "Description": table_def.comment,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "table_type": "ICEBERG",
                "format": "parquet",
                "metadata_location": f"{loc}/metadata",
                **table_def.tblproperties,
            },
            "StorageDescriptor": {
                "Location": loc,
                "Columns": self.build_glue_columns(table_def.columns),
                "InputFormat": "org.apache.iceberg.mr.hive.HiveIcebergInputFormat",
                "OutputFormat": "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.iceberg.mr.hive.HiveIcebergSerDe",
                    "Parameters": {},
                },
            },
        }

        if table_def.partition_keys:
            table_input["PartitionKeys"] = [
                {"Name": pk, "Type": "date", "Comment": "Partition key"}
                for pk in table_def.partition_keys
            ]

        self.client.create_table(DatabaseName=db, TableInput=table_input)
        logger.info("Successfully registered Iceberg table '%s.%s' in Glue Catalog.", db, table_def.table_name)
        return True
