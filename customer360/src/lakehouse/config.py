from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from src.config.settings import get_settings


@dataclass(frozen=True)
class LakehouseConfig:
    """Configuration for AWS-native Apache Iceberg Lakehouse layer."""
    database_name: str
    bucket: str
    region_name: str
    s3_prefix: str
    athena_workgroup: str
    athena_output_location: str
    table_format_version: str = "2"
    write_compression: str = "snappy"

    @property
    def warehouse_location(self) -> str:
        """Root S3 URI for all Iceberg tables."""
        return f"s3://{self.bucket}/{self.s3_prefix}"

    def table_location(self, table_name: str) -> str:
        """S3 URI for a specific Iceberg table directory."""
        return f"{self.warehouse_location}/{table_name}"


def get_lakehouse_config(
    database_name: Optional[str] = None,
    bucket: Optional[str] = None,
    region_name: Optional[str] = None,
    athena_workgroup: Optional[str] = None,
    athena_output_location: Optional[str] = None,
    s3_prefix: Optional[str] = None,
) -> LakehouseConfig:
    """Construct LakehouseConfig resolved from application settings and environment overrides."""
    settings = get_settings()

    resolved_bucket = bucket or settings.S3_BUCKET
    resolved_region = region_name or settings.AWS_REGION
    resolved_db = database_name or settings.ICEBERG_DATABASE
    resolved_wg = athena_workgroup or settings.ATHENA_WORKGROUP
    resolved_prefix = s3_prefix or settings.ICEBERG_S3_PREFIX
    resolved_output = (
        athena_output_location
        or settings.ATHENA_OUTPUT_LOCATION
        or f"s3://{resolved_bucket}/athena-results/"
    )

    return LakehouseConfig(
        database_name=resolved_db,
        bucket=resolved_bucket,
        region_name=resolved_region,
        s3_prefix=resolved_prefix,
        athena_workgroup=resolved_wg,
        athena_output_location=resolved_output,
    )
