from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional
import uuid

from src.storage.s3_client import default_s3_client


@dataclass
class IngestionMetadata:
    source_name: str
    object_name: str
    run_id: str
    ingestion_timestamp: str
    records_extracted: int
    records_uploaded: int
    status: str
    source_url_or_api: str
    schema_version: str
    file_path: str
    error_message: Optional[str] = None


def build_metadata(**kwargs) -> IngestionMetadata:
    return IngestionMetadata(**kwargs)


def save_metadata(metadata: IngestionMetadata):
    s3 = default_s3_client()
    key = f"metadata/{metadata.source_name}/{metadata.object_name}/ingestion_date={metadata.ingestion_timestamp.split('T')[0]}/run_id={metadata.run_id}/metadata.json"
    s3.upload_json(key, asdict(metadata))
