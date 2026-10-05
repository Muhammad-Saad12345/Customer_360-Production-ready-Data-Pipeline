from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List

from src.checkpoint import CheckpointStore
from src.storage.s3_client import default_s3_client
from src.metadata.ingestion_metadata import IngestionMetadata, build_metadata, save_metadata
from src.utils.logging import setup_logging


class BaseIngest:
    def __init__(self, source: str, object_name: str):
        self.source = source
        self.object_name = object_name
        self.run_id = str(uuid.uuid4())
        self.ingestion_date = datetime.now(timezone.utc).date().isoformat()
        self.s3 = default_s3_client()
        self.logger = setup_logging(name=f"ingest.{source}.{object_name}")
        self.checkpoint_store = CheckpointStore()

    def log_event(self, message: str, **context: Any) -> None:
        payload = {"run_id": self.run_id, "source": self.source, "object": self.object_name}
        payload.update(context)
        self.logger.info(message, extra=payload)

    @staticmethod
    def _parse_datetime(value: str | None):
        if not value:
            return None
        try:
            if value.endswith("Z"):
                return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
            return datetime.fromisoformat(value).astimezone(timezone.utc)
        except ValueError:
            return None

    def resolve_watermark(
        self,
        default_watermark: str,
        lookback_minutes: int = 15,
        now_utc: str | None = None,
    ) -> str:
        checkpoint = self.checkpoint_store.get_or_create(
            source=self.source,
            entity=self.object_name,
            default_watermark=default_watermark,
        )

        last_watermark = checkpoint.get("last_successful_watermark") or default_watermark
        return last_watermark

    def get_lookback_minutes(self, default_minutes: int = 15) -> int:
        try:
            value = getattr(self, "settings", None)
            if value is not None:
                configured = getattr(value, "HUBSPOT_LOOKBACK_MINUTES", default_minutes)
                if configured is not None:
                    return int(configured)
        except Exception:
            pass
        return int(default_minutes)

    def compute_window_start(
        self,
        watermark: str,
        lookback_minutes: int = 15,
        now_utc: str | None = None,
        extraction_end: str | None = None,
    ) -> str:
        watermark_dt = self._parse_datetime(watermark)
        if not watermark_dt:
            return watermark

        start_dt = watermark_dt - timedelta(minutes=lookback_minutes)
        return start_dt.isoformat().replace("+00:00", "Z")

    def capture_extraction_end(self) -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def update_checkpoint(self, watermark: str, run_id: str | None = None, status: str = "success") -> dict:
        before = self.resolve_watermark(default_watermark=watermark, lookback_minutes=0)
        self.log_event(
            "checkpoint_update_start",
            watermark_before=before,
            watermark_after=watermark,
            status=status,
            checkpoint_source=self.source,
            checkpoint_entity=self.object_name,
        )
        updated = self.checkpoint_store.update(
            source=self.source,
            entity=self.object_name,
            watermark=watermark,
            run_id=run_id or self.run_id,
            status=status,
        )
        self.log_event(
            "checkpoint_update_complete",
            watermark_before=before,
            watermark_after=updated.get("last_successful_watermark"),
            run_id=run_id or self.run_id,
            checkpoint_status=updated.get("status"),
        )
        return updated

    def persist_raw(self, records: Iterable[Any]) -> str:
        key = self.s3.make_raw_key(self.source, self.object_name, self.ingestion_date, self.run_id)
        items = list(records)
        self.log_event(
            "s3_raw_write_start",
            s3_key=key,
            records_to_write=len(items),
            source=self.source,
            object_name=self.object_name,
        )
        try:
            self.s3.upload_jsonlines(key, items)
            self.log_event(
                "s3_raw_write_success",
                s3_key=key,
                records_written=len(items),
                source=self.source,
                object_name=self.object_name,
            )
            return key
        except Exception as exc:  # pragma: no cover - local development fallback
            message = str(exc)
            self.log_event(
                "s3_raw_write_failed",
                s3_key=key,
                error_message=message,
                records_to_write=len(items),
                source=self.source,
                object_name=self.object_name,
            )
            if any(token in message for token in ["InvalidAccessKeyId", "NoSuchBucket", "AccessDenied", "bucket", "credentials", "placeholder"]):
                self.logger.warning("S3 upload skipped because AWS credentials or bucket configuration is not valid for the current environment: %s", message)
                return ""
            raise

    def record_metadata(self, records_extracted: int, records_uploaded: int, status: str, source_url: str, file_path: str, error_message: str | None = None):
        meta = build_metadata(
            source_name=self.source,
            object_name=self.object_name,
            run_id=self.run_id,
            ingestion_timestamp=datetime.now(timezone.utc).isoformat(),
            records_extracted=records_extracted,
            records_uploaded=records_uploaded,
            status=status,
            source_url_or_api=source_url,
            schema_version="raw_v1",
            file_path=file_path,
            error_message=error_message,
        )
        self.log_event(
            "metadata_recorded",
            records_extracted=records_extracted,
            records_uploaded=records_uploaded,
            status=status,
            source_url=source_url,
            s3_key=file_path,
        )
        try:
            save_metadata(meta)
        except Exception as exc:  # pragma: no cover - local development fallback
            message = str(exc)
            self.log_event(
                "metadata_write_failed",
                s3_key=file_path,
                error_message=message,
            )
            if any(token in message for token in ["InvalidAccessKeyId", "NoSuchBucket", "AccessDenied", "bucket", "credentials", "placeholder"]):
                self.logger.warning("Metadata persistence skipped because AWS credentials or bucket configuration is not valid for the current environment: %s", message)
                return
            raise
