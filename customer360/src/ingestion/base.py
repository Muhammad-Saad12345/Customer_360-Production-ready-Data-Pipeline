from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from src.checkpoint import CheckpointStore
from src.errors import RawWriteError, SourceModeError
from src.ingestion.source_mode import resolve_source_mode
from src.metadata.ingestion_metadata import build_metadata, save_metadata
from src.storage.s3_client import default_s3_client
from src.utils.logging import setup_logging

DEFAULT_WATERMARK = "1970-01-01T00:00:00Z"
MAX_MOCK_API_PAGES = 1000


class BaseIngest:
    """Shared ingestion flow for every source.

    run():  resolve watermark -> fetch (by source mode) -> write raw to S3
            -> write metadata -> move checkpoint (only after the raw write
            succeeded) -> return a result dict for Step Functions.

    Subclasses provide ``_synthetic_records``; ``_fetch_page`` for mock_api;
    ``_fetch_api`` for api mode; and ``watermark_fields``.
    """

    watermark_fields: List[str] = ["updated_at"]

    def __init__(self, source: str, object_name: str, run_id: Optional[str] = None):
        self.source = source
        self.object_name = object_name
        self.run_id = run_id or str(uuid.uuid4())
        self.ingestion_date = datetime.now(timezone.utc).date().isoformat()
        self.s3 = default_s3_client()
        self.logger = setup_logging(name=f"ingest.{source}.{object_name}")
        self.checkpoint_store = CheckpointStore()
        self.source_mode = resolve_source_mode(source)
        self._last_result: Dict[str, Any] = {}

    @property
    def source_url(self) -> str:
        return getattr(self, "base_url", "") or self.source_mode

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

    def update_checkpoint(
        self,
        watermark: str,
        run_id: str | None = None,
        status: str = "success",
        extra: Optional[Dict[str, Any]] = None,
    ) -> dict:
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
            extra=extra,
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
        """Write raw records to S3. Raises RawWriteError on any failure.

        Earlier versions returned "" on AccessDenied/NoSuchBucket and the run
        reported success with nothing written; that is no longer possible.
        """
        key = self.s3.make_raw_key(self.source, self.object_name, self.ingestion_date, self.run_id)
        items = list(records)
        self.log_event("s3_raw_write_start", s3_key=key, records_to_write=len(items))
        try:
            self.s3.upload_jsonlines(key, items)
        except Exception as exc:
            self.log_event("s3_raw_write_failed", s3_key=key, error_type=type(exc).__name__, records_to_write=len(items))
            raise RawWriteError(f"{self.source}/{self.object_name}: raw write to {key} failed: {exc}") from exc
        self.log_event("s3_raw_write_success", s3_key=key, records_written=len(items))
        return key

    def record_metadata(
        self,
        records_extracted: int,
        records_uploaded: int,
        status: str,
        source_url: str,
        file_path: str,
        error_message: str | None = None,
    ):
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
            source_mode=self.source_mode,
            s3_key=file_path,
        )
        save_metadata(meta)

    # ── fetching ──────────────────────────────────────────────────────────

    def record_watermark(self, record: Dict[str, Any]) -> Optional[str]:
        for field in self.watermark_fields:
            value = record.get(field) if isinstance(record, dict) else None
            if value:
                return str(value)
        return None

    def latest_watermark(self, records: List[Dict[str, Any]], default: str) -> str:
        latest: Optional[datetime] = None
        latest_raw = default
        for record in records:
            raw = self.record_watermark(record)
            parsed = self._parse_datetime(raw)
            if parsed and (latest is None or parsed > latest):
                latest, latest_raw = parsed, parsed.isoformat().replace("+00:00", "Z")
        return latest_raw

    def _filter_since(self, records: List[Dict[str, Any]], watermark: Optional[str]) -> List[Dict[str, Any]]:
        start = self._parse_datetime(watermark) if watermark else None
        if start is None:
            return records
        kept = []
        for record in records:
            value = self._parse_datetime(self.record_watermark(record))
            if value is None or value >= start:
                kept.append(record)
        return kept

    def _synthetic_records(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        raise SourceModeError(f"{self.source}: synthetic mode is not implemented")

    def _fetch_page(self, page: int = 1, page_size: int = 100, updated_after: Optional[str] = None) -> Dict[str, Any]:
        raise SourceModeError(f"{self.source}: mock_api mode is not implemented")

    def _fetch_api(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        raise SourceModeError(f"{self.source}: api mode is not implemented")

    def _fetch_mock_api(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        """Paginate the mock_sources REST contract: {items, next_page|next_cursor}."""
        records: List[Dict[str, Any]] = []
        page = 1
        for _ in range(MAX_MOCK_API_PAGES):
            payload = self._fetch_page(page=page, page_size=100, updated_after=watermark)
            records.extend(payload.get("items", []))
            next_marker = payload.get("next_page")
            if next_marker is None:
                next_marker = payload.get("next_cursor")
            if not next_marker:
                return records
            page = next_marker if isinstance(next_marker, int) else page + 1
        raise SourceModeError(f"{self.source}: mock API returned more than {MAX_MOCK_API_PAGES} pages")

    def _dataset_records(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        """Read JSON / JSON-lines files from s3://<bucket>/datasets/<source>/<entity>/."""
        client = self.s3.s3
        prefix = f"datasets/{self.source}/{self.object_name}/"
        records: List[Dict[str, Any]] = []
        found = False
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.s3.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item["Key"]
                if not (key.endswith(".json") or key.endswith(".jsonl")):
                    continue
                found = True
                body = client.get_object(Bucket=self.s3.bucket, Key=key)["Body"].read().decode("utf-8")
                stripped = body.strip()
                if not stripped:
                    continue
                if stripped.startswith("["):
                    records.extend(json.loads(stripped))
                else:
                    records.extend(json.loads(line) for line in stripped.splitlines() if line.strip())
        if not found:
            raise SourceModeError(f"{self.source}: dataset mode but no .json/.jsonl files under s3://{self.s3.bucket}/{prefix}")
        return self._filter_since(records, watermark)

    def fetch_records(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch by the configured source mode. No fallback between modes."""
        mode = self.source_mode
        self.log_event("fetch_start", source_mode=mode, watermark=watermark)
        if mode == "synthetic":
            return self._synthetic_records(watermark)
        if mode == "dataset":
            return self._dataset_records(watermark)
        if mode == "mock_api":
            return self._fetch_mock_api(watermark)
        if mode == "api":
            return self._fetch_api(watermark)
        raise SourceModeError(f"{self.source}: unknown source mode {mode!r}")

    def prepare_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Hook for de-duplication etc. Every record is tagged with its source mode."""
        return [{**r, "source_mode": self.source_mode} if isinstance(r, dict) else r for r in records]

    # ── the flow ──────────────────────────────────────────────────────────

    def _result(self, status: str, records: int, key: str, watermark_before: str, watermark_after: str) -> Dict[str, Any]:
        self._last_result = {
            "status": status,
            "records_ingested": records,
            "s3_raw_key": key,
            "source_mode": self.source_mode,
            "watermark_before": watermark_before,
            "watermark_after": watermark_after,
        }
        return self._last_result

    def run(self) -> Dict[str, Any]:
        checkpoint = self.checkpoint_store.get_or_create(
            source=self.source, entity=self.object_name, default_watermark=DEFAULT_WATERMARK
        )
        # A Step Functions retry of an attempt that already finished: return the
        # same answer instead of re-fetching from the moved watermark and
        # overwriting this run's raw file with fewer rows.
        if checkpoint.get("last_successful_run_id") == self.run_id and checkpoint.get("last_raw_key") is not None:
            self.log_event("ingest_already_completed_for_run", s3_key=checkpoint.get("last_raw_key"))
            wm = checkpoint.get("last_successful_watermark") or DEFAULT_WATERMARK
            return self._result(
                "success" if checkpoint.get("last_raw_key") else "no_new_records",
                int(checkpoint.get("last_record_count") or 0),
                checkpoint.get("last_raw_key") or "",
                checkpoint.get("watermark_before") or wm,
                wm,
            )

        watermark = checkpoint.get("last_successful_watermark") or DEFAULT_WATERMARK
        lookback = timedelta(minutes=int(os.environ.get("C360_LOOKBACK_MINUTES", "15")))
        query_from = watermark
        if lookback and self._parse_datetime(watermark):
            query_from = (self._parse_datetime(watermark) - lookback).isoformat().replace("+00:00", "Z")

        records = self.prepare_records(self.fetch_records(watermark=query_from))
        if not records:
            self.record_metadata(0, 0, "success", self.source_url, "")
            return self._result("no_new_records", 0, "", watermark, watermark)

        key = self.persist_raw(records)  # raises; checkpoint below never moves on failure
        new_watermark = self.latest_watermark(records, default=watermark)
        if (self._parse_datetime(new_watermark) or datetime.min.replace(tzinfo=timezone.utc)) < (
            self._parse_datetime(watermark) or datetime.min.replace(tzinfo=timezone.utc)
        ):
            new_watermark = watermark  # never move a watermark backwards
        self.record_metadata(len(records), len(records), "success", self.source_url, key)
        self.update_checkpoint(
            new_watermark,
            run_id=self.run_id,
            extra={"last_raw_key": key, "last_record_count": len(records), "watermark_before": watermark, "source_mode": self.source_mode},
        )
        return self._result("success", len(records), key, watermark, new_watermark)
