"""Persistent Checkpoint Store for Customer 360 Lakehouse.

Supports dual backends:
1. S3-backed persistent store (default in production and staging environments)
   Path: s3://<bucket>/checkpoints/<source>/<entity>.json
2. Local filesystem store (for unit tests, offline development, and dev mode)
   Path: <base_dir>/<source>/<entity>.json

Decision: S3 vs DynamoDB
-------------------------
For the Customer 360 batch and hourly pipelines, high-water mark updates occur
at the completion of each source extraction task. S3 provides:
- Strong read-after-write consistency (as of AWS S3 Dec 2020 update)
- Zero additional provisioned infrastructure/cost
- Versioning support for historical checkpoint auditability
- Natural integration with the S3 Lakehouse architecture
DynamoDB is reserved for sub-second, highly concurrent atomic distributed locking
if required in future real-time streaming iterations.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger("customer360.checkpoint")


class CheckpointStore:
    def __init__(
        self,
        base_dir: str | Path | None = None,
        bucket: Optional[str] = None,
        s3_client: Optional[Any] = None,
        storage_backend: Optional[str] = None,
    ):
        self.base_dir = Path(base_dir) if base_dir is not None else None
        self._s3_client = s3_client
        self._bucket = bucket

        # Determine backend: explicit override > base_dir passed (local) > ENVIRONMENT check
        if storage_backend:
            self.backend = storage_backend.lower()
        elif self.base_dir is not None:
            self.backend = "local"
        else:
            env = os.getenv("ENVIRONMENT", "dev").lower().strip()
            if env in ("prod", "production", "staging", "stage"):
                self.backend = "s3"
            else:
                self.backend = os.getenv("CHECKPOINT_BACKEND", "local").lower()

        if self.backend == "local":
            if self.base_dir is None:
                self.base_dir = Path(__file__).resolve().parent.parent / ".checkpoints"
            self.base_dir.mkdir(parents=True, exist_ok=True)
        else:
            if not self._bucket:
                self._bucket = os.getenv("S3_BUCKET") or "customer360-lakehouse-prod"

    def _get_s3_client(self) -> Any:
        if self._s3_client is not None:
            return self._s3_client
        import boto3
        region = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
        self._s3_client = boto3.client("s3", region_name=region)
        return self._s3_client

    def _s3_key_for(self, source: str, entity: str) -> str:
        prefix = os.getenv("CHECKPOINT_S3_PREFIX", "checkpoints").strip("/")
        return f"{prefix}/{source}/{entity}.json"

    def _local_path_for(self, source: str, entity: str) -> Path:
        assert self.base_dir is not None
        return self.base_dir / source / f"{entity}.json"

    def get_or_create(
        self,
        source: str,
        entity: str,
        default_watermark: Optional[str] = None,
    ) -> Dict[str, Any]:
        default_record = {
            "source": source,
            "entity": entity,
            "last_successful_watermark": default_watermark,
            "last_successful_run_id": None,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "status": "not_started",
        }

        if self.backend == "s3":
            s3 = self._get_s3_client()
            key = self._s3_key_for(source, entity)
            try:
                resp = s3.get_object(Bucket=self._bucket, Key=key)
                content = resp["Body"].read().decode("utf-8")
                return json.loads(content)
            except Exception:
                # Key does not exist or S3 read error; initialize default checkpoint
                logger.info("Initializing S3 checkpoint s3://%s/%s with default watermark %s", self._bucket, key, default_watermark)
                try:
                    s3.put_object(
                        Bucket=self._bucket,
                        Key=key,
                        Body=json.dumps(default_record, indent=2).encode("utf-8"),
                        ContentType="application/json",
                    )
                except Exception as write_err:
                    logger.warning("Failed to initialize S3 checkpoint: %s. Using in-memory fallback.", write_err)
                return default_record

        # Local backend
        path = self._local_path_for(source, entity)
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                pass

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(default_record, indent=2), encoding="utf-8")
        return default_record

    def update(
        self,
        source: str,
        entity: str,
        watermark: str,
        run_id: str,
        status: str = "success",
    ) -> Dict[str, Any]:
        record = self.get_or_create(source, entity, default_watermark=watermark)
        record.update(
            {
                "source": source,
                "entity": entity,
                "last_successful_watermark": watermark,
                "last_successful_run_id": run_id,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "status": status,
            }
        )

        if self.backend == "s3":
            s3 = self._get_s3_client()
            key = self._s3_key_for(source, entity)
            s3.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=json.dumps(record, indent=2).encode("utf-8"),
                ContentType="application/json",
            )
            logger.info("Updated S3 checkpoint s3://%s/%s -> watermark=%s, run_id=%s", self._bucket, key, watermark, run_id)
            return record

        path = self._local_path_for(source, entity)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(record, indent=2), encoding="utf-8")
        return record
