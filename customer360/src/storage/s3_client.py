from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any, Iterable

import boto3
from botocore.exceptions import ClientError

from src.config.settings import get_settings
from src.utils.retry import retry_on_exception


class S3Client:
    def __init__(self):
        self.settings = get_settings()
        self.s3 = boto3.client("s3", region_name=self.settings.AWS_REGION)
        self.bucket = self.settings.S3_BUCKET

    def bucket_exists(self) -> bool:
        try:
            self.s3.head_bucket(Bucket=self.bucket)
            return True
        except ClientError:
            return False

    @retry_on_exception(max_attempts=4)
    def upload_json(self, key: str, payload: Any) -> dict:
        body = json.dumps(payload, default=str, ensure_ascii=False)
        return self._put_object(key, body)

    @retry_on_exception(max_attempts=4)
    def upload_jsonlines(self, key: str, items: Iterable[Any]) -> dict:
        body = "\n".join(json.dumps(i, default=str, ensure_ascii=False) for i in items)
        return self._put_object(key, body)

    def _put_object(self, key: str, body: str) -> dict:
        resp = self.s3.put_object(Bucket=self.bucket, Key=key, Body=body.encode("utf-8"))
        return resp

    def make_raw_key(self, source: str, obj: str, ingestion_date: str, run_id: str) -> str:
        return f"raw/{source}/{obj}/ingestion_date={ingestion_date}/run_id={run_id}/data.json"


def default_s3_client() -> S3Client:
    return S3Client()
