from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from src.config.settings import get_settings
from src.ingestion.base import BaseIngest


class LoyaltyIngest(BaseIngest):
    def __init__(self):
        super().__init__(source="loyalty", object_name="loyalty")
        self.settings = get_settings()
        self.base_url = (
            getattr(self.settings, "LOYALTY_BASE_URL", None)
            or getattr(self.settings, "LOYALTY_API_BASE_URL", None)
            or "http://localhost:8003"
        )

    def _synthetic_records(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        base = [
            {
                "loyalty_id": "LOY-1001",
                "customer_id": "C-1001",
                "tier": "gold",
                "points_balance": 3400.0,
                "earn_ts": (now - timedelta(days=5)).isoformat().replace("+00:00", "Z"),
                "redeem_ts": (now - timedelta(days=5, minutes=-30)).isoformat().replace("+00:00", "Z"),
                "benefit_code": "VIP10",
                "updated_at": (now - timedelta(days=5)).isoformat().replace("+00:00", "Z"),
                "source_type": "synthetic",
            },
            {
                "loyalty_id": "LOY-1002",
                "customer_id": "C-1002",
                "tier": "silver",
                "points_balance": 1200.0,
                "earn_ts": (now - timedelta(days=3)).isoformat().replace("+00:00", "Z"),
                "redeem_ts": (now - timedelta(days=3, minutes=-30)).isoformat().replace("+00:00", "Z"),
                "benefit_code": "STANDARD",
                "updated_at": (now - timedelta(days=3)).isoformat().replace("+00:00", "Z"),
                "source_type": "synthetic",
            },
        ]
        if watermark:
            return [r for r in base if r["updated_at"] >= watermark]
        return base

    def _fetch_page(self, page: int = 1, page_size: int = 100, updated_after: Optional[str] = None) -> Dict[str, Any]:
        params = {"page": page, "page_size": page_size}
        if updated_after:
            params["updated_after"] = updated_after
        response = requests.get(f"{self.base_url}/api/v1/loyalty", params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def fetch_records(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        try:
            records: List[Dict[str, Any]] = []
            page = 1
            while True:
                payload = self._fetch_page(page=page, page_size=100, updated_after=watermark)
                items = payload.get("items", [])
                records.extend(items)
                next_marker = payload.get("next_page")
                if next_marker is None:
                    next_marker = payload.get("next_cursor")
                if not next_marker:
                    break
                if isinstance(next_marker, int):
                    page = next_marker
                else:
                    page += 1

            normalized_records: List[Dict[str, Any]] = []
            for record in records:
                if "source_type" not in record:
                    record = {**record, "source_type": "synthetic"}
                normalized_records.append(record)
            return normalized_records
        except Exception as exc:
            env = getattr(self.settings, "ENVIRONMENT", "dev").lower()
            allow_synthetic = os.environ.get("ALLOW_SYNTHETIC_FALLBACK", os.environ.get("MOCK_SOURCE_FALLBACK", "false")).lower() in ("true", "1", "yes")
            if env in ("prod", "production", "staging", "stage") and not allow_synthetic:
                self.logger.error("Loyalty ingestion failed in %s environment: %s", env, exc)
                raise
            self.logger.warning("Loyalty API failed in %s mode; falling back to synthetic records: %s", env, exc)
            return self._synthetic_records(watermark)

    def run(self) -> None:
        watermark = self.resolve_watermark(default_watermark="1970-01-01T00:00:00Z")
        records = self.fetch_records(watermark=watermark)
        if not records:
            self.record_metadata(0, 0, "success", self.base_url, self.object_name)
            return
        s3_key = self.persist_raw(records)
        latest_watermark = max((r.get("updated_at") or r.get("earn_ts") or watermark for r in records), default=watermark)
        self.update_checkpoint(latest_watermark, run_id=self.run_id)
        self.record_metadata(len(records), len(records), "success", self.base_url, s3_key)
