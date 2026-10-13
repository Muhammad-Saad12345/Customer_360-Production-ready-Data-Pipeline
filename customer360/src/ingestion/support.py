from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from src.config.settings import get_settings
from src.ingestion.base import BaseIngest


class ZendeskIngest(BaseIngest):
    def __init__(self):
        super().__init__(source="support", object_name="tickets")
        self.settings = get_settings()
        self.base_url = (
            f"https://{self.settings.ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/"
            if self.settings.ZENDESK_SUBDOMAIN
            else "https://example.zendesk.com/api/v2/"
        )

    def _synthetic_records(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        base = datetime.now(timezone.utc)
        records: List[Dict[str, Any]] = []
        for idx in range(2):
            created = (base - timedelta(minutes=idx * 50)).isoformat()
            if watermark and created <= watermark:
                continue
            records.append(
                {
                    "ticket_id": f"ticket_{idx + 1}",
                    "customer_id": f"cust_{idx + 200}",
                    "channel": "email",
                    "category": "billing",
                    "priority": "normal",
                    "status": "open",
                    "agent_id": f"agent_{idx + 1}",
                    "created_at": created,
                    "resolved_at": None,
                    "csat_score": 4.5,
                    "source_type": "synthetic",
                }
            )
        return records

    def fetch_records(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        env = getattr(self.settings, "ENVIRONMENT", "dev").lower()
        allow_synthetic = os.environ.get("ALLOW_SYNTHETIC_FALLBACK", os.environ.get("MOCK_SOURCE_FALLBACK", "false")).lower() in ("true", "1", "yes")
        api_key = self.settings.ZENDESK_API_KEY or self.settings.ZENDESK_SECRET_KEY
        if not api_key or not self.settings.ZENDESK_EMAIL:
            if env in ("prod", "production", "staging", "stage") and not allow_synthetic:
                raise ValueError(f"Zendesk credentials missing in {env} environment.")
            self.logger.warning("Zendesk credentials missing in %s mode; using synthetic records.", env)
            return self._synthetic_records(watermark)

        endpoint = f"{self.base_url}incremental/tickets/cursor.json"
        params = {"page[size]": 100}
        if watermark:
            params["start_time"] = watermark
        try:
            response = requests.get(endpoint, params=params, auth=(self.settings.ZENDESK_EMAIL, api_key), timeout=30)
            response.raise_for_status()
            payload = response.json()
            records = payload.get("tickets", [])
            return [r for r in records if not watermark or (r.get("updated_at") or r.get("created_at") or "1970") >= watermark]
        except Exception as exc:
            if env in ("prod", "production", "staging", "stage") and not allow_synthetic:
                self.logger.error("Zendesk API call failed in %s environment: %s", env, exc)
                raise
            self.logger.warning("Zendesk API failed in %s mode; falling back to synthetic records: %s", env, exc)
            return self._synthetic_records(watermark)

    def run(self) -> None:
        watermark = self.resolve_watermark(default_watermark="1970-01-01T00:00:00Z")
        records = self.fetch_records(watermark)
        if not records:
            self.record_metadata(0, 0, "success", self.base_url, self.object_name)
            return

        key = self.persist_raw(records)
        self.update_checkpoint(records[-1].get("updated_at") or records[-1].get("created_at") or watermark, run_id=self.run_id)
        self.record_metadata(len(records), len(records), "success", self.base_url, key)
