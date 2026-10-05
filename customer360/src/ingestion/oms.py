from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

import requests

from src.config.settings import get_settings
from src.ingestion.base import BaseIngest


class OmsIngest(BaseIngest):
    def __init__(self):
        super().__init__(source="oms", object_name="orders")
        self.settings = get_settings()
        self.base_url = self.settings.OMS_BASE_URL if hasattr(self.settings, "OMS_BASE_URL") else "http://localhost:8001"

    def _fetch_page(self, page: int = 1, page_size: int = 100, updated_after: Optional[str] = None) -> Dict[str, Any]:
        params = {"page": page, "page_size": page_size}
        if updated_after:
            params["updated_after"] = updated_after
        response = requests.get(f"{self.base_url}/api/v1/orders", params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def _synthetic_records(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        def _rec(order_id: str, customer_id: str, updated_at: datetime, status: str = "paid") -> Dict[str, Any]:
            subtotal = 150.0 if order_id.endswith("1") else 250.0
            tax = subtotal * 0.1
            shipping = 8.0 if status != "cancelled" else 0.0
            discount = 10.0 if customer_id.startswith("C") else 0.0
            net_total = subtotal + tax + shipping - discount
            return {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_ts": (updated_at - timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
                "order_status": status,
                "subtotal": subtotal,
                "tax": tax,
                "shipping": shipping,
                "discount": discount,
                "net_total": net_total,
                "currency": "USD",
                "channel": "web",
                "updated_at": updated_at.isoformat().replace("+00:00", "Z"),
            }
        records = [
            _rec("OMS-1001", "C-1001", now - timedelta(days=1, hours=2), "paid"),
            _rec("OMS-1002", "C-1002", now - timedelta(days=1), "shipped"),
            _rec("OMS-1003", "C-1001", now - timedelta(hours=20), "paid"),
            _rec("OMS-1004", "C-1003", now - timedelta(hours=12), "fulfilled"),
            _rec("OMS-1005", "C-1002", now - timedelta(hours=9), "cancelled"),
            _rec("OMS-1006", "C-1004", now - timedelta(days=2), "paid"),
            _rec("OMS-1007", "C-1005", now - timedelta(hours=2), "paid"),
            _rec("OMS-1008", "C-1001", now - timedelta(minutes=30), "paid"),
            _rec("OMS-1009", "C-1009", now - timedelta(minutes=10), "paid"),
            _rec("OMS-1010", "C-1006", now - timedelta(minutes=5), "paid"),
            {"order_id": "OMS-INVALID-1", "customer_id": "", "order_ts": "bad-ts", "order_status": "unknown", "subtotal": "bad", "tax": 0, "shipping": 0, "discount": 0, "net_total": 0, "currency": "USD", "channel": "web", "updated_at": now.isoformat().replace("+00:00", "Z")},
            {"order_id": "OMS-DUP-1008", "customer_id": "C-1001", "order_ts": (now - timedelta(minutes=20)).isoformat().replace("+00:00", "Z"), "order_status": "paid", "subtotal": 170, "tax": 17, "shipping": 7, "discount": 0, "net_total": 194, "currency": "USD", "channel": "web", "updated_at": (now - timedelta(minutes=5)).isoformat().replace("+00:00", "Z")},
            {"order_id": "OMS-MISSING-CUST", "customer_id": "", "order_ts": (now - timedelta(minutes=4)).isoformat().replace("+00:00", "Z"), "order_status": "paid", "subtotal": 99, "tax": 9, "shipping": 4, "discount": 0, "net_total": 112, "currency": "USD", "channel": "web", "updated_at": (now - timedelta(minutes=4)).isoformat().replace("+00:00", "Z")},
        ]
        if watermark:
            return [r for r in records if (r.get("updated_at") or r.get("order_ts") or "1970") >= watermark]
        return records

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
            return records
        except Exception as exc:
            env = getattr(self.settings, "ENVIRONMENT", "dev").lower()
            if env in ("prod", "production", "staging", "stage"):
                self.logger.error("OMS ingestion failed in %s environment: %s", env, exc)
                raise
            self.logger.warning("OMS API failed in %s mode; falling back to synthetic records: %s", env, exc)
            return self._synthetic_records(watermark)

    def run(self) -> None:
        watermark = self.resolve_watermark(default_watermark="1970-01-01T00:00:00Z")
        records = self.fetch_records(watermark=watermark)
        if not records:
            self.record_metadata(0, 0, "success", self.base_url, self.object_name)
            return
        s3_key = self.persist_raw(records)
        latest_watermark = max((r.get("updated_at") or r.get("order_ts") or watermark for r in records), default=watermark)
        self.update_checkpoint(latest_watermark, run_id=self.run_id)
        self.record_metadata(len(records), len(records), "success", self.base_url, s3_key)
