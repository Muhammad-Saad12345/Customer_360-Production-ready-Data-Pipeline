from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from src.config.settings import get_settings
from src.ingestion.base import BaseIngest


class PimIngest(BaseIngest):
    def __init__(self):
        super().__init__(source="pim", object_name="products")
        self.settings = get_settings()
        self.base_url = self.settings.PIM_BASE_URL if hasattr(self.settings, "PIM_BASE_URL") else "http://localhost:8002"

    def _fetch_page(self, page: int = 1, page_size: int = 100, updated_after: Optional[str] = None) -> Dict[str, Any]:
        params = {"page": page, "page_size": page_size}
        if updated_after:
            params["updated_after"] = updated_after
        response = requests.get(f"{self.base_url}/api/v1/products", params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def _synthetic_records(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        def _rec(product_id: str, sku: str, name: str, category: str, brand: str, price: float, cost: float, status: str, launch_date: datetime, updated_at: datetime) -> Dict[str, Any]:
            return {
                "product_id": product_id,
                "sku": sku,
                "name": name,
                "category": category,
                "brand": brand,
                "price": price,
                "cost": cost,
                "attributes": {"color": "black", "size": "M"},
                "status": status,
                "launch_date": launch_date.isoformat().replace("+00:00", "Z"),
                "updated_at": updated_at.isoformat().replace("+00:00", "Z"),
            }
        records = [
            _rec("PIM-1001", "SKU-1001", "Aero Smartwatch", "Wearables", "Aero", 299.0, 180.0, "active", now - timedelta(days=120), now - timedelta(days=18)),
            _rec("PIM-1002", "SKU-1002", "Nova Headphones", "Audio", "Nova", 199.0, 120.0, "active", now - timedelta(days=90), now - timedelta(days=7)),
            _rec("PIM-1003", "SKU-1003", "Prime Laptop", "Computing", "Prime", 999.0, 750.0, "active", now - timedelta(days=150), now - timedelta(days=2)),
            _rec("PIM-1004", "SKU-1004", "Flex Keyboard", "Accessories", "Flex", 89.0, 40.0, "archived", now - timedelta(days=200), now - timedelta(hours=5)),
            _rec("PIM-1005", "SKU-1005", "Pulse Charger", "Accessories", "Pulse", 49.0, 28.0, "active", now - timedelta(days=30), now - timedelta(minutes=40)),
            {"product_id": "PIM-INVALID-1", "sku": "SKU-INVALID-1", "name": "Bad Product", "category": "Phones", "brand": "Acme", "price": -10, "cost": 50, "attributes": {"color": "black"}, "status": "invalid-status", "launch_date": "not-a-date", "updated_at": (now - timedelta(minutes=3)).isoformat().replace("+00:00", "Z")},
            {"product_id": "", "sku": "SKU-DUPLICATE-1", "name": "Missing Product ID", "category": "Phones", "brand": "Acme", "price": 120.0, "cost": 70, "attributes": {"color": "blue"}, "status": "active", "launch_date": (now - timedelta(days=10)).isoformat().replace("+00:00", "Z"), "updated_at": (now - timedelta(minutes=20)).isoformat().replace("+00:00", "Z")},
            {"product_id": "PIM-1006", "sku": "SKU-1001", "name": "Duplicate SKU", "category": "Phones", "brand": "Acme", "price": 159.0, "cost": 100.0, "attributes": {"color": "red"}, "status": "active", "launch_date": (now - timedelta(days=12)).isoformat().replace("+00:00", "Z"), "updated_at": (now - timedelta(minutes=15)).isoformat().replace("+00:00", "Z")},
        ]
        if watermark:
            return [r for r in records if (r.get("updated_at") or r.get("launch_date") or "1970") >= watermark]
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
                self.logger.error("PIM ingestion failed in %s environment: %s", env, exc)
                raise
            self.logger.warning("PIM API failed in %s mode; falling back to synthetic records: %s", env, exc)
            return self._synthetic_records(watermark)

    def run(self) -> None:
        watermark = self.resolve_watermark(default_watermark="1970-01-01T00:00:00Z")
        records = self.fetch_records(watermark=watermark)
        if not records:
            self.record_metadata(0, 0, "success", self.base_url, self.object_name)
            return
        s3_key = self.persist_raw(records)
        latest_watermark = max((r.get("updated_at") or r.get("launch_date") or watermark for r in records), default=watermark)
        self.update_checkpoint(latest_watermark, run_id=self.run_id)
        self.record_metadata(len(records), len(records), "success", self.base_url, s3_key)
