from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.config.settings import get_settings
from src.ingestion.base import BaseIngest
from src.ingestion.http_client import SourceHttpClient
from src.ingestion.source_mode import load_source_credentials
from src.ingestion.synthetic import include_invalid_synthetic


class PimIngest(BaseIngest):
    """Products. api = Shopify Admin GraphQL; mock_api = mock_sources/pim REST."""

    watermark_fields = ["updated_at", "launch_date"]

    def __init__(self, run_id: Optional[str] = None):
        super().__init__(source="pim", object_name="products", run_id=run_id)
        self.settings = get_settings()
        self.base_url = getattr(self.settings, "PIM_BASE_URL", None) or "http://localhost:8002"
        self._http: Optional[SourceHttpClient] = None

    def _fetch_page(self, page: int = 1, page_size: int = 100, updated_after: Optional[str] = None) -> Dict[str, Any]:
        if self._http is None:
            token = load_source_credentials(self.source).get("api_token")
            self._http = SourceHttpClient(self.source, headers={"Authorization": f"Bearer {token}"} if token else {})
        params: Dict[str, Any] = {"page": page, "page_size": page_size}
        if updated_after:
            params["updated_after"] = updated_after
        return self._http.get_json(f"{self.base_url}/api/v1/products", params=params)

    def _fetch_api(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        from src.ingestion.shopify import ShopifyClient

        client = ShopifyClient(load_source_credentials(self.source), source=self.source)
        self.base_url = f"https://{client.shop}"
        return client.fetch_products(updated_after=watermark)

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
        ]
        invalid = [
            {"product_id": "PIM-INVALID-1", "sku": "SKU-INVALID-1", "name": "Bad Product", "category": "Phones", "brand": "Acme", "price": -10, "cost": 50, "attributes": {"color": "black"}, "status": "invalid-status", "launch_date": "not-a-date", "updated_at": (now - timedelta(minutes=3)).isoformat().replace("+00:00", "Z")},
            {"product_id": "", "sku": "SKU-DUPLICATE-1", "name": "Missing Product ID", "category": "Phones", "brand": "Acme", "price": 120.0, "cost": 70, "attributes": {"color": "blue"}, "status": "active", "launch_date": (now - timedelta(days=10)).isoformat().replace("+00:00", "Z"), "updated_at": (now - timedelta(minutes=20)).isoformat().replace("+00:00", "Z")},
            {"product_id": "PIM-1006", "sku": "SKU-1001", "name": "Duplicate SKU", "category": "Phones", "brand": "Acme", "price": 159.0, "cost": 100.0, "attributes": {"color": "red"}, "status": "active", "launch_date": (now - timedelta(days=12)).isoformat().replace("+00:00", "Z"), "updated_at": (now - timedelta(minutes=15)).isoformat().replace("+00:00", "Z")},
        ]
        if include_invalid_synthetic():
            records = records + invalid
        if watermark:
            return [r for r in records if (r.get("updated_at") or r.get("launch_date") or "1970") >= watermark]
        return records
