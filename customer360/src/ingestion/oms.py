from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.config.settings import get_settings
from src.ingestion.base import BaseIngest
from src.ingestion.http_client import SourceHttpClient
from src.ingestion.source_mode import load_source_credentials
from src.ingestion.synthetic import include_invalid_synthetic


class OmsIngest(BaseIngest):
    """Orders. api = Shopify Admin GraphQL; mock_api = mock_sources/oms REST."""

    watermark_fields = ["updated_at", "order_ts"]

    def __init__(self, run_id: Optional[str] = None):
        super().__init__(source="oms", object_name="orders", run_id=run_id)
        self.settings = get_settings()
        self.base_url = getattr(self.settings, "OMS_BASE_URL", None) or "http://localhost:8001"
        self._http: Optional[SourceHttpClient] = None

    def _fetch_page(self, page: int = 1, page_size: int = 100, updated_after: Optional[str] = None) -> Dict[str, Any]:
        if self._http is None:
            token = load_source_credentials(self.source).get("api_token")
            self._http = SourceHttpClient(self.source, headers={"Authorization": f"Bearer {token}"} if token else {})
        params: Dict[str, Any] = {"page": page, "page_size": page_size}
        if updated_after:
            params["updated_after"] = updated_after
        return self._http.get_json(f"{self.base_url}/api/v1/orders", params=params)

    def _fetch_api(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        from src.ingestion.shopify import ShopifyClient

        client = ShopifyClient(load_source_credentials(self.source), source=self.source)
        self.base_url = f"https://{client.shop}"
        return client.fetch_orders(updated_after=watermark)

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
                "customer_email": f"customer{customer_id.split('-')[-1]}@synthetic.local",
                "source_type": "synthetic",
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
        ]
        invalid = [
            {"order_id": "OMS-INVALID-1", "customer_id": "", "order_ts": "bad-ts", "order_status": "unknown", "subtotal": "bad", "tax": 0, "shipping": 0, "discount": 0, "net_total": 0, "currency": "USD", "channel": "web", "updated_at": now.isoformat().replace("+00:00", "Z")},
            {"order_id": "OMS-DUP-1008", "customer_id": "C-1001", "order_ts": (now - timedelta(minutes=20)).isoformat().replace("+00:00", "Z"), "order_status": "paid", "subtotal": 170, "tax": 17, "shipping": 7, "discount": 0, "net_total": 194, "currency": "USD", "channel": "web", "updated_at": (now - timedelta(minutes=5)).isoformat().replace("+00:00", "Z")},
            {"order_id": "OMS-MISSING-CUST", "customer_id": "", "order_ts": (now - timedelta(minutes=4)).isoformat().replace("+00:00", "Z"), "order_status": "paid", "subtotal": 99, "tax": 9, "shipping": 4, "discount": 0, "net_total": 112, "currency": "USD", "channel": "web", "updated_at": (now - timedelta(minutes=4)).isoformat().replace("+00:00", "Z")},
        ]
        if include_invalid_synthetic():
            records = records + invalid
        if watermark:
            return [r for r in records if (r.get("updated_at") or r.get("order_ts") or "1970") >= watermark]
        return records
