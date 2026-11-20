from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.config.settings import get_settings
from src.errors import SourceModeError
from src.ingestion.base import BaseIngest
from src.ingestion.http_client import SourceHttpClient
from src.ingestion.source_mode import load_source_credentials


class LoyaltyIngest(BaseIngest):
    """Loyalty ledger. api and mock_api both use the REST contract
    GET {base_url}/api/v1/loyalty?page=&page_size=&updated_after= ; api mode reads
    base_url and api_token from the secret customer360/<env>/loyalty."""

    watermark_fields = ["updated_at", "earn_ts"]

    def __init__(self, run_id: Optional[str] = None):
        super().__init__(source="loyalty", object_name="loyalty", run_id=run_id)
        self.settings = get_settings()
        self.base_url = (
            getattr(self.settings, "LOYALTY_BASE_URL", None)
            or getattr(self.settings, "LOYALTY_API_BASE_URL", None)
            or "http://localhost:8003"
        )
        self._http: Optional[SourceHttpClient] = None

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

    def _client(self) -> SourceHttpClient:
        if self._http is None:
            creds = load_source_credentials(self.source)
            if self.source_mode == "api":
                if not creds.get("base_url"):
                    raise SourceModeError("loyalty api mode needs base_url in secret customer360/<env>/loyalty")
                self.base_url = creds["base_url"].rstrip("/")
            token = creds.get("api_token")
            self._http = SourceHttpClient(self.source, headers={"Authorization": f"Bearer {token}"} if token else {})
        return self._http

    def _fetch_page(self, page: int = 1, page_size: int = 100, updated_after: Optional[str] = None) -> Dict[str, Any]:
        client = self._client()
        params: Dict[str, Any] = {"page": page, "page_size": page_size}
        if updated_after:
            params["updated_after"] = updated_after
        return client.get_json(f"{self.base_url}/api/v1/loyalty", params=params)

    def _fetch_api(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        return [{**r, "source_type": r.get("source_type", "loyalty_api")} for r in self._fetch_mock_api(watermark)]
