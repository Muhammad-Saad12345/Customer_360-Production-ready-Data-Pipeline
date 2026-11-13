from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.config.settings import get_settings
from src.errors import SourceModeError
from src.ingestion.base import BaseIngest
from src.ingestion.http_client import SourceHttpClient
from src.ingestion.source_mode import load_source_credentials

_CHANNEL_MAP = {"email": "email", "web": "web", "web_form": "web", "chat": "chat", "native_messaging": "chat",
                "voice": "phone", "phone": "phone", "api": "api"}


def map_zendesk_ticket(ticket: Dict[str, Any]) -> Dict[str, Any]:
    """Zendesk ticket -> support contract. customer_id is the requester id."""
    via = ((ticket.get("via") or {}).get("channel") or "api").lower()
    satisfaction = (ticket.get("satisfaction_rating") or {}).get("score")
    csat = {"good": 5.0, "bad": 1.0}.get(str(satisfaction).lower()) if satisfaction else None
    status = (ticket.get("status") or "").lower()
    return {
        "ticket_id": f"zd-{ticket.get('id')}",
        "customer_id": f"zd-user-{ticket.get('requester_id')}" if ticket.get("requester_id") else "",
        "channel": _CHANNEL_MAP.get(via, "api"),
        "category": (ticket.get("type") or "question"),
        "priority": (ticket.get("priority") or "normal"),
        "status": status,
        "agent_id": str(ticket.get("assignee_id") or ""),
        "created_at": ticket.get("created_at"),
        "resolved_at": ticket.get("updated_at") if status in ("solved", "closed") else None,
        "updated_at": ticket.get("updated_at"),
        "csat_score": csat,
        "source_type": "zendesk",
    }


class ZendeskIngest(BaseIngest):
    """Zendesk tickets via the incremental cursor export (api mode).

    Secret customer360/<env>/support: {"subdomain": "...", "email": "...", "api_token": "..."}.
    Falls back to the legacy ZENDESK_* settings.
    """

    watermark_fields = ["updated_at", "created_at"]

    def __init__(self, run_id: Optional[str] = None):
        super().__init__(source="support", object_name="tickets", run_id=run_id)
        self.settings = get_settings()
        subdomain = getattr(self.settings, "ZENDESK_SUBDOMAIN", None)
        self.base_url = f"https://{subdomain}.zendesk.com/api/v2/" if subdomain else ""

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
                    "customer_id": f"C-{1001 + idx}",
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

    def _fetch_api(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        secret = load_source_credentials(self.source)
        subdomain = secret.get("subdomain") or getattr(self.settings, "ZENDESK_SUBDOMAIN", None)
        email = secret.get("email") or getattr(self.settings, "ZENDESK_EMAIL", None)
        token = secret.get("api_token") or getattr(self.settings, "ZENDESK_API_KEY", None) or getattr(self.settings, "ZENDESK_SECRET_KEY", None)
        if not subdomain or not email or not token:
            raise SourceModeError("support api mode needs Zendesk subdomain, email and api_token")
        self.base_url = f"https://{subdomain}.zendesk.com/api/v2/"
        http = SourceHttpClient(self.source, auth=(f"{email}/token", token))
        start = self._parse_datetime(watermark) if watermark else None
        start_time = int(start.timestamp()) if start else 0
        url: Optional[str] = f"{self.base_url}incremental/tickets/cursor.json"
        params: Optional[Dict[str, Any]] = {"start_time": start_time, "per_page": 1000}
        records: List[Dict[str, Any]] = []
        for _ in range(1000):
            payload = http.get_json(url, params=params)
            records.extend(map_zendesk_ticket(t) for t in payload.get("tickets") or [])
            if payload.get("end_of_stream") or not payload.get("after_url"):
                return records
            url, params = payload["after_url"], None
        raise SourceModeError("support: more than 1000 pages from Zendesk")
