from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.config.settings import get_settings
from src.errors import SourceModeError
from src.ingestion.base import BaseIngest
from src.ingestion.http_client import SourceHttpClient
from src.ingestion.source_mode import load_source_credentials


class MarketingIngest(BaseIngest):
    """Mailchimp audience members (api mode). No mock_api server exists for marketing.

    Secret customer360/<env>/marketing: {"api_key": "<key>-<dc>", "audience_id": "..."}.
    Falls back to the legacy MAILCHIMP_API_KEY / MAILCHIMP_AUDIENCE_ID settings.
    """

    watermark_fields = ["send_ts"]

    def __init__(self, run_id: Optional[str] = None):
        super().__init__(source="marketing", object_name="campaign_events", run_id=run_id)
        self.settings = get_settings()
        self.provider = (getattr(self.settings, "MARKETING_PROVIDER", None) or "mailchimp").lower()
        self._creds: Optional[Dict[str, Any]] = None
        self._http: Optional[SourceHttpClient] = None

    def _credentials(self) -> Dict[str, Any]:
        if self._creds is None:
            secret = load_source_credentials(self.source)
            self._creds = {
                "api_key": secret.get("api_key") or getattr(self.settings, "MAILCHIMP_API_KEY", None) or getattr(self.settings, "MARKETING_API_KEY", None),
                "audience_id": secret.get("audience_id") or getattr(self.settings, "MAILCHIMP_AUDIENCE_ID", None),
            }
        return self._creds

    def _audience_id(self) -> Optional[str]:
        return self._credentials().get("audience_id")

    def _synthetic_records(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        base_time = datetime.now(timezone.utc)
        records: List[Dict[str, Any]] = []
        for idx in range(3):
            ts = (base_time - timedelta(minutes=idx * 20)).isoformat()
            if watermark and ts <= watermark:
                continue
            records.append(
                {
                    "campaign_id": f"camp_{idx + 1}",
                    "customer_id": f"C-{1001 + idx}",
                    "channel": "email",
                    "send_ts": ts,
                    "open_ts": ts,
                    "click_ts": ts,
                    "audience_id": f"aud_{idx + 1}",
                    "offer_id": f"offer_{idx + 1}",
                    "member_id": f"member_{idx + 100}",
                    "email_address": f"customer{1001 + idx}@synthetic.local",
                    "status": "subscribed",
                    "source_type": "synthetic",
                }
            )
        return records

    def _mailchimp_member_to_record(self, member: Dict[str, Any]) -> Dict[str, Any]:
        merge_fields = member.get("merge_fields") or {}
        last_changed = member.get("last_changed") or member.get("timestamp_opt") or member.get("timestamp_signup") or datetime.now(timezone.utc).isoformat()
        customer_id = merge_fields.get("customer_id") or merge_fields.get("CUSTOMER_ID") or member.get("id")
        return {
            "campaign_id": self._audience_id(),
            "customer_id": str(customer_id),
            "channel": "email",
            "send_ts": last_changed,
            "open_ts": last_changed,
            "click_ts": None,
            "audience_id": self._audience_id(),
            "offer_id": None,
            "member_id": member.get("id"),
            "email_address": member.get("email_address"),
            "status": member.get("status"),
            "source_type": "mailchimp",
        }

    def _fetch_api(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        if self.provider != "mailchimp":
            raise SourceModeError(f"marketing api mode supports provider 'mailchimp', not {self.provider!r}")
        creds = self._credentials()
        api_key, audience_id = creds.get("api_key"), creds.get("audience_id")
        if not api_key or "-" not in api_key or not audience_id:
            raise SourceModeError("marketing api mode needs a Mailchimp api_key '<key>-<dc>' and audience_id")
        server_prefix = api_key.rsplit("-", 1)[-1]
        self.base_url = f"https://{server_prefix}.api.mailchimp.com/3.0"
        if self._http is None:
            self._http = SourceHttpClient(self.source, auth=("anystring", api_key))
        records: List[Dict[str, Any]] = []
        offset = 0
        for _ in range(1000):
            params: Dict[str, Any] = {"count": 1000, "offset": offset}
            if watermark:
                params["since_last_changed"] = watermark
            payload = self._http.get_json(f"{self.base_url}/lists/{audience_id}/members", params=params)
            members = payload.get("members") or []
            records.extend(self._mailchimp_member_to_record(m) for m in members)
            offset += len(members)
            if not members or offset >= int(payload.get("total_items") or 0):
                return self._filter_since(records, watermark)
        raise SourceModeError("marketing: more than 1000 pages from Mailchimp")
