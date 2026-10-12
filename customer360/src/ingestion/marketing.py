from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from src.config.settings import get_settings
from src.ingestion.base import BaseIngest


class MarketingIngest(BaseIngest):
    def __init__(self):
        super().__init__(source="marketing", object_name="campaign_events")
        self.settings = get_settings()
        self.provider = (self.settings.MARKETING_PROVIDER or "synthetic").lower()

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
                    "customer_id": f"cust_{idx + 100}",
                    "channel": "email",
                    "send_ts": ts,
                    "open_ts": ts,
                    "click_ts": ts,
                    "audience_id": f"aud_{idx + 1}",
                    "offer_id": f"offer_{idx + 1}",
                    "member_id": f"member_{idx + 100}",
                    "email_address": f"cust{idx + 100}@example.com",
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
            "campaign_id": self.settings.MAILCHIMP_AUDIENCE_ID,
            "customer_id": str(customer_id),
            "channel": "email",
            "send_ts": last_changed,
            "open_ts": last_changed,
            "click_ts": None,
            "audience_id": self.settings.MAILCHIMP_AUDIENCE_ID,
            "offer_id": None,
            "member_id": member.get("id"),
            "email_address": member.get("email_address"),
            "status": member.get("status"),
            "source_type": "mailchimp",
        }

    def fetch_records(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        if self.provider == "mailchimp":
            api_key = self.settings.MAILCHIMP_API_KEY or self.settings.MARKETING_API_KEY
            audience_id = self.settings.MAILCHIMP_AUDIENCE_ID
            # Mailchimp API keys encode the data-centre as suffix: "<key>-<dc>"
            server_prefix = api_key.split("-")[-1] if api_key and "-" in api_key else None
            env = getattr(self.settings, "ENVIRONMENT", "dev").lower()
            if not api_key or not server_prefix or not audience_id:
                if env in ("prod", "production", "staging", "stage"):
                    raise ValueError(f"Mailchimp credentials missing in {env} environment.")
                return self._synthetic_records(watermark)

            base_url = f"https://{server_prefix}.api.mailchimp.com/3.0"
            records: List[Dict[str, Any]] = []
            offset = 0
            try:
                while True:
                    response = requests.get(
                        f"{base_url}/lists/{audience_id}/members",
                        params={"count": 100, "offset": offset},
                        auth=("user", api_key),
                        timeout=30,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    members = payload.get("members") or []
                    for member in members:
                        member_ts = member.get("last_changed") or member.get("timestamp_opt") or member.get("timestamp_signup") or "1970-01-01T00:00:00Z"
                        if watermark and member_ts <= watermark:
                            continue
                        records.append(self._mailchimp_member_to_record(member))

                    total_items = payload.get("total_items") or 0
                    if len(members) < 100 or offset + len(members) >= total_items:
                        break
                    offset += len(members)

                return records
            except Exception as exc:
                if env in ("prod", "production", "staging", "stage"):
                    self.logger.error("Mailchimp API call failed in %s environment: %s", env, exc)
                    raise
                self.logger.warning("Mailchimp API failed in %s mode; falling back to synthetic records: %s", env, exc)
                return self._synthetic_records(watermark)

        env = getattr(self.settings, "ENVIRONMENT", "dev").lower()
        allow_synthetic = os.environ.get("ALLOW_SYNTHETIC_FALLBACK", os.environ.get("MOCK_SOURCE_FALLBACK", "false")).lower() in ("true", "1", "yes")
        if env in ("prod", "production", "staging", "stage") and not allow_synthetic:
            raise ValueError(f"Marketing provider '{self.provider}' with synthetic fallback is not permitted in {env} environment.")

        return self._synthetic_records(watermark)

    def run(self) -> None:
        watermark = self.resolve_watermark(default_watermark="1970-01-01T00:00:00Z")
        records = self.fetch_records(watermark)
        if not records:
            self.logger.info("No new marketing events found for this window")
            self.record_metadata(0, 0, "success", "marketing-provider", self.object_name)
            return

        key = self.persist_raw(records)
        self.update_checkpoint(records[-1].get("send_ts", watermark), run_id=self.run_id)
        self.record_metadata(len(records), len(records), "success", self.provider, key)
