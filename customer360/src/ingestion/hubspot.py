from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.config.settings import get_settings
from src.errors import SourceModeError
from src.ingestion.base import BaseIngest
from src.ingestion.http_client import SourceHttpClient
from src.ingestion.source_mode import load_source_credentials

CONTACT_PROPERTIES = [
    "firstname", "lastname", "email", "phone", "createdate",
    "lastmodifieddate", "lifecyclestage", "hubspot_owner_id",
]


class HubSpotIngest(BaseIngest):
    """HubSpot CRM contacts (contacts only; orders are OMS, products are PIM).

    api mode reads a private-app token from secret customer360/<env>/hubspot
    {"access_token": "pat-..."} or the legacy HUBSPOT_ACCESS_TOKEN setting.
    """

    watermark_fields = ["lastmodifieddate", "hs_lastmodifieddate", "updatedAt"]

    def __init__(self, run_id: Optional[str] = None):
        super().__init__(source="hubspot", object_name="contacts", run_id=run_id)
        self.settings = get_settings()
        self.base_url = getattr(self.settings, "HUBSPOT_BASE_URL", None) or "https://api.hubapi.com"
        self.token: Optional[str] = getattr(self.settings, "HUBSPOT_ACCESS_TOKEN", None)
        self._http: Optional[SourceHttpClient] = None

    @staticmethod
    def _is_placeholder_token(value: Optional[str]) -> bool:
        if not value:
            return True
        normalized = value.strip().lower()
        return normalized.startswith("your_") or any(
            item in normalized for item in ("placeholder", "example", "changeme", "dummy")
        )

    def _synthetic_contact_records(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        """Six contacts whose emails match the synthetic OMS/loyalty/marketing/support customers."""
        base = datetime.now(timezone.utc)
        records: List[Dict[str, Any]] = []
        for idx in range(6):
            number = 1001 + idx
            updated_at = (base - timedelta(minutes=idx * 30)).isoformat().replace("+00:00", "Z")
            if watermark and updated_at < watermark:
                continue
            records.append({
                "id": f"{9000000 + number}",
                "properties": {
                    "firstname": "Synthetic",
                    "lastname": f"Customer {number}",
                    "email": f"customer{number}@synthetic.local",
                    "phone": f"+1555000{number}",
                    "createdate": (base - timedelta(days=idx + 1)).isoformat().replace("+00:00", "Z"),
                    "lastmodifieddate": updated_at,
                    "lifecyclestage": "customer",
                    "hubspot_owner_id": "owner_001",
                    "customer_id": f"C-{number}",
                },
                "source_type": "synthetic",
            })
        return records

    def _synthetic_records(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        return self._synthetic_contact_records(watermark)

    def _fetch_page(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        if self._http is None:
            self._http = SourceHttpClient(
                self.source,
                headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
            )
        return self._http.get_json(url, params=params)

    def record_watermark(self, record: Dict[str, Any]) -> Optional[str]:
        props = record.get("properties") or {} if isinstance(record, dict) else {}
        return props.get("lastmodifieddate") or props.get("hs_lastmodifieddate") or record.get("updatedAt")

    def _fetch_api(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        secret = load_source_credentials(self.source)
        self.token = secret.get("access_token") or self.token
        if self._is_placeholder_token(self.token):
            raise SourceModeError("hubspot api mode needs a real private-app access_token")
        records, _ = self._fetch_objects("contacts", {"contact": CONTACT_PROPERTIES})
        return self._filter_since(records, watermark)

    def prepare_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return super().prepare_records(self._deduplicate_records(records))

    def _fetch_objects(
        self,
        object_type: str,
        field_map: Dict[str, List[str]],
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> tuple[List[Dict[str, Any]], List[str]]:
        """Paginate through the HubSpot CRM objects API for *object_type*.

        Args:
            object_type: HubSpot CRM object type (e.g. ``"contacts"``).
            field_map: Mapping of internal field names to HubSpot property names
                used to derive the ``properties`` query param.
            extra_params: Additional query parameters merged into every request.

        Returns:
            A tuple of ``(records, selected_properties)`` where *records* is the
            full list of raw API objects across all pages and *selected_properties*
            is the list of property names that were requested.
        """
        base_url = f"{self.settings.HUBSPOT_BASE_URL}/crm/v3/objects/{object_type}"

        # Collect all unique property names from the field_map values
        selected: List[str] = []
        seen: set = set()
        for names in field_map.values():
            for name in names:
                if name not in seen:
                    selected.append(name)
                    seen.add(name)

        params: Dict[str, Any] = {"limit": 100, "properties": ",".join(selected), "archived": False}
        if extra_params:
            params.update(extra_params)

        results: List[Dict[str, Any]] = []
        after: Optional[str] = None
        while True:
            if after:
                params["after"] = after
            page = self._fetch_page(base_url, params)
            results.extend(page.get("results", []))
            paging = page.get("paging")
            if not paging:
                break
            next_token = paging.get("next", {}).get("after")
            if not next_token:
                break
            after = next_token

        return results, selected

    @staticmethod
    def _deduplicate_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        unique: Dict[str, Dict[str, Any]] = {}
        for record in records:
            record_id = record.get("id") or record.get("properties", {}).get("hs_object_id") or str(record)
            if record_id not in unique:
                unique[record_id] = record
            else:
                current = unique[record_id]
                current_updated = current.get("properties", {}).get("lastmodifieddate") if isinstance(current, dict) else None
                next_updated = record.get("properties", {}).get("lastmodifieddate") if isinstance(record, dict) else None
                if next_updated and (not current_updated or next_updated > current_updated):
                    unique[record_id] = record
        return list(unique.values())

    def _latest_record_timestamp(self, records: List[Dict[str, Any]], field_names: Optional[List[str]] = None) -> Optional[str]:
        if field_names is None:
            field_names = ["lastmodifieddate", "hs_lastmodifieddate", "updatedAt"]

        latest: Optional[datetime] = None
        for record in records:
            properties = record.get("properties", {}) if isinstance(record, dict) else {}
            candidates = [record.get("updatedAt")] if isinstance(record, dict) and record.get("updatedAt") else []
            for field_name in field_names:
                value = properties.get(field_name)
                if value:
                    candidates.append(value)
            for value in candidates:
                value_dt = self._parse_datetime(value)
                if value_dt and (latest is None or value_dt > latest):
                    latest = value_dt
        if latest is None:
            return None
        return latest.isoformat().replace("+00:00", "Z")

    def _filter_records_since(
        self,
        records: List[Dict[str, Any]],
        field_name: str = "lastmodifieddate",
        watermark: Optional[str] = None,
        lookback_minutes: int = 15,
        now_utc: Optional[str] = None,
        extraction_end: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        default_watermark = "1970-01-01T00:00:00Z"
        watermark_value = watermark or self.resolve_watermark(default_watermark, lookback_minutes=lookback_minutes)
        extraction_end_value = extraction_end or (now_utc or datetime.now(timezone.utc).isoformat())
        window_start = self.compute_window_start(watermark_value, lookback_minutes=lookback_minutes, now_utc=now_utc, extraction_end=extraction_end_value)
        window_start_dt = self._parse_datetime(window_start)
        self.log_event(
            "hubspot_extraction_window",
            watermark_before=watermark_value,
            lookback_minutes=lookback_minutes,
            lookback_start=window_start,
            extraction_end=extraction_end_value,
            source_records=len(records),
        )
        if not window_start_dt:
            return records

        filtered: List[Dict[str, Any]] = []
        for record in records:
            properties = record.get("properties", {})
            updated_at = properties.get(field_name)
            value_dt = self._parse_datetime(updated_at)
            if value_dt and value_dt >= window_start_dt:
                filtered.append(record)
        deduped = self._deduplicate_records(filtered)
        self.log_event(
            "hubspot_extraction_filter_summary",
            records_before_filter=len(records),
            records_after_filter=len(filtered),
            records_written=len(deduped),
            watermark_before=watermark_value,
            lookback_start=window_start,
            extraction_end=extraction_end_value,
        )
        return deduped
