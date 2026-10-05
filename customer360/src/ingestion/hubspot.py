from __future__ import annotations

from datetime import datetime, timedelta, timezone

import requests
from typing import Iterable, List, Dict, Any, Optional

from src.ingestion.base import BaseIngest
from src.config.settings import get_settings
from src.utils.retry import retry_on_exception


class HubSpotIngest(BaseIngest):
    def __init__(self):
        super().__init__(source="hubspot", object_name="contacts")
        self.settings = get_settings()
        self.token = self.settings.HUBSPOT_ACCESS_TOKEN
        self._use_synthetic = self._is_placeholder_token(self.token)
        self.headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"} if self.token and not self._use_synthetic else {}

    @staticmethod
    def _is_placeholder_token(value: Optional[str]) -> bool:
        if not value:
            return True
        normalized = value.strip().lower()
        placeholders = {"your_hubspot_token", "your_hubspot_api_key", "your_api_key", "placeholder", "example", "changeme", "dummy"}
        return normalized.startswith("your_") or any(item in normalized for item in ("placeholder", "example", "changeme", "dummy")) or normalized in placeholders

    def _synthetic_contact_records(self, watermark: Optional[str] = None) -> List[Dict[str, Any]]:
        base = datetime.now(timezone.utc)
        records: List[Dict[str, Any]] = []
        for idx in range(2):
            updated_at = (base - timedelta(minutes=idx * 30)).isoformat()
            if watermark and updated_at <= watermark:
                continue
            records.append({
                "id": f"synthetic_contact_{idx + 1}",
                "properties": {
                    "firstname": f"Synthetic",
                    "lastname": f"Customer {idx + 1}",
                    "email": f"customer{idx + 1}@synthetic.local",
                    "phone": "+1555000000{idx}",
                    "createdate": (base - timedelta(days=idx + 1)).isoformat(),
                    "lastmodifieddate": updated_at,
                    "lifecyclestage": "customer",
                    "hubspot_owner_id": "owner_001",
                },
                "source_type": "synthetic",
            })
        return records


    @retry_on_exception(max_attempts=4)
    def _fetch_page(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        resp = requests.get(url, headers=self.headers, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

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

    def _synthetic_mode(self) -> bool:
        env = getattr(self.settings, "ENVIRONMENT", "dev").lower()
        if env in ("prod", "production", "staging", "stage"):
            return False
        return self._use_synthetic or not self.token


    def run(self) -> None:
        url = f"{self.settings.HUBSPOT_BASE_URL}/crm/v3/objects/contacts"
        properties = [
            "firstname",
            "lastname",
            "email",
            "phone",
            "createdate",
            "lastmodifieddate",
            "lifecyclestage",
            "hubspot_owner_id",
        ]

        params = {"limit": 100, "properties": ",".join(properties), "archived": False}

        default_watermark = "1970-01-01T00:00:00Z"
        lookback_minutes = self.get_lookback_minutes(self.settings.HUBSPOT_LOOKBACK_MINUTES)
        watermark = self.resolve_watermark(default_watermark, lookback_minutes=lookback_minutes)
        extraction_end = self.capture_extraction_end()
        window_start = self.compute_window_start(watermark, lookback_minutes=lookback_minutes, extraction_end=extraction_end)
        window_start_dt = self._parse_datetime(window_start)
        self.log_event(
            "hubspot_contact_extraction_start",
            watermark_before=watermark,
            lookback_minutes=lookback_minutes,
            lookback_start=window_start,
            extraction_end=extraction_end,
            page_size=params.get("limit"),
            source_url=url,
        )

        results: List[Dict[str, Any]] = []
        page_count = 0
        try:
            after: Optional[str] = None
            while True:
                if after:
                    params["after"] = after
                page = self._fetch_page(url, params)
                page_count += 1
                page_results = page.get("results", [])
                self.log_event(
                    "hubspot_api_page_response",
                    page_number=page_count,
                    page_size=len(page_results),
                    next_after=page.get("paging", {}).get("next", {}).get("after"),
                    has_paging=bool(page.get("paging")),
                )
                for obj in page_results:
                    props = obj.get("properties", {})
                    updated_at = props.get("lastmodifieddate")
                    value_dt = self._parse_datetime(updated_at)
                    if window_start_dt is None or not value_dt or value_dt >= window_start_dt:
                        results.append(obj)

                paging = page.get("paging")
                if not paging:
                    break
                next_token = paging.get("next", {}).get("after")
                if not next_token:
                    break
                after = next_token
        except Exception as exc:
            env = getattr(self.settings, "ENVIRONMENT", "dev").lower()
            if env in ("prod", "production", "staging", "stage"):
                self.logger.error("HubSpot API failed in %s environment: %s", env, exc)
                raise
            if not self._synthetic_mode():
                raise
            self.logger.warning("HubSpot API unavailable; falling back to synthetic contacts for local development.")
            results = self._synthetic_contact_records(watermark)

        deduped_results = self._deduplicate_records(results)
        self.log_event(
            "hubspot_contact_extraction_summary",
            watermark_before=watermark,
            lookback_start=window_start,
            extraction_end=extraction_end,
            raw_records=len(results),
            accepted_records=len(deduped_results),
            page_count=page_count,
            watermark_after=watermark,
        )

        if not deduped_results:
            self.log_event(
                "hubspot_no_records_for_window",
                source_object="contacts",
                records_seen=len(results),
                watermark_before=watermark,
                lookback_start=window_start,
                extraction_end=extraction_end,
                window_type="incremental",
            )
            self.logger.info("No contact records matched the incremental window for HubSpot; no data persisted.")
            self.record_metadata(0, 0, "success", f"{self.settings.HUBSPOT_BASE_URL}/crm/v3/objects/contacts", "")
            return

        key = self.persist_raw(deduped_results)
        if not key:
            self.log_event("hubspot_contact_write_failed", records_extracted=len(deduped_results), records_written=0, s3_key=key, watermark_before=watermark)
            self.record_metadata(len(deduped_results), 0, "failed", f"{self.settings.HUBSPOT_BASE_URL}/crm/v3/objects/contacts", key, error_message="S3 persistence failed")
            return

        latest_watermark = self._latest_record_timestamp(deduped_results, field_names=["lastmodifieddate", "hs_lastmodifieddate"])
        if latest_watermark:
            self.update_checkpoint(latest_watermark, run_id=self.run_id)
        self.log_event("hubspot_contact_write_summary", records_extracted=len(deduped_results), records_written=len(deduped_results), s3_key=key, watermark_after=latest_watermark)

        self.record_metadata(
            records_extracted=len(deduped_results),
            records_uploaded=len(deduped_results),
            status="success",
            source_url=f"{self.settings.HUBSPOT_BASE_URL}/crm/v3/objects/contacts",
            file_path=key,
        )
