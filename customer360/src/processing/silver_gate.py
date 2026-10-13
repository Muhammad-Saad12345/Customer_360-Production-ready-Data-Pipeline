from __future__ import annotations

import io
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import boto3
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    import subprocess
    import sys
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyarrow>=14.0.0"])
        import pyarrow as pa
        import pyarrow.parquet as pq
    except Exception:
        pa = None
        pq = None

from src.config.settings import get_settings
from src.processing.hubspot_processor import HubSpotDataProcessor

logger = logging.getLogger("customer360.silver")


class HubSpotSilverProcessor:
    """Source/entity-aware schema and DQ validation for the Customer 360 Silver gate."""

    ENTITY_CONTRACTS: Dict[tuple[str, str], Dict[str, Any]] = {
        ("hubspot", "contacts"): {
            "required_fields": ["id", "email"],
            "required_nested": ["properties"],
            "timestamp_fields": ["createdate", "lastmodifieddate"],
            "email_fields": ["email"],
            "enum_fields": {},
            "numeric_fields": {},
            "key_fields": ["id", "properties.hs_object_id"],
        },
        ("hubspot", "orders"): {
            "required_fields": ["id"],
            "required_nested": ["properties"],
            "timestamp_fields": ["createdate", "lastmodifieddate"] ,
            "email_fields": [],
            "enum_fields": {},
            "numeric_fields": {},
            "key_fields": ["id", "properties.hs_object_id"],
        },
        ("hubspot", "products"): {
            "required_fields": ["id"],
            "required_nested": ["properties"],
            "timestamp_fields": ["lastmodifieddate"],
            "email_fields": [],
            "enum_fields": {},
            "numeric_fields": {},
            "key_fields": ["id", "properties.hs_object_id"],
        },
        ("oms", "orders"): {
            "required_fields": [
                "order_id",
                "customer_id",
                "order_ts",
                "order_status",
                "subtotal",
                "tax",
                "shipping",
                "discount",
                "net_total",
                "currency",
                "channel",
                "updated_at",
            ],
            "timestamp_fields": ["order_ts", "updated_at"],
            "email_fields": [],
            "enum_fields": {"order_status": {"paid", "shipped", "fulfilled", "cancelled", "pending", "refunded", "processing", "failed"}},
            "numeric_fields": {"subtotal": {"min": 0}, "tax": {"min": 0}, "shipping": {"min": 0}, "discount": {"min": 0}, "net_total": {"min": 0}},
            "key_fields": ["order_id"],
        },
        ("pim", "products"): {
            "required_fields": [
                "product_id",
                "sku",
                "name",
                "category",
                "brand",
                "price",
                "cost",
                "status",
                "launch_date",
                "updated_at",
            ],
            "timestamp_fields": ["launch_date", "updated_at"],
            "email_fields": [],
            "enum_fields": {"status": {"active", "archived", "draft", "retired", "inactive"}},
            "numeric_fields": {"price": {"min": 0}, "cost": {"min": 0}},
            "key_fields": ["product_id"],
        },
        ("loyalty", "loyalty"): {
            "required_fields": [
                "loyalty_id",
                "customer_id",
                "tier",
                "points_balance",
                "earn_ts",
                "redeem_ts",
                "benefit_code",
                "updated_at",
            ],
            "timestamp_fields": ["earn_ts", "redeem_ts", "updated_at"],
            "email_fields": [],
            "enum_fields": {"tier": {"gold", "silver", "bronze", "platinum", "diamond"}},
            "numeric_fields": {"points_balance": {"min": 0}},
            "key_fields": ["loyalty_id"],
        },
        ("mailchimp", "campaign_events"): {
            "required_fields": [
                "campaign_id",
                "customer_id",
                "channel",
                "send_ts",
                "open_ts",
                "audience_id",
                "member_id",
                "email_address",
                "status",
                "source_type",
            ],
            "timestamp_fields": ["send_ts", "open_ts", "click_ts"],
            "email_fields": ["email_address"],
            "enum_fields": {"channel": {"email"}, "status": {"subscribed", "unsubscribed", "cleaned", "pending", "transactional"}},
            "numeric_fields": {},
            "key_fields": ["member_id", "customer_id"],
        },
        ("marketing", "campaign_events"): {
            "required_fields": [
                "campaign_id",
                "customer_id",
                "channel",
                "send_ts",
                "open_ts",
                "audience_id",
                "member_id",
                "email_address",
                "status",
                "source_type",
            ],
            "timestamp_fields": ["send_ts", "open_ts", "click_ts"],
            "email_fields": ["email_address"],
            "enum_fields": {"channel": {"email"}, "status": {"subscribed", "unsubscribed", "cleaned", "pending", "transactional"}},
            "numeric_fields": {},
            "key_fields": ["member_id", "customer_id"],
        },
        ("support", "tickets"): {
            "required_fields": [
                "ticket_id",
                "customer_id",
                "channel",
                "category",
                "priority",
                "status",
                "created_at",
            ],
            "timestamp_fields": ["created_at", "resolved_at"],
            "email_fields": [],
            "enum_fields": {
                "channel": {"email", "web", "chat", "phone", "api"},
                "priority": {"low", "normal", "high", "urgent"},
                "status": {"new", "open", "pending", "hold", "solved", "closed"},
            },
            "numeric_fields": {"csat_score": {"min": 0, "max": 5}},
            "key_fields": ["ticket_id"],
        },
        ("zendesk", "tickets"): {
            "required_fields": [
                "ticket_id",
                "customer_id",
                "channel",
                "category",
                "priority",
                "status",
                "created_at",
            ],
            "timestamp_fields": ["created_at", "resolved_at"],
            "email_fields": [],
            "enum_fields": {
                "channel": {"email", "web", "chat", "phone", "api"},
                "priority": {"low", "normal", "high", "urgent"},
                "status": {"new", "open", "pending", "hold", "solved", "closed"},
            },
            "numeric_fields": {"csat_score": {"min": 0, "max": 5}},
            "key_fields": ["ticket_id"],
        },
    }

    def __init__(self, base_dir: str | Path | None = None):
        self.settings = get_settings()
        self.base_dir = Path(base_dir) if base_dir else Path(__file__).resolve().parents[1] / "silver"
        self.processor = HubSpotDataProcessor()
        self.s3 = boto3.client("s3", region_name=self.settings.AWS_REGION)
        self.bucket = self.settings.S3_BUCKET

    def _iso_datetime(self, value: Any) -> Optional[datetime]:
        if value is None or value == "":
            return None
        if isinstance(value, datetime):
            return value
        if not isinstance(value, str):
            return None
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    def _is_missing(self, value: Any) -> bool:
        return value is None or (isinstance(value, str) and value.strip() == "")

    def _is_valid_email(self, value: Any) -> bool:
        if not isinstance(value, str):
            return False
        pattern = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        return bool(pattern.match(value.strip()))

    def _to_float(self, value: Any) -> Optional[float]:
        if self._is_missing(value):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _entity_contract(self, source: str, entity: str) -> Dict[str, Any]:
        return self.ENTITY_CONTRACTS.get((source, entity), {
            "required_fields": [],
            "required_nested": [],
            "timestamp_fields": [],
            "email_fields": [],
            "enum_fields": {},
            "numeric_fields": {},
            "key_fields": ["id"],
        })

    def _record_key(self, source: str, entity: str, record: Dict[str, Any]) -> Optional[str]:
        if not isinstance(record, dict):
            return None
        contract = self._entity_contract(source, entity)
        for key_path in contract.get("key_fields", ["id"]):
            if "." not in key_path:
                value = record.get(key_path)
                if value not in (None, ""):
                    return str(value)
                continue
            head, tail = key_path.split(".", 1)
            nested = record.get(head)
            if isinstance(nested, dict):
                value = nested.get(tail)
                if value not in (None, ""):
                    return str(value)
        if source == "hubspot":
            properties = record.get("properties")
            if isinstance(properties, dict) and "hs_object_id" in properties and properties.get("hs_object_id") not in (None, ""):
                return str(properties["hs_object_id"])
        return None

    def _build_quarantine_record(
        self,
        source: str,
        entity: str,
        run_id: str,
        record: Dict[str, Any],
        validation_rule: str,
        failure_reason: str,
    ) -> Dict[str, Any]:
        return {
            "run_id": run_id,
            "source": source,
            "entity": entity,
            "validation_rule": validation_rule,
            "failure_reason": failure_reason,
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "record": record,
        }

    def _validate_hubspot_record(self, source: str, entity: str, record: Dict[str, Any], seen_keys: set[str]) -> Optional[Dict[str, Any]]:
        if not isinstance(record, dict):
            return self._build_quarantine_record(source, entity, "", record, "schema", "record is not a JSON object")
        if "properties" not in record or not isinstance(record.get("properties"), dict):
            return self._build_quarantine_record(source, entity, "", record, "schema", "missing properties object")

        record_id = self._record_key(source, entity, record)
        if record_id is None:
            return self._build_quarantine_record(source, entity, "", record, "required_key", "missing required primary key")
        if record_id in seen_keys:
            return self._build_quarantine_record(source, entity, "", record, "duplicate_key", f"duplicate primary key {record_id}")

        props = record["properties"]
        for field in ["id", "email"] if entity == "contacts" else ["id"]:
            if field == "email":
                if self._is_missing(props.get("email")):
                    return self._build_quarantine_record(source, entity, "", record, "required_key", "missing required field 'email'")
                continue
            if record_id is None:
                return self._build_quarantine_record(source, entity, "", record, "required_key", "missing required id")

        if "email" in props and not self._is_valid_email(props.get("email")):
            return self._build_quarantine_record(source, entity, "", record, "email_format", f"invalid email: {props.get('email')}")

        for timestamp_field in ("createdate", "lastmodifieddate"):
            if timestamp_field in props and props.get(timestamp_field) not in (None, ""):
                if self._iso_datetime(props.get(timestamp_field)) is None:
                    return self._build_quarantine_record(source, entity, "", record, "timestamp_format", f"malformed timestamp in '{timestamp_field}'")

        seen_keys.add(record_id)
        return None

    def _validate_generic_record(self, source: str, entity: str, record: Dict[str, Any], seen_keys: set[str]) -> Optional[Dict[str, Any]]:
        if not isinstance(record, dict):
            return self._build_quarantine_record(source, entity, "", record, "schema", "record is not a JSON object")

        contract = self._entity_contract(source, entity)
        record_id = self._record_key(source, entity, record)
        if record_id is None:
            return self._build_quarantine_record(source, entity, "", record, "required_key", "missing required primary key")
        if record_id in seen_keys:
            return self._build_quarantine_record(source, entity, "", record, "duplicate_key", f"duplicate primary key {record_id}")

        for field in contract.get("required_fields", []):
            if self._is_missing(record.get(field)):
                return self._build_quarantine_record(source, entity, "", record, "required_key", f"missing required field '{field}'")

        for field, allowed in contract.get("enum_fields", {}).items():
            value = record.get(field)
            if self._is_missing(value):
                continue
            if value not in allowed:
                return self._build_quarantine_record(source, entity, "", record, "enum", f"invalid enum value for '{field}': {value}")

        for field, limits in contract.get("numeric_fields", {}).items():
            value = record.get(field)
            if self._is_missing(value):
                continue
            numeric_value = self._to_float(value)
            if numeric_value is None:
                return self._build_quarantine_record(source, entity, "", record, "numeric", f"invalid numeric value for '{field}': {value}")
            if "min" in limits and numeric_value < limits["min"]:
                return self._build_quarantine_record(source, entity, "", record, "numeric", f"numeric value below minimum for '{field}': {value}")

        for field in contract.get("email_fields", []):
            value = record.get(field)
            if self._is_missing(value):
                continue
            if not self._is_valid_email(value):
                return self._build_quarantine_record(source, entity, "", record, "email_format", f"invalid email: {value}")

        for field in contract.get("timestamp_fields", []):
            value = record.get(field)
            if self._is_missing(value):
                continue
            if self._iso_datetime(value) is None:
                return self._build_quarantine_record(source, entity, "", record, "timestamp_format", f"malformed timestamp in '{field}'")

        seen_keys.add(record_id)
        return None

    def _validate_record(self, source: str, entity: str, record: Dict[str, Any], seen_keys: set[str]) -> Optional[Dict[str, Any]]:
        if (source, entity) == ("hubspot", "contacts"):
            return self._validate_hubspot_record(source, entity, record, seen_keys)
        if source == "hubspot" and entity in {"orders", "products"}:
            return self._validate_hubspot_record(source, entity, record, seen_keys)
        return self._validate_generic_record(source, entity, record, seen_keys)

    def _base_standardized_row(self, source: str, entity: str, record: Dict[str, Any], run_id: str) -> Dict[str, Any]:
        return {
            "source": source,
            "entity": entity,
            "run_id": run_id,
            "record_id": self._record_key(source, entity, record),
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _standardize_hubspot(self, source: str, entity: str, record: Dict[str, Any], run_id: str) -> Dict[str, Any]:
        properties = record.get("properties", {})
        item = self._base_standardized_row(source, entity, record, run_id)
        if entity == "contacts":
            item.update({
                "customer_id": record.get("id"),
                "first_name": properties.get("firstname"),
                "last_name": properties.get("lastname"),
                "email": properties.get("email"),
                "phone": properties.get("phone"),
                "customer_stage": properties.get("lifecyclestage"),
                "created_at": properties.get("createdate"),
                "updated_at": properties.get("lastmodifieddate"),
                "owner_id": properties.get("hubspot_owner_id"),
            })
        elif entity == "orders":
            item.update(self.processor.process_orders([record])[0])
        elif entity == "products":
            item.update(self.processor.process_products([record])[0])
        return item

    def _standardize_oms_orders(self, record: Dict[str, Any], run_id: str) -> Dict[str, Any]:
        item = self._base_standardized_row("oms", "orders", record, run_id)
        item.update({
            "order_id": record.get("order_id"),
            "customer_id": record.get("customer_id"),
            "order_ts": record.get("order_ts"),
            "order_status": record.get("order_status"),
            "subtotal": self._to_float(record.get("subtotal")),
            "tax": self._to_float(record.get("tax")),
            "shipping": self._to_float(record.get("shipping")),
            "discount": self._to_float(record.get("discount")),
            "net_total": self._to_float(record.get("net_total")),
            "currency": record.get("currency"),
            "channel": record.get("channel"),
            "updated_at": record.get("updated_at"),
        })
        return item

    def _standardize_pim_products(self, record: Dict[str, Any], run_id: str) -> Dict[str, Any]:
        item = self._base_standardized_row("pim", "products", record, run_id)
        item.update({
            "product_id": record.get("product_id"),
            "sku": record.get("sku"),
            "name": record.get("name"),
            "category": record.get("category"),
            "brand": record.get("brand"),
            "price": self._to_float(record.get("price")),
            "cost": self._to_float(record.get("cost")),
            "attributes": record.get("attributes"),
            "status": record.get("status"),
            "launch_date": record.get("launch_date"),
            "updated_at": record.get("updated_at"),
        })
        return item

    def _standardize_loyalty(self, record: Dict[str, Any], run_id: str) -> Dict[str, Any]:
        item = self._base_standardized_row("loyalty", "loyalty", record, run_id)
        item.update({
            "loyalty_id": record.get("loyalty_id"),
            "customer_id": record.get("customer_id"),
            "tier": record.get("tier"),
            "points_balance": self._to_float(record.get("points_balance")),
            "earn_ts": record.get("earn_ts"),
            "redeem_ts": record.get("redeem_ts"),
            "benefit_code": record.get("benefit_code"),
            "updated_at": record.get("updated_at"),
        })
        return item

    def _standardize_mailchimp(self, record: Dict[str, Any], run_id: str) -> Dict[str, Any]:
        item = self._base_standardized_row("mailchimp", "campaign_events", record, run_id)
        item.update({
            "campaign_id": record.get("campaign_id"),
            "customer_id": record.get("customer_id"),
            "channel": record.get("channel"),
            "send_ts": record.get("send_ts"),
            "open_ts": record.get("open_ts"),
            "click_ts": record.get("click_ts"),
            "audience_id": record.get("audience_id"),
            "offer_id": record.get("offer_id"),
            "member_id": record.get("member_id"),
            "email_address": record.get("email_address"),
            "status": record.get("status"),
            "source_type": record.get("source_type"),
        })
        return item

    def _standardize_support_tickets(self, source: str, record: Dict[str, Any], run_id: str) -> Dict[str, Any]:
        item = self._base_standardized_row(source, "tickets", record, run_id)
        item.update({
            "ticket_id": record.get("ticket_id"),
            "customer_id": record.get("customer_id"),
            "channel": record.get("channel"),
            "category": record.get("category"),
            "priority": record.get("priority"),
            "status": record.get("status"),
            "agent_id": record.get("agent_id"),
            "created_at": record.get("created_at"),
            "resolved_at": record.get("resolved_at"),
            "csat_score": self._to_float(record.get("csat_score")),
            "source_type": record.get("source_type"),
        })
        return item

    def _standardize_self(self, source: str, entity: str, record: Dict[str, Any], run_id: str) -> Dict[str, Any]:
        if source == "hubspot":
            return self._standardize_hubspot(source, entity, record, run_id)
        if source == "oms" and entity == "orders":
            return self._standardize_oms_orders(record, run_id)
        if source == "pim" and entity == "products":
            return self._standardize_pim_products(record, run_id)
        if source == "loyalty" and entity == "loyalty":
            return self._standardize_loyalty(record, run_id)
        if source in ("mailchimp", "marketing") and entity == "campaign_events":
            return self._standardize_mailchimp(record, run_id)
        if source in ("support", "zendesk") and entity == "tickets":
            return self._standardize_support_tickets(source, record, run_id)
        return self._base_standardized_row(source, entity, record, run_id)

    def _build_s3_key(self, layer: str, source: str, entity: str, run_id: str, suffix: str) -> str:
        ingestion_date = datetime.now(timezone.utc).date().isoformat()
        return f"{layer}/{source}/{entity}/ingestion_date={ingestion_date}/run_id={run_id}/{suffix}"

    def _normalize_for_parquet(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return json.dumps(value, default=str)
        if isinstance(value, (datetime,)):
            return value.isoformat()
        if isinstance(value, (int, float, bool, str)):
            return value
        return str(value)

    def _write_silver_parquet(self, rows: List[Dict[str, Any]], source: str, entity: str, run_id: str) -> str:
        key = self._build_s3_key("silver", source, entity, run_id, "data.parquet")
        if not rows:
            table = pa.table({
                "source": pa.array([], type=pa.string()),
                "entity": pa.array([], type=pa.string()),
                "run_id": pa.array([], type=pa.string()),
                "record_id": pa.array([], type=pa.string()),
                "ingestion_timestamp": pa.array([], type=pa.string()),
            })
        else:
            sanitized_rows = [{
                field: self._normalize_for_parquet(row.get(field))
                for field in sorted({key for row in rows for key in row.keys()})
            } for row in rows]
            table = pa.Table.from_pylist(sanitized_rows)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())
        return f"s3://{self.bucket}/{key}"

    def _write_quarantine(self, invalid_rows: List[Dict[str, Any]], source: str, entity: str, run_id: str) -> str:
        key = self._build_s3_key("quarantine", source, entity, run_id, "invalid_records.json")
        payload = [{
            "run_id": row.get("run_id"),
            "source": row.get("source"),
            "entity": row.get("entity"),
            "validation_rule": row.get("validation_rule"),
            "failure_reason": row.get("failure_reason"),
            "ingestion_timestamp": row.get("ingestion_timestamp"),
            "record": row.get("record"),
        } for row in invalid_rows]
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=json.dumps(payload, indent=2, default=str).encode("utf-8"))
        return f"s3://{self.bucket}/{key}"

    def read_raw_records_from_s3(self, source: str, entity: str, run_id: Optional[str] = None) -> List[Dict[str, Any]]:
        s3 = boto3.client("s3", region_name=self.settings.AWS_REGION)
        objects = []
        if run_id:
            today = datetime.now(timezone.utc).date().isoformat()
            today_prefix = f"raw/{source}/{entity}/ingestion_date={today}/run_id={run_id}/"
            resp = s3.list_objects_v2(Bucket=self.settings.S3_BUCKET, Prefix=today_prefix)
            objects = resp.get("Contents", [])
            if not objects:
                general_prefix = f"raw/{source}/{entity}/"
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=self.settings.S3_BUCKET, Prefix=general_prefix):
                    for item in page.get("Contents", []):
                        if f"run_id={run_id}" in item.get("Key", ""):
                            objects.append(item)
                    if objects:
                        break
        else:
            prefix = f"raw/{source}/{entity}/"
            resp = s3.list_objects_v2(Bucket=self.settings.S3_BUCKET, Prefix=prefix)
            objects = resp.get("Contents", [])
        records: List[Dict[str, Any]] = []
        for item in objects:
            key = item.get("Key")
            if not key or not key.endswith("data.json"):
                continue
            body = s3.get_object(Bucket=self.settings.S3_BUCKET, Key=key)["Body"].read().decode("utf-8")
            if not body.strip():
                continue
            try:
                parsed = json.loads(body)
                if isinstance(parsed, list):
                    records.extend(parsed)
                elif isinstance(parsed, dict):
                    records.append(parsed)
            except json.JSONDecodeError:
                for line in body.splitlines():
                    if line.strip():
                        records.append(json.loads(line))
        return records

    def process_records(
        self,
        source: str,
        entity: str,
        run_id: str,
        raw_records: Optional[Iterable[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        if raw_records is not None:
            records = list(raw_records)
        else:
            records = self.read_raw_records_from_s3(source, entity, run_id=run_id) if run_id else []

        input_record_count = len(records)
        valid_rows: List[Dict[str, Any]] = []
        invalid_rows: List[Dict[str, Any]] = []
        seen_keys: set[str] = set()

        for record in records:
            issue = self._validate_record(source, entity, record, seen_keys)
            if issue is not None:
                issue["run_id"] = run_id
                issue["source"] = source
                issue["entity"] = entity
                invalid_rows.append(issue)
                logger.warning(
                    "validation_failed",
                    extra={
                        "run_id": run_id,
                        "source": source,
                        "object": entity,
                        "validation_rule": issue.get("validation_rule"),
                        "failure_reason": issue.get("failure_reason"),
                    },
                )
                continue
            valid_rows.append(self._standardize_self(source, entity, record, run_id))

        quarantine_path = self._write_quarantine(invalid_rows, source, entity, run_id) if invalid_rows else ""
        silver_path = self._write_silver_parquet(valid_rows, source, entity, run_id) if valid_rows else self._write_silver_parquet([], source, entity, run_id)

        logger.info(
            "silver_processing_summary",
            extra={
                "run_id": run_id,
                "source": source,
                "object": entity,
                "input_record_count": input_record_count,
                "valid_count": len(valid_rows),
                "invalid_count": len(invalid_rows),
                "quarantine_path": quarantine_path,
                "silver_path": silver_path,
            },
        )

        return {
            "input_record_count": input_record_count,
            "valid_count": len(valid_rows),
            "invalid_count": len(invalid_rows),
            "quarantine_path": quarantine_path,
            "silver_path": silver_path,
            "quarantine_records": invalid_rows,
            "valid_records": valid_rows,
        }
