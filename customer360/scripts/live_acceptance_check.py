from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.config.settings import get_settings
from src.ingestion.loyalty import LoyaltyIngest
from src.ingestion.marketing import MarketingIngest
from src.ingestion.oms import OmsIngest
from src.ingestion.pim import PimIngest
from src.processing.silver_gate import HubSpotSilverProcessor


settings = get_settings()
s3 = boto3.client("s3", region_name=settings.AWS_REGION)
processor = HubSpotSilverProcessor()
bucket = settings.S3_BUCKET


def read_parquet_rows(path: str | None):
    if not path:
        return []
    key = path.replace(f"s3://{bucket}/", "", 1)
    payload = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    table = pq.read_table(pa.BufferReader(payload))
    return table.to_pylist()


results = []
source_specs = [
    ("oms", "orders", OmsIngest(), "1970-01-01T00:00:00Z", "oms-live-accept"),
    ("pim", "products", PimIngest(), "1970-01-01T00:00:00Z", "pim-live-accept"),
    ("loyalty", "loyalty", LoyaltyIngest(), "1970-01-01T00:00:00Z", "loyalty-live-accept"),
    ("mailchimp", "campaign_events", MarketingIngest(), "1970-01-01T00:00:00Z", "mailchimp-live-accept"),
]

for source, entity, ingester, watermark, run_id in source_specs:
    records = ingester.fetch_records(watermark=watermark)
    raw_key = ingester.persist_raw(records)
    result = processor.process_records(source=source, entity=entity, run_id=run_id, raw_records=records)
    parquet_rows = read_parquet_rows(result["silver_path"])
    summary = {
        "source": source,
        "entity": entity,
        "command": f"{ingester.__class__.__name__}.fetch_records('{watermark}') -> persist_raw() -> process_records(source='{source}', entity='{entity}', run_id='{run_id}')",
        "raw_record_count": len(records),
        "raw_s3_key": raw_key,
        "valid_count": result["valid_count"],
        "invalid_count": result["invalid_count"],
        "silver_path": result["silver_path"],
        "quarantine_path": result["quarantine_path"],
        "quarantine_count": len(result.get("quarantine_records") or []),
        "parquet_written": bool(result["silver_path"]),
        "parquet_read_success": bool(result["silver_path"]) and isinstance(parquet_rows, list),
        "silver_rows_read_back": len(parquet_rows),
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    results.append(summary)

oms_ing = OmsIngest()
oms_records = oms_ing.fetch_records("1970-01-01T00:00:00Z")
first = processor.process_records(source="oms", entity="orders", run_id="oms-idempotency-check", raw_records=oms_records)
second = processor.process_records(source="oms", entity="orders", run_id="oms-idempotency-check", raw_records=oms_records)
results.append(
    {
        "source": "oms",
        "entity": "orders",
        "command": "repeat same run_id with identical raw_records",
        "idempotent_same_path": first["silver_path"] == second["silver_path"],
        "idempotent_valid_count_equal": first["valid_count"] == second["valid_count"],
        "idempotent_invalid_count_equal": first["invalid_count"] == second["invalid_count"],
        "run_id": "oms-idempotency-check",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
)

print(json.dumps(results, indent=2, default=str))
print("RAW_PREFIX_SAMPLE")
print(json.dumps(s3.list_objects_v2(Bucket=bucket, Prefix="raw/", MaxKeys=20).get("Contents", [])[:10], default=str))
print("SILVER_PREFIX_SAMPLE")
print(json.dumps(s3.list_objects_v2(Bucket=bucket, Prefix="silver/", MaxKeys=20).get("Contents", [])[:10], default=str))
print("QUARANTINE_PREFIX_SAMPLE")
print(json.dumps(s3.list_objects_v2(Bucket=bucket, Prefix="quarantine/", MaxKeys=20).get("Contents", [])[:10], default=str))
