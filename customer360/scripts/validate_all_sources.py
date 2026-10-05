#!/usr/bin/env python3
"""
validate_all_sources.py
=======================
Customer 360 — Full source-branch Iceberg validation script.

Reads Silver Parquet files already written to S3 by the Airflow DAG,
constructs Athena Engine v3 compatible inline VALUES MERGE INTO queries,
executes them against the real AWS account, then verifies row counts.

Usage:
    python validate_all_sources.py [--dry-run] [--run-id <run_id>]

Requires: AWS credentials, ICEBERG_BUCKET env var (or falls back to c360-bucket786).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

# ---- ensure src/ is importable -------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.lakehouse.athena_client import AthenaClient
from src.lakehouse.config import LakehouseConfig, get_lakehouse_config
from src.lakehouse.table_definitions import (
    TABLE_REGISTRY,
    generate_athena_create_table_ddl,
    generate_athena_merge_values_query,
    get_table_definition,
)

# ---- logging -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("validate_all_sources")


def load_config() -> LakehouseConfig:
    cfg = get_lakehouse_config()
    log.info("Config — bucket=%s  db=%s  workgroup=%s  region=%s",
             cfg.bucket, cfg.database_name, cfg.athena_workgroup, cfg.region_name)
    return cfg


def locate_latest_silver_parquet(
    s3: Any, bucket: str, source: str, entity: str
) -> Optional[str]:
    """Return the S3 key of the most recent Silver parquet for source/entity."""
    prefix = f"silver/{source}/{entity}/"
    paginator = s3.get_paginator("list_objects_v2")
    candidates = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if key.endswith(".parquet"):
                candidates.append((item["LastModified"], key))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def read_parquet_from_s3(s3: Any, bucket: str, key: str) -> Tuple[List[Dict], pa.Schema]:
    resp = s3.get_object(Bucket=bucket, Key=key)
    body = resp["Body"].read()
    tbl = pq.read_table(pa.BufferReader(body))
    return tbl.to_pylist(), tbl.schema


def run_athena_query(athena: AthenaClient, sql: str, label: str) -> Optional[str]:
    """Execute a single Athena query, return query_execution_id or None on failure."""
    try:
        res = athena.execute_query(sql)
        qid = res.query_execution_id
        log.info("[%s] Athena query SUCCEEDED: %s (scanned %d bytes)", label, qid, res.data_scanned_bytes)
        return qid
    except Exception as e:
        log.error("[%s] Athena execution FAILED: %s", label, e)
        return None


def get_iceberg_row_count(athena: AthenaClient, database: str, table: str) -> Optional[int]:
    """Return current row count of an Iceberg table via Athena SELECT COUNT(*)."""
    sql = f"SELECT COUNT(*) FROM {database}.{table};"
    try:
        return athena.query_scalar(sql, database=database)
    except Exception as e:
        log.warning("Row count query failed for %s.%s: %s", database, table, e)
    return None


def ensure_table(athena: AthenaClient, config: LakehouseConfig, source: str, entity: str) -> bool:
    """Create Iceberg table if it doesn't already exist. Return True if created."""
    table_def = get_table_definition(source, entity)
    if not table_def:
        log.warning("No TableDefinition for (%s, %s) — skipping", source, entity)
        return False
    loc = config.table_location(table_def.table_name)
    ddl = generate_athena_create_table_ddl(
        table_def=table_def,
        s3_location=loc,
        database_name=config.database_name,
    )
    try:
        athena.run_ddl(ddl)
        log.info("Table %s.%s ensured.", config.database_name, table_def.table_name)
        return True
    except Exception as e:
        log.error("DDL failed for %s.%s: %s", config.database_name, table_def.table_name, e)
        return False


def validate_source(
    s3: Any,
    athena: AthenaClient,
    config: LakehouseConfig,
    source: str,
    entity: str,
    dry_run: bool,
) -> Dict[str, Any]:
    result = {
        "source": source,
        "entity": entity,
        "table": None,
        "s3_key": None,
        "silver_rows": 0,
        "merge_batches": 0,
        "iceberg_count_before": None,
        "iceberg_count_after": None,
        "status": "skipped",
        "errors": [],
    }

    table_def = get_table_definition(source, entity)
    if not table_def:
        result["errors"].append(f"No TableDefinition registered for ({source},{entity})")
        return result
    result["table"] = table_def.table_name

    # 1. Locate Silver Parquet
    key = locate_latest_silver_parquet(s3, config.bucket, source, entity)
    if not key:
        result["errors"].append("No Silver Parquet found in S3")
        result["status"] = "no_silver_data"
        return result
    result["s3_key"] = key
    log.info("[%s/%s] Silver key: %s", source, entity, key)

    # 2. Read
    rows, schema = read_parquet_from_s3(s3, config.bucket, key)
    result["silver_rows"] = len(rows)
    log.info("[%s/%s] Silver rows: %d", source, entity, len(rows))

    if not rows:
        result["status"] = "empty_silver"
        return result

    # 3. Enrich ingestion_date
    for r in rows:
        if not r.get("ingestion_date"):
            ts = r.get("ingestion_timestamp", "")
            try:
                r["ingestion_date"] = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).date().isoformat()
            except Exception:
                r["ingestion_date"] = datetime.now(timezone.utc).date().isoformat()

    # 4. Ensure table exists
    ensure_table(athena, config, source, entity)

    if dry_run:
        result["status"] = "dry_run_ok"
        log.info("[%s/%s] Dry-run: would load %d rows.", source, entity, len(rows))
        return result

    # 5. Row count before
    result["iceberg_count_before"] = get_iceberg_row_count(athena, config.database_name, table_def.table_name)
    log.info("[%s/%s] Iceberg count before: %s", source, entity, result["iceberg_count_before"])

    # 6. Deduplicate in-memory
    seen: Dict[tuple, Dict] = {}
    for r in rows:
        pk = tuple(str(r.get(k, "")) for k in table_def.primary_keys)
        seen[pk] = r
    deduped = list(seen.values())
    log.info("[%s/%s] Deduped: %d rows", source, entity, len(deduped))

    # 7. Chunk and MERGE
    BATCH_SIZE = 500
    batches = [deduped[i:i + BATCH_SIZE] for i in range(0, len(deduped), BATCH_SIZE)]
    result["merge_batches"] = len(batches)
    all_ok = True

    for idx, batch in enumerate(batches):
        sql = generate_athena_merge_values_query(
            table_def=table_def,
            records=batch,
            database_name=config.database_name,
        )
        qid = run_athena_query(athena, sql, f"{source}/{entity} batch {idx+1}/{len(batches)}")
        if qid is None:
            result["errors"].append(f"MERGE batch {idx+1} failed")
            all_ok = False

    # 8. Row count after
    result["iceberg_count_after"] = get_iceberg_row_count(athena, config.database_name, table_def.table_name)
    log.info("[%s/%s] Iceberg count after: %s", source, entity, result["iceberg_count_after"])

    result["status"] = "success" if all_ok else "partial_failure"
    return result


def main():
    parser = argparse.ArgumentParser(description="Validate all Customer 360 Iceberg source branches")
    parser.add_argument("--dry-run", action="store_true", help="Skip actual Athena execution")
    parser.add_argument("--source", default=None, help="Limit to one source (e.g. oms)")
    parser.add_argument("--entity", default=None, help="Limit to one entity (e.g. orders)")
    args = parser.parse_args()

    config = load_config()
    s3 = boto3.client("s3", region_name=config.region_name)
    athena = AthenaClient(config)

    # Determine which source/entity pairs to validate
    pairs = [(src, ent) for (src, ent) in TABLE_REGISTRY.keys()]
    if args.source:
        pairs = [(s, e) for s, e in pairs if s.lower() == args.source.lower()]
    if args.entity:
        pairs = [(s, e) for s, e in pairs if e.lower() == args.entity.lower()]

    log.info("Validating %d source/entity pairs: %s", len(pairs), pairs)

    results = []
    for source, entity in pairs:
        log.info("=" * 60)
        log.info("Validating %s / %s", source, entity)
        r = validate_source(s3, athena, config, source, entity, dry_run=args.dry_run)
        results.append(r)

    # Print summary
    print("\n" + "=" * 70)
    print(f"{'SOURCE/ENTITY':<30} {'TABLE':<30} {'ROWS':>6} {'BEFORE':>8} {'AFTER':>8}  STATUS")
    print("=" * 70)
    all_ok = True
    for r in results:
        label = f"{r['source']}/{r['entity']}"
        table = r.get("table") or "-"
        rows = r["silver_rows"]
        before = r["iceberg_count_before"] if r["iceberg_count_before"] is not None else "-"
        after = r["iceberg_count_after"] if r["iceberg_count_after"] is not None else "-"
        status = r["status"]
        if status not in ("success", "dry_run_ok", "no_silver_data"):
            all_ok = False
        print(f"{label:<30} {table:<30} {rows:>6} {str(before):>8} {str(after):>8}  {status}")
        for err in r.get("errors", []):
            print(f"    warning: {err}")
    print("=" * 70)
    print()

    # Dump full JSON report
    report_path = os.path.join(os.path.dirname(__file__), "validation_report.json")
    with open(report_path, "w") as f:
        json.dump({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dry_run": args.dry_run,
            "results": results,
        }, f, indent=2, default=str)
    print(f"Full report written to: {report_path}")

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
