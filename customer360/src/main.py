from __future__ import annotations

import argparse
import os
import sys
import uuid
from typing import Optional

# Ensure project root is always in sys.path whether executed via python src/main.py or python -m src.main
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import boto3
from src.config.settings import get_settings
from src.storage.s3_client import default_s3_client
from src.ingestion.hubspot import HubSpotIngest
from src.ingestion.oms import OmsIngest
from src.ingestion.pim import PimIngest
from src.ingestion.loyalty import LoyaltyIngest
from src.ingestion.marketing import MarketingIngest
from src.ingestion.support import ZendeskIngest
from src.processing.silver_gate import HubSpotSilverProcessor
from src.lakehouse.iceberg_loader import IcebergLoader
from src.orchestration.customer360_pipeline import Customer360PipelineRunner


def validate_aws():
    """Verify AWS S3 connectivity and credentials."""
    settings = get_settings()
    s3 = default_s3_client()
    ok = s3.bucket_exists()
    if not ok:
        print(f"S3 bucket '{settings.S3_BUCKET}' not found or inaccessible")
        sys.exit(2)
    print(f"S3 bucket '{settings.S3_BUCKET}' exists and is accessible")


def run_hubspot():
    """Ingest HubSpot customer/contact data. HubSpot = CONTACTS ONLY."""
    print(">>> Running HubSpot Contacts Ingestion...")
    ing = HubSpotIngest()
    ing.run()
    print(">>> HubSpot Contacts Ingestion completed successfully.")


def run_oms():
    """Ingest OMS order data. OMS = ORDER DATA."""
    print(">>> Running OMS Orders Ingestion...")
    OmsIngest().run()
    print(">>> OMS Orders Ingestion completed successfully.")


def run_pim():
    """Ingest PIM product catalog data. PIM = PRODUCT DATA."""
    print(">>> Running PIM Products Ingestion...")
    PimIngest().run()
    print(">>> PIM Products Ingestion completed successfully.")


def run_loyalty():
    """Ingest loyalty/rewards data. Loyalty = REWARDS/POINTS."""
    print(">>> Running Loyalty Ingestion...")
    LoyaltyIngest().run()
    print(">>> Loyalty Ingestion completed successfully.")


def run_marketing():
    """Ingest marketing campaign events data."""
    print(">>> Running Marketing Campaign Events Ingestion...")
    MarketingIngest().run()
    print(">>> Marketing Ingestion completed successfully.")


def run_support():
    """Ingest Zendesk support tickets data."""
    print(">>> Running Zendesk Support Tickets Ingestion...")
    ZendeskIngest().run()
    print(">>> Zendesk Support Ingestion completed successfully.")


def run_all_sources():
    """Run all source ingestion connectors according to source ownership rules."""
    print("==================================================")
    print("Ingesting all authoritative source systems...")
    print("==================================================")
    run_hubspot()
    run_oms()
    run_pim()
    run_loyalty()
    run_marketing()
    run_support()
    print("All source ingestions finished.")


def find_latest_run_id_in_s3(source: str, entity: str, tier: str = "raw") -> Optional[str]:
    """Inspect S3 bucket to locate the most recent run_id for given source and entity."""
    settings = get_settings()
    s3 = boto3.client("s3", region_name=settings.AWS_REGION)
    prefix = f"{tier}/{source}/{entity}/"
    paginator = s3.get_paginator("list_objects_v2")
    latest_item = None
    for page in paginator.paginate(Bucket=settings.S3_BUCKET, Prefix=prefix):
        for item in page.get("Contents", []):
            if latest_item is None or item["LastModified"] > latest_item["LastModified"]:
                latest_item = item
    if not latest_item:
        return None
    for part in latest_item["Key"].split("/"):
        if part.startswith("run_id="):
            return part.split("=", 1)[1]
    return None


def run_silver_gate(run_id: Optional[str] = None):
    """Execute Silver Gate processing across active entities."""
    print("==================================================")
    print("Processing Raw -> Silver Quality Gate...")
    print("==================================================")
    processor = HubSpotSilverProcessor()
    entities = [
        ("hubspot", "contacts"),
        ("oms", "orders"),
        ("pim", "products"),
        ("loyalty", "loyalty"),
        ("marketing", "campaign_events"),
    ]
    for source, entity in entities:
        effective_run_id = run_id or find_latest_run_id_in_s3(source, entity, tier="raw")
        if not effective_run_id:
            print(f"> Skipping {source}/{entity}: No raw records found in S3.")
            continue
        print(f"> Processing Silver for {source}/{entity} (run_id={effective_run_id})...")
        try:
            summary = processor.process_records(source=source, entity=entity, run_id=effective_run_id)
            print(f"  Valid: {summary.get('valid_count', 0)}, Quarantined: {summary.get('invalid_count', 0)}")
            print(f"  Silver Parquet: {summary.get('silver_path')}")
        except Exception as e:
            print(f"  [ERROR] {source}/{entity}: {e}")


def run_iceberg_load(run_id: Optional[str] = None, dry_run: bool = False):
    """Execute Iceberg Loader for all active tables."""
    print("==================================================")
    print(f"Loading into Apache Iceberg Tables (dry_run={dry_run})...")
    print("==================================================")
    loader = IcebergLoader()
    entities = [
        ("hubspot", "contacts"),
        ("oms", "orders"),
        ("pim", "products"),
        ("loyalty", "loyalty"),
        ("marketing", "campaign_events"),
    ]
    for source, entity in entities:
        effective_run_id = run_id or find_latest_run_id_in_s3(source, entity, tier="silver")
        if not effective_run_id:
            print(f"> Skipping {source}/{entity}: No Silver parquet found in S3.")
            continue
        print(f"> Iceberg load for {source}/{entity} (run_id={effective_run_id})...")
        try:
            summary = loader.load_silver_run(source=source, entity=entity, run_id=effective_run_id, dry_run=dry_run)
            print(f"  Status: {summary.get('status')}, Records processed: {summary.get('silver_row_count', 0)}")
        except Exception as e:
            print(f"  [ERROR] {source}/{entity}: {e}")


def run_hourly_pipeline():
    """Run hourly orchestrator."""
    Customer360PipelineRunner().run_hourly()


def run_full_load_pipeline():
    """Run full load orchestrator."""
    Customer360PipelineRunner().run_full_load()


def main():
    parser = argparse.ArgumentParser(description="customer360 ingestion runner")
    parser.add_argument(
        "job",
        choices=[
            "validate",
            "hubspot",       # HubSpot contacts ONLY
            "oms",           # OMS orders
            "pim",           # PIM products
            "loyalty",       # Loyalty rewards
            "marketing",     # Marketing campaign events
            "support",       # Zendesk support tickets
            "all-sources",   # Ingest all sources in sequence
            "silver-gate",   # Run Silver quality gate on all active sources
            "iceberg-load",  # Load Silver records into Apache Iceberg
            "pipeline-hourly",
            "pipeline-full-load",
        ],
        help="job to run",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Specific run_id to process for silver-gate or iceberg-load (defaults to latest in S3)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform dry-run without modifying Iceberg tables or executing Athena MERGE",
    )
    args = parser.parse_args()

    if args.job == "validate":
        validate_aws()
    elif args.job == "hubspot":
        run_hubspot()
    elif args.job == "oms":
        run_oms()
    elif args.job == "pim":
        run_pim()
    elif args.job == "loyalty":
        run_loyalty()
    elif args.job == "marketing":
        run_marketing()
    elif args.job == "support":
        run_support()
    elif args.job == "all-sources":
        run_all_sources()
    elif args.job == "silver-gate":
        run_silver_gate(run_id=args.run_id)
    elif args.job == "iceberg-load":
        run_iceberg_load(run_id=args.run_id, dry_run=args.dry_run)
    elif args.job == "pipeline-hourly":
        run_hourly_pipeline()
    elif args.job == "pipeline-full-load":
        run_full_load_pipeline()


if __name__ == "__main__":
    main()
