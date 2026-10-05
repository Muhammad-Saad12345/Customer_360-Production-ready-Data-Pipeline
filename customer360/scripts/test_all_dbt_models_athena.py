"""
Script to validate and test all 14 dbt models on Athena with actual Iceberg data.
"""
from __future__ import annotations

import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import re
import time
import boto3

from src.config.settings import get_settings

s = get_settings()
athena = boto3.client('athena', region_name=s.AWS_REGION)
MODELS_DIR = Path(__file__).resolve().parents[1] / 'dbt' / 'models'


def compile_dbt_sql(sql: str) -> str:
    # replace {{ source('iceberg_silver', 'table_name') }} -> table_name
    sql = re.sub(r"\{\{\s*source\s*\(\s*['\"][^'\"]+['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}", r"\1", sql)
    # replace {{ ref('model_name') }} -> model_name
    sql = re.sub(r"\{\{\s*ref\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}", r"\1", sql)
    # remove {{ config(...) }}
    sql = re.sub(r"\{\{\s*config\s*\([^)]*\)\s*\}\}", "", sql)
    # remove any remaining jinja tags
    sql = re.sub(r"\{%[^%]*%\}|\{\{[^}]*\}\}", "", sql)
    return sql.strip()


def run_athena_sql(sql: str, desc: str):
    print(f"Testing: {desc}...", flush=True)
    res = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={'Database': s.ICEBERG_DATABASE},
        ResultConfiguration={'OutputLocation': f's3://{s.S3_BUCKET}/athena-results/'}
    )
    qid = res['QueryExecutionId']
    for _ in range(45):
        status = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
    if status != 'SUCCEEDED':
        err = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
        print(f"  ❌ FAILED: {err}", flush=True)
        return False, err
    results = athena.get_query_results(QueryExecutionId=qid)
    rows = results['ResultSet']['Rows']
    row_count = max(0, len(rows) - 1)
    print(f"  ✅ SUCCESS ({row_count} rows returned)", flush=True)
    return True, rows


def main():
    models = [
        ('staging/stg_hubspot_contacts.sql', 'stg_hubspot_contacts'),
        ('staging/stg_oms_orders.sql', 'stg_oms_orders'),
        ('staging/stg_pim_products.sql', 'stg_pim_products'),
        ('staging/stg_loyalty.sql', 'stg_loyalty'),
        ('staging/stg_marketing_campaign_events.sql', 'stg_marketing_campaign_events'),
        ('staging/stg_zendesk_tickets.sql', 'stg_zendesk_tickets'),
        ('intermediate/int_customer_identities_unioned.sql', 'int_customer_identities_unioned'),
        ('identity/bridge_customer_identity.sql', 'bridge_customer_identity'),
        ('dimensions/dim_date.sql', 'dim_date'),
        ('dimensions/dim_product.sql', 'dim_product'),
        ('dimensions/dim_campaign.sql', 'dim_campaign'),
        ('dimensions/dim_customer.sql', 'dim_customer'),
        ('facts/fact_order.sql', 'fact_order'),
        ('facts/fact_support_case.sql', 'fact_support_case'),
        ('facts/fact_campaign_engagement.sql', 'fact_campaign_engagement'),
        ('facts/fact_loyalty_activity.sql', 'fact_loyalty_activity'),
        ('marts/customer_360.sql', 'customer_360'),
        ('marts/mart_customer_metrics.sql', 'mart_customer_metrics'),
        ('marts/mart_executive_kpis.sql', 'mart_executive_kpis'),
    ]

    print("=" * 70)
    print("EXECUTING AND VALIDATING ALL DBT SQL MODELS ON ATHENA / ICEBERG")
    print("=" * 70)

    # First, let's materialize staging views / CTEs in Athena or test them directly
    compiled_dict = {}
    for rel_path, name in models:
        raw_sql = (MODELS_DIR / rel_path).read_text(encoding='utf-8')
        compiled_dict[name] = compile_dbt_sql(raw_sql)

    # 1. Staging models
    staging_models = [
        ('staging/stg_hubspot_contacts.sql', 'stg_hubspot_contacts'),
        ('staging/stg_oms_orders.sql', 'stg_oms_orders'),
        ('staging/stg_pim_products.sql', 'stg_pim_products'),
        ('staging/stg_loyalty.sql', 'stg_loyalty'),
        ('staging/stg_marketing_campaign_events.sql', 'stg_marketing_campaign_events'),
        ('staging/stg_zendesk_tickets.sql', 'stg_zendesk_tickets'),
        ('dimensions/dim_date.sql', 'dim_date'),
    ]

    for rel_path, name in staging_models:
        sql = f"{compiled_dict[name]}\nLIMIT 5"
        run_athena_sql(sql, f"Model: {name}")

    # 2. Intermediate model: int_customer_identities_unioned
    # We create views or flatten CTEs for downstream models
    print("\n--- Testing Materialization of Views in customer360_iceberg ---")
    for rel_path, name in models:
        view_sql = f"CREATE OR REPLACE VIEW customer360_iceberg.{name} AS\n{compiled_dict[name]}"
        success, _ = run_athena_sql(view_sql, f"Create View: {name}")
        if success:
            run_athena_sql(f"SELECT * FROM customer360_iceberg.{name} LIMIT 5", f"Query View: {name}")


if __name__ == '__main__':
    main()
