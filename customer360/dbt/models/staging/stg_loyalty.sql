{{ config(materialized='view') }}

with source_loyalty as (
    select * from {{ source('iceberg_silver', 'loyalty_loyalty') }}
),

staged as (
    select
        cast(loyalty_id as varchar) as loyalty_id,
        cast(customer_id as varchar) as customer_id,
        coalesce(lower(trim(tier)), 'bronze') as loyalty_tier,
        cast(coalesce(points_balance, 0.0) as double) as points_balance,
        cast(earn_ts as varchar) as last_earn_timestamp_raw,
        cast(redeem_ts as varchar) as last_redeem_timestamp_raw,
        trim(benefit_code) as active_benefit_code,
        cast(updated_at as varchar) as updated_at_raw,
        'loyalty' as source_system,
        run_id,
        ingestion_timestamp,
        ingestion_date
    from source_loyalty
    where loyalty_id is not null
)

select * from staged
