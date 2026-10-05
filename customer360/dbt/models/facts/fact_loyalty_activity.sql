{{ config(
    materialized='table',
    table_type='iceberg'
) }}

with loyalty as (
    select * from {{ ref('stg_loyalty') }}
),

bridge as (
    select distinct
        source_system,
        source_customer_id,
        master_customer_id
    from {{ ref('bridge_customer_identity') }}
    where source_system = 'loyalty'
)

select
    l.loyalty_id,
    l.customer_id as source_customer_id,
    coalesce(b.master_customer_id, concat('MCID_LOYALTY_', l.customer_id)) as master_customer_id,
    l.loyalty_tier,
    l.points_balance,
    l.last_earn_timestamp_raw,
    l.last_redeem_timestamp_raw,
    l.active_benefit_code,
    l.updated_at_raw,
    'loyalty' as source_system,
    cast(current_timestamp as timestamp) as fact_created_at
from loyalty l
left join bridge b
    on l.customer_id = b.source_customer_id
