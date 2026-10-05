{{ config(
    materialized='table',
    table_type='iceberg'
) }}

with orders as (
    select * from {{ ref('stg_oms_orders') }}
),

bridge as (
    select distinct
        source_system,
        source_customer_id,
        master_customer_id
    from {{ ref('bridge_customer_identity') }}
    where source_system = 'oms'
)

select
    o.order_id,
    o.customer_id as source_customer_id,
    coalesce(b.master_customer_id, concat('MCID_OMS_', o.customer_id)) as master_customer_id,
    o.order_timestamp_raw,
    o.order_status,
    case
        when o.order_status in ('paid', 'shipped', 'fulfilled') then 'completed'
        when o.order_status in ('cancelled', 'refunded', 'failed') then 'reversed'
        else 'in_progress'
    end as order_disposition,
    case
        when o.order_status in ('paid', 'shipped', 'fulfilled') then 1
        else 0
    end as is_revenue_recognized,
    o.subtotal_amount,
    o.tax_amount,
    o.shipping_amount,
    o.discount_amount,
    o.net_total_amount,
    o.currency_code,
    o.sales_channel,
    o.updated_at_raw,
    'oms' as source_system,
    cast(current_timestamp as timestamp) as fact_created_at
from orders o
left join bridge b
    on o.customer_id = b.source_customer_id
