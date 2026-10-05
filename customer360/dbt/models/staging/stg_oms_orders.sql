{{ config(materialized='view') }}

with source_orders as (
    select * from {{ source('iceberg_silver', 'oms_orders') }}
),

staged as (
    select
        cast(order_id as varchar) as order_id,
        cast(customer_id as varchar) as customer_id,
        'oms' as source_system,
        cast(order_ts as varchar) as order_timestamp_raw,
        coalesce(lower(trim(order_status)), 'unknown') as order_status,
        cast(coalesce(subtotal, 0.0) as double) as subtotal_amount,
        cast(coalesce(tax, 0.0) as double) as tax_amount,
        cast(coalesce(shipping, 0.0) as double) as shipping_amount,
        cast(coalesce(discount, 0.0) as double) as discount_amount,
        cast(coalesce(net_total, 0.0) as double) as net_total_amount,
        coalesce(upper(trim(currency)), 'USD') as currency_code,
        coalesce(lower(trim(channel)), 'unknown') as sales_channel,
        cast(updated_at as varchar) as updated_at_raw,
        run_id,
        ingestion_timestamp,
        ingestion_date
    from source_orders
    where order_id is not null
)

select * from staged
