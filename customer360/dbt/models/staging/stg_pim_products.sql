{{ config(materialized='view') }}

with source_products as (
    select * from {{ source('iceberg_silver', 'pim_products') }}
),

staged as (
    select
        cast(product_id as varchar) as product_id,
        trim(sku) as sku,
        trim(name) as product_name,
        lower(trim(category)) as category,
        trim(brand) as brand,
        cast(coalesce(price, 0.0) as double) as retail_price,
        cast(coalesce(cost, 0.0) as double) as unit_cost,
        attributes as attributes_json,
        coalesce(lower(trim(status)), 'active') as product_status,
        cast(launch_date as varchar) as launch_date_raw,
        cast(updated_at as varchar) as updated_at_raw,
        'pim' as source_system,
        run_id,
        ingestion_timestamp,
        ingestion_date
    from source_products
    where product_id is not null
)

select * from staged
