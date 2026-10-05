{{ config(
    materialized='table',
    table_type='iceberg'
) }}

with staged_products as (
    select * from {{ ref('stg_pim_products') }}
)

select
    product_id as product_key,
    product_id,
    sku,
    product_name,
    category as product_category,
    brand as product_brand,
    retail_price,
    unit_cost,
    round(retail_price - unit_cost, 2) as gross_margin_amount,
    case
        when retail_price > 0
        then round((retail_price - unit_cost) / retail_price * 100.0, 2)
        else 0.0
    end as gross_margin_percentage,
    attributes_json,
    product_status,
    launch_date_raw as commercial_launch_date,
    updated_at_raw as catalog_last_updated_at,
    'pim' as authoritative_source,
    cast(current_timestamp as timestamp) as dimension_updated_at
from staged_products
