{{ config(materialized='view') }}

with source_contacts as (
    select * from {{ source('iceberg_silver', 'hubspot_contacts') }}
),

staged as (
    select
        cast(customer_id as varchar) as hubspot_contact_id,
        cast(customer_id as varchar) as source_customer_id,
        'hubspot' as source_system,
        lower(trim(email)) as email,
        trim(phone) as phone,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        concat(coalesce(trim(first_name), ''), case when first_name is not null and last_name is not null then ' ' else '' end, coalesce(trim(last_name), '')) as full_name,
        coalesce(lower(trim(customer_stage)), 'unknown') as customer_stage,
        cast(created_at as varchar) as created_at_raw,
        cast(updated_at as varchar) as updated_at_raw,
        cast(owner_id as varchar) as owner_id,
        run_id,
        ingestion_timestamp,
        ingestion_date
    from source_contacts
    where customer_id is not null and email is not null
)

select * from staged
