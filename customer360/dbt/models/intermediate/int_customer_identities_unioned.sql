{{ config(materialized='view') }}

with hubspot_ids as (
    select
        source_system,
        source_customer_id,
        email,
        phone,
        first_name,
        last_name,
        created_at_raw as identity_observed_at
    from {{ ref('stg_hubspot_contacts') }}
),

oms_ids as (
    select distinct
        source_system,
        customer_id as source_customer_id,
        cast(null as varchar) as email,
        cast(null as varchar) as phone,
        cast(null as varchar) as first_name,
        cast(null as varchar) as last_name,
        order_timestamp_raw as identity_observed_at
    from {{ ref('stg_oms_orders') }}
    where customer_id is not null
),

loyalty_ids as (
    select distinct
        source_system,
        customer_id as source_customer_id,
        cast(null as varchar) as email,
        cast(null as varchar) as phone,
        cast(null as varchar) as first_name,
        cast(null as varchar) as last_name,
        updated_at_raw as identity_observed_at
    from {{ ref('stg_loyalty') }}
    where customer_id is not null
),

marketing_ids as (
    select distinct
        source_system,
        customer_id as source_customer_id,
        recipient_email as email,
        cast(null as varchar) as phone,
        cast(null as varchar) as first_name,
        cast(null as varchar) as last_name,
        send_timestamp_raw as identity_observed_at
    from {{ ref('stg_marketing_campaign_events') }}
    where customer_id is not null or recipient_email is not null
),

zendesk_ids as (
    select distinct
        source_system,
        customer_id as source_customer_id,
        cast(null as varchar) as email,
        cast(null as varchar) as phone,
        cast(null as varchar) as first_name,
        cast(null as varchar) as last_name,
        created_at_raw as identity_observed_at
    from {{ ref('stg_zendesk_tickets') }}
    where customer_id is not null
),

unioned as (
    select * from hubspot_ids
    union all
    select * from oms_ids
    union all
    select * from loyalty_ids
    union all
    select * from marketing_ids
    union all
    select * from zendesk_ids
)

select * from unioned
