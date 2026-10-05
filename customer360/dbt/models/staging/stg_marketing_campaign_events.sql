{{ config(materialized='view') }}

with source_marketing as (
    select * from {{ source('iceberg_silver', 'marketing_campaign_events') }}
),

staged as (
    select
        cast(record_id as varchar) as event_id,
        cast(campaign_id as varchar) as campaign_id,
        cast(customer_id as varchar) as customer_id,
        coalesce(lower(trim(channel)), 'email') as engagement_channel,
        cast(send_ts as varchar) as send_timestamp_raw,
        cast(open_ts as varchar) as open_timestamp_raw,
        cast(click_ts as varchar) as click_timestamp_raw,
        cast(audience_id as varchar) as audience_id,
        trim(offer_id) as offer_id,
        cast(member_id as varchar) as marketing_member_id,
        lower(trim(email_address)) as recipient_email,
        coalesce(lower(trim(status)), 'unknown') as engagement_status,
        source_type as provider_type,
        'marketing' as source_system,
        run_id,
        ingestion_timestamp,
        ingestion_date
    from source_marketing
    where record_id is not null
)

select * from staged
