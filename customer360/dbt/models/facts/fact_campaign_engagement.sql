{{ config(
    materialized='table',
    table_type='iceberg'
) }}

with events as (
    select * from {{ ref('stg_marketing_campaign_events') }}
),

bridge as (
    select distinct
        source_system,
        source_customer_id,
        master_customer_id
    from {{ ref('bridge_customer_identity') }}
    where source_system = 'marketing'
)

select
    e.event_id,
    e.campaign_id,
    e.customer_id as source_customer_id,
    coalesce(b.master_customer_id, concat('MCID_MARKETING_', coalesce(e.customer_id, e.marketing_member_id))) as master_customer_id,
    e.engagement_channel,
    e.send_timestamp_raw,
    e.open_timestamp_raw,
    e.click_timestamp_raw,
    case when e.open_timestamp_raw is not null then 1 else 0 end as is_opened,
    case when e.click_timestamp_raw is not null then 1 else 0 end as is_clicked,
    e.audience_id,
    e.offer_id,
    e.marketing_member_id,
    e.recipient_email,
    e.engagement_status,
    e.provider_type,
    'marketing' as source_system,
    cast(current_timestamp as timestamp) as fact_created_at
from events e
left join bridge b
    on e.customer_id = b.source_customer_id
