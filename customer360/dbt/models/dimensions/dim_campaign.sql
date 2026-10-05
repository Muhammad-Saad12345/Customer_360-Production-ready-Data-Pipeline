{{ config(
    materialized='table',
    table_type='iceberg'
) }}

with staged_events as (
    select * from {{ ref('stg_marketing_campaign_events') }}
),

campaign_aggregates as (
    select
        campaign_id,
        min(send_timestamp_raw) as first_broadcast_at,
        max(send_timestamp_raw) as latest_broadcast_at,
        count(distinct event_id) as total_dispatched_messages,
        count(distinct case when open_timestamp_raw is not null then event_id end) as total_opens,
        count(distinct case when click_timestamp_raw is not null then event_id end) as total_clicks,
        count(distinct recipient_email) as unique_recipients,
        max(provider_type) as primary_provider
    from staged_events
    group by campaign_id
)

select
    campaign_id as campaign_key,
    campaign_id,
    first_broadcast_at,
    latest_broadcast_at,
    total_dispatched_messages,
    total_opens,
    total_clicks,
    unique_recipients,
    case
        when total_dispatched_messages > 0
        then round(cast(total_opens as double) / total_dispatched_messages * 100.0, 2)
        else 0.0
    end as open_rate_pct,
    case
        when total_opens > 0
        then round(cast(total_clicks as double) / total_opens * 100.0, 2)
        else 0.0
    end as click_through_rate_pct,
    primary_provider as marketing_platform,
    cast(current_timestamp as timestamp) as dimension_updated_at
from campaign_aggregates
