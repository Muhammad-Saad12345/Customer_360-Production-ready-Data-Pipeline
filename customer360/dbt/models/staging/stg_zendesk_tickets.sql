{{ config(materialized='view') }}

with source_zendesk as (
    select * from {{ source('iceberg_silver', 'zendesk_tickets') }}
),

staged as (
    select
        cast(ticket_id as varchar) as ticket_id,
        cast(customer_id as varchar) as customer_id,
        coalesce(lower(trim(channel)), 'email') as support_channel,
        coalesce(lower(trim(category)), 'general') as ticket_category,
        coalesce(lower(trim(priority)), 'normal') as ticket_priority,
        coalesce(lower(trim(status)), 'open') as ticket_status,
        cast(agent_id as varchar) as assigned_agent_id,
        cast(created_at as varchar) as created_at_raw,
        cast(resolved_at as varchar) as resolved_at_raw,
        cast(coalesce(csat_score, 0.0) as double) as csat_rating,
        source_type as provider_type,
        'zendesk' as source_system,
        run_id,
        ingestion_timestamp,
        ingestion_date
    from source_zendesk
    where ticket_id is not null
)

select * from staged
