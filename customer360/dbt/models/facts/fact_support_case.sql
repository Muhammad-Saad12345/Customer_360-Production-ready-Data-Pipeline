{{ config(
    materialized='table',
    table_type='iceberg'
) }}

with tickets as (
    select * from {{ ref('stg_zendesk_tickets') }}
),

bridge as (
    select distinct
        source_system,
        source_customer_id,
        master_customer_id
    from {{ ref('bridge_customer_identity') }}
    where source_system = 'zendesk'
)

select
    t.ticket_id,
    t.customer_id as source_customer_id,
    coalesce(b.master_customer_id, concat('MCID_ZENDESK_', t.customer_id)) as master_customer_id,
    t.support_channel,
    t.ticket_category,
    t.ticket_priority,
    t.ticket_status,
    case
        when t.ticket_status in ('solved', 'closed') then 1
        else 0
    end as is_resolved,
    case
        when t.ticket_status in ('new', 'open', 'pending', 'hold') then 1
        else 0
    end as is_open,
    t.assigned_agent_id,
    t.created_at_raw,
    t.resolved_at_raw,
    t.csat_rating,
    t.provider_type,
    'zendesk' as source_system,
    cast(current_timestamp as timestamp) as fact_created_at
from tickets t
left join bridge b
    on t.customer_id = b.source_customer_id
