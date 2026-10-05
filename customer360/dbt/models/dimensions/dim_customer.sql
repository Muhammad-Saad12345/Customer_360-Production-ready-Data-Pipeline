{{ config(
    materialized='table',
    table_type='iceberg'
) }}

/*
=============================================================================
SURVIVORSHIP RULES (Authoritative Source Hierarchy):
1. Profile & Demographics: HubSpot CRM (authoritative for name, email, stage, phone)
2. Loyalty & Rewards: Loyalty Ledger (authoritative for tier, balance, benefit code)
3. Transactional & Revenue: OMS Orders (authoritative for purchase dates, orders, spend)
4. Marketing Engagement: Mailchimp/Marketing (authoritative for sends, opens, clicks)
5. Support & Service History: Zendesk (authoritative for tickets, status, CSAT)
=============================================================================
*/

with bridge as (
    select distinct
        master_customer_id,
        source_system,
        source_customer_id,
        match_confidence
    from {{ ref('bridge_customer_identity') }}
),

crm_profiles as (
    select
        b.master_customer_id,
        c.first_name,
        c.last_name,
        c.full_name,
        c.email,
        c.phone,
        c.customer_stage,
        c.owner_id,
        c.created_at_raw as crm_created_at
    from bridge b
    join {{ ref('stg_hubspot_contacts') }} c
        on b.source_system = 'hubspot' and b.source_customer_id = c.hubspot_contact_id
),

loyalty_ledger as (
    select
        b.master_customer_id,
        l.loyalty_tier,
        l.points_balance,
        l.active_benefit_code,
        l.last_earn_timestamp_raw,
        l.last_redeem_timestamp_raw,
        row_number() over (partition by b.master_customer_id order by l.points_balance desc) as rn
    from bridge b
    join {{ ref('stg_loyalty') }} l
        on b.source_system = 'loyalty' and b.source_customer_id = l.customer_id
),

oms_summary as (
    select
        b.master_customer_id,
        count(distinct o.order_id) as lifetime_orders_count,
        sum(case when o.order_status in ('paid', 'shipped', 'fulfilled') then o.net_total_amount else 0.0 end) as lifetime_net_spend,
        min(case when o.order_status in ('paid', 'shipped', 'fulfilled') then o.order_timestamp_raw else null end) as first_order_timestamp,
        max(case when o.order_status in ('paid', 'shipped', 'fulfilled') then o.order_timestamp_raw else null end) as latest_order_timestamp
    from bridge b
    join {{ ref('stg_oms_orders') }} o
        on b.source_system = 'oms' and b.source_customer_id = o.customer_id
    group by b.master_customer_id
),

marketing_summary as (
    select
        b.master_customer_id,
        count(distinct m.event_id) as total_campaign_events,
        count(distinct case when m.open_timestamp_raw is not null then m.event_id end) as total_campaigns_opened,
        count(distinct case when m.click_timestamp_raw is not null then m.event_id end) as total_campaigns_clicked,
        max(coalesce(m.click_timestamp_raw, m.open_timestamp_raw, m.send_timestamp_raw)) as latest_marketing_engagement_ts
    from bridge b
    join {{ ref('stg_marketing_campaign_events') }} m
        on b.source_system = 'marketing' and b.source_customer_id = m.customer_id
    group by b.master_customer_id
),

support_summary as (
    select
        b.master_customer_id,
        count(distinct s.ticket_id) as total_support_tickets,
        count(distinct case when s.ticket_status in ('new', 'open', 'pending', 'hold') then s.ticket_id end) as open_support_tickets,
        count(distinct case when s.ticket_status in ('solved', 'closed') then s.ticket_id end) as resolved_support_tickets,
        avg(case when s.csat_rating > 0 then s.csat_rating else null end) as avg_csat_score
    from bridge b
    join {{ ref('stg_zendesk_tickets') }} s
        on b.source_system = 'zendesk' and b.source_customer_id = s.customer_id
    group by b.master_customer_id
),

all_masters as (
    select distinct master_customer_id from bridge
),

golden_customer as (
    select
        m.master_customer_id,
        -- Authoritative CRM Demographics
        coalesce(crm.first_name, 'Unknown') as first_name,
        coalesce(crm.last_name, 'Unknown') as last_name,
        coalesce(crm.full_name, 'Unknown Customer') as full_name,
        crm.email as primary_email,
        crm.phone as primary_phone,
        coalesce(crm.customer_stage, 'unregistered') as customer_lifecycle_stage,
        crm.owner_id as account_owner_id,
        crm.crm_created_at,

        -- Authoritative Loyalty Status
        coalesce(loy.loyalty_tier, 'standard') as loyalty_tier,
        coalesce(loy.points_balance, 0.0) as loyalty_points_balance,
        loy.active_benefit_code as active_loyalty_voucher,

        -- Authoritative Purchase Metrics
        coalesce(oms.lifetime_orders_count, 0) as lifetime_orders_count,
        coalesce(oms.lifetime_net_spend, 0.0) as lifetime_net_spend,
        case
            when coalesce(oms.lifetime_orders_count, 0) > 0
            then oms.lifetime_net_spend / oms.lifetime_orders_count
            else 0.0
        end as lifetime_aov,
        oms.first_order_timestamp,
        oms.latest_order_timestamp,

        -- Authoritative Marketing Engagement
        coalesce(mkt.total_campaign_events, 0) as total_campaign_events,
        coalesce(mkt.total_campaigns_opened, 0) as total_campaigns_opened,
        coalesce(mkt.total_campaigns_clicked, 0) as total_campaigns_clicked,
        mkt.latest_marketing_engagement_ts,

        -- Authoritative Support Metrics
        coalesce(sup.total_support_tickets, 0) as total_support_tickets,
        coalesce(sup.open_support_tickets, 0) as open_support_tickets,
        coalesce(sup.resolved_support_tickets, 0) as resolved_support_tickets,
        coalesce(sup.avg_csat_score, 0.0) as average_csat_score,

        -- Survivorship Metadata
        'CRM:HubSpot;Loyalty:Loyalty;Orders:OMS;Marketing:Mailchimp;Support:Zendesk' as survivorship_authority_manifest,
        cast(current_timestamp as timestamp) as record_created_at

    from all_masters m
    left join crm_profiles crm on m.master_customer_id = crm.master_customer_id
    left join (select * from loyalty_ledger where rn = 1) loy on m.master_customer_id = loy.master_customer_id
    left join oms_summary oms on m.master_customer_id = oms.master_customer_id
    left join marketing_summary mkt on m.master_customer_id = mkt.master_customer_id
    left join support_summary sup on m.master_customer_id = sup.master_customer_id
)

select * from golden_customer
