{{ config(
    materialized='table',
    table_type='iceberg'
) }}

with golden_customers as (
    select * from {{ ref('dim_customer') }}
),

identity_bridge as (
    select
        master_customer_id,
        max(match_confidence) as identity_match_confidence,
        max(match_method) as primary_match_method,
        max(rule_version) as identity_rule_version
    from {{ ref('bridge_customer_identity') }}
    group by master_customer_id
),

order_aggregates as (
    select
        master_customer_id,
        count(distinct order_id) as total_completed_orders,
        sum(net_total_amount) as total_net_revenue,
        min(order_timestamp_raw) as first_order_at,
        max(order_timestamp_raw) as latest_order_at,
        avg(net_total_amount) as average_order_value
    from {{ ref('fact_order') }}
    where is_revenue_recognized = 1
    group by master_customer_id
),

campaign_aggregates as (
    select
        master_customer_id,
        count(distinct event_id) as total_marketing_touches,
        sum(is_opened) as total_opens,
        sum(is_clicked) as total_clicks
    from {{ ref('fact_campaign_engagement') }}
    group by master_customer_id
),

support_aggregates as (
    select
        master_customer_id,
        count(distinct ticket_id) as lifetime_support_tickets,
        sum(is_open) as open_support_cases,
        sum(is_resolved) as resolved_support_cases,
        avg(case when csat_rating > 0 then csat_rating end) as average_csat_rating
    from {{ ref('fact_support_case') }}
    group by master_customer_id
),

c360_joined as (
    select
        c.master_customer_id,

        -- 1. Unified Customer Profile & Contact
        c.first_name,
        c.last_name,
        c.full_name,
        c.primary_email,
        c.primary_phone,
        c.customer_lifecycle_stage,
        c.account_owner_id,
        c.crm_created_at,

        -- 2. Identity Resolution Auditability
        coalesce(b.primary_match_method, 'unresolved_singleton_exception') as identity_match_method,
        coalesce(b.identity_match_confidence, 0.50) as identity_confidence_score,
        coalesce(b.identity_rule_version, 'v1.0-deterministic') as identity_rule_version,

        -- 3. Transactional & Revenue Metrics (OMS Authority)
        coalesce(o.total_completed_orders, 0) as total_completed_orders,
        round(coalesce(o.total_net_revenue, 0.0), 2) as total_net_revenue,
        round(coalesce(o.average_order_value, 0.0), 2) as average_order_value,
        o.first_order_at,
        o.latest_order_at,

        -- 4. Recency & Frequency Metrics
        case
            when o.latest_order_at is not null
            then date_diff('day', date(substr(o.latest_order_at, 1, 10)), current_date)
            else 999
        end as recency_days,

        -- 5. RFM Customer Segmentation
        case
            when coalesce(o.total_completed_orders, 0) >= 3 and coalesce(o.total_net_revenue, 0) >= 500
                then 'Champions / VIP'
            when coalesce(o.total_completed_orders, 0) >= 2 and coalesce(o.total_net_revenue, 0) >= 200
                then 'Loyal Customers'
            when coalesce(o.total_completed_orders, 0) = 1
                then 'New Buyers'
            when coalesce(o.total_completed_orders, 0) = 0 and c.customer_lifecycle_stage in ('lead', 'subscriber')
                then 'Active Prospects'
            else 'Dormant / Lapsed'
        end as rfm_customer_segment,

        -- 6. Loyalty Status (Loyalty Authority)
        c.loyalty_tier,
        c.loyalty_points_balance,
        c.active_loyalty_voucher,

        -- 7. Marketing Engagement (Mailchimp/Marketing Authority)
        coalesce(m.total_marketing_touches, 0) as total_marketing_touches,
        coalesce(m.total_opens, 0) as total_marketing_opens,
        coalesce(m.total_clicks, 0) as total_marketing_clicks,
        case
            when coalesce(m.total_marketing_touches, 0) > 0
            then round(cast(coalesce(m.total_opens, 0) as double) / m.total_marketing_touches * 100.0, 2)
            else 0.0
        end as marketing_open_rate_pct,

        -- 8. Support & Customer Satisfaction (Zendesk Authority)
        coalesce(s.lifetime_support_tickets, 0) as lifetime_support_tickets,
        coalesce(s.open_support_cases, 0) as open_support_cases,
        coalesce(s.resolved_support_cases, 0) as resolved_support_cases,
        round(coalesce(s.average_csat_rating, 0.0), 2) as average_csat_score,
        case when coalesce(s.open_support_cases, 0) > 0 then 1 else 0 end as has_active_ticket,

        -- 9. Churn Risk & Health Indicators
        case
            when coalesce(s.open_support_cases, 0) > 1 and coalesce(s.average_csat_rating, 5.0) < 3.0
                then 'High Risk'
            when o.latest_order_at is not null and date_diff('day', date(substr(o.latest_order_at, 1, 10)), current_date) > 180
                then 'Medium Risk'
            else 'Low Risk / Healthy'
        end as customer_churn_risk,

        -- 10. Customer Lifetime Value (CLV) Foundation
        round(coalesce(o.total_net_revenue, 0.0) + (coalesce(o.average_order_value, 50.0) * 1.5), 2) as estimated_clv,

        -- Lineage & Audit
        c.survivorship_authority_manifest,
        cast(current_timestamp as timestamp) as mart_updated_at

    from golden_customers c
    left join identity_bridge b on c.master_customer_id = b.master_customer_id
    left join order_aggregates o on c.master_customer_id = o.master_customer_id
    left join campaign_aggregates m on c.master_customer_id = m.master_customer_id
    left join support_aggregates s on c.master_customer_id = s.master_customer_id
)

select * from c360_joined
