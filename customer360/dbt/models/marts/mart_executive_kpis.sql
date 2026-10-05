{{ config(
    materialized='table',
    table_type='iceberg'
) }}

/*
=============================================================================
EXECUTIVE KPI DASHBOARD MART
Consolidated high-level operational performance indicators across all 6 business domains.
=============================================================================
*/

with customer_base as (
    select
        count(distinct master_customer_id) as total_master_customers,
        count(distinct case when total_completed_orders > 0 then master_customer_id end) as total_purchasing_customers,
        sum(total_completed_orders) as total_orders_fulfilled,
        sum(total_net_revenue) as enterprise_net_revenue,
        avg(case when total_completed_orders > 0 then average_order_value end) as enterprise_aov,
        sum(case when total_completed_orders >= 2 then 1 else 0 end) as repeat_customers,
        sum(loyalty_points_balance) as outstanding_loyalty_points_liability,
        sum(total_marketing_touches) as total_marketing_impressions,
        sum(total_marketing_opens) as total_marketing_opens,
        sum(lifetime_support_tickets) as total_support_cases_logged,
        sum(open_support_cases) as current_open_backlog_tickets,
        avg(case when average_csat_score > 0 then average_csat_score end) as enterprise_csat_index
    from {{ ref('customer_360') }}
)

select
    total_master_customers,
    total_purchasing_customers,
    total_orders_fulfilled,
    round(enterprise_net_revenue, 2) as enterprise_net_revenue,
    round(enterprise_aov, 2) as enterprise_aov,
    round(
        cast(repeat_customers as double) / nullif(total_purchasing_customers, 0) * 100.0,
        2
    ) as repeat_purchase_rate_pct,
    round(outstanding_loyalty_points_liability, 0) as outstanding_loyalty_points_liability,
    total_marketing_impressions,
    round(
        cast(total_marketing_opens as double) / nullif(total_marketing_impressions, 0) * 100.0,
        2
    ) as blended_email_open_rate_pct,
    total_support_cases_logged,
    current_open_backlog_tickets,
    round(enterprise_csat_index, 2) as enterprise_csat_index,
    cast(current_timestamp as timestamp) as kpi_reported_at
from customer_base
