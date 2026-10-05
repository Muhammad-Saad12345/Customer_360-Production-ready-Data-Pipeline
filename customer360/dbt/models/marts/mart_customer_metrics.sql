{{ config(
    materialized='table',
    table_type='iceberg'
) }}

/*
=============================================================================
BUSINESS KPI DEFINITIONS:
1. Total Customers: Distinct count of unified Master Customer IDs (dim_customer).
2. Buyers: Customers with at least 1 completed order.
3. Repeat Customers: Customers with >= 2 completed orders.
4. Repeat Purchase Rate: Repeat Customers / Total Buyers.
5. Active Customers: Customers who placed an order in the last 90 days or opened an email in 30 days.
6. Dormant Customers: Customers with no order in > 180 days.
7. Blended AOV (Average Order Value): Total Net Revenue / Total Completed Orders.
8. Average CLV: Mean estimated lifetime value across all buyers.
9. Churn Vulnerability Rate: High Risk Customers / Total Buyers.
=============================================================================
*/

with c360 as (
    select * from {{ ref('customer_360') }}
)

select
    rfm_customer_segment,
    customer_churn_risk,
    count(distinct master_customer_id) as customer_count,
    sum(total_completed_orders) as total_completed_orders,
    round(sum(total_net_revenue), 2) as aggregate_net_revenue,
    case
        when sum(total_completed_orders) > 0
        then round(sum(total_net_revenue) / sum(total_completed_orders), 2)
        else 0.0
    end as cohort_aov,
    round(avg(estimated_clv), 2) as avg_estimated_clv,
    round(avg(loyalty_points_balance), 1) as avg_loyalty_points,
    round(avg(average_csat_score), 2) as avg_csat_score,
    sum(case when total_completed_orders >= 2 then 1 else 0 end) as repeat_buyer_count,
    sum(case when total_completed_orders = 1 then 1 else 0 end) as one_time_buyer_count,
    round(
        cast(sum(case when total_completed_orders >= 2 then 1 else 0 end) as double) /
        nullif(sum(case when total_completed_orders >= 1 then 1 else 0 end), 0) * 100.0,
        2
    ) as repeat_purchase_rate_pct,
    cast(current_timestamp as timestamp) as calculation_timestamp
from c360
group by rfm_customer_segment, customer_churn_risk
