-- Revenue must survive the journey fact_order -> customer_360 -> executive KPIs.
-- Any difference means orders were lost or duplicated by identity resolution.
with fact as (
    select round(coalesce(sum(net_total_amount), 0), 2) as revenue, count(*) as orders
    from {{ ref('fact_order') }} where is_revenue_recognized = 1
),
c360 as (
    -- customer_360 rounds each customer to cents, so allow half a cent per customer
    select round(coalesce(sum(total_net_revenue), 0), 2) as revenue, coalesce(sum(total_completed_orders), 0) as orders,
           0.01 + 0.005 * count(*) as tolerance
    from {{ ref('customer_360') }}
),
kpi as (
    select round(coalesce(enterprise_net_revenue, 0), 2) as revenue, coalesce(total_orders_fulfilled, 0) as orders
    from {{ ref('mart_executive_kpis') }}
)
select fact.revenue as fact_revenue, c360.revenue as c360_revenue, kpi.revenue as kpi_revenue,
       fact.orders as fact_orders, c360.orders as c360_orders, kpi.orders as kpi_orders
from fact cross join c360 cross join kpi
where abs(fact.revenue - c360.revenue) > c360.tolerance
   or abs(fact.revenue - kpi.revenue) > c360.tolerance
   or fact.orders <> c360.orders
   or fact.orders <> kpi.orders
