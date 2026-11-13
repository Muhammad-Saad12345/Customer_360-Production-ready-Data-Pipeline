{{ config(
    materialized='table',
    table_type='iceberg'
) }}

/*
Daily trading KPIs, one row per order date and currency.
  orders_placed        all orders created that day
  orders_recognized    paid / shipped / fulfilled
  orders_reversed      cancelled / refunded / failed
  recognized_revenue   sum of net_total for recognised orders
  aov                  recognized_revenue / orders_recognized
  buyers               distinct master customers with a recognised order
Revenue is not converted between currencies; filter or group by currency.
*/

with orders as (
    select
        try(date(substr(order_timestamp_raw, 1, 10))) as order_date,
        currency_code,
        order_id,
        master_customer_id,
        order_disposition,
        is_revenue_recognized,
        net_total_amount
    from {{ ref('fact_order') }}
    where order_timestamp_raw is not null
      and try(date(substr(order_timestamp_raw, 1, 10))) is not null
)

select
    o.order_date,
    d.calendar_year,
    d.calendar_month,
    d.day_name,
    o.currency_code,
    count(*) as orders_placed,
    sum(o.is_revenue_recognized) as orders_recognized,
    sum(case when o.order_disposition = 'reversed' then 1 else 0 end) as orders_reversed,
    round(sum(case when o.is_revenue_recognized = 1 then o.net_total_amount else 0 end), 2) as recognized_revenue,
    round(
        sum(case when o.is_revenue_recognized = 1 then o.net_total_amount else 0 end)
        / nullif(sum(o.is_revenue_recognized), 0), 2
    ) as aov,
    count(distinct case when o.is_revenue_recognized = 1 then o.master_customer_id end) as buyers,
    cast(current_timestamp as timestamp) as calculated_at
from orders o
left join {{ ref('dim_date') }} d on o.order_date = d.calendar_date
group by o.order_date, d.calendar_year, d.calendar_month, d.day_name, o.currency_code
