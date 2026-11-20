-- One row per order in fact_order.
select order_id, count(*) as rows_per_order
from {{ ref('fact_order') }}
group by order_id
having count(*) > 1
