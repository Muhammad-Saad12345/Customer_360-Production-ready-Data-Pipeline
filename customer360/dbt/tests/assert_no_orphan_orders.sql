-- Returns orders whose master_customer_id does not exist in dim_customer.
-- Under the lakehouse architecture, all orders must link to a valid golden customer record.
select
    o.order_id,
    o.master_customer_id
from {{ ref('fact_order') }} o
left join {{ ref('dim_customer') }} c
    on o.master_customer_id = c.master_customer_id
where c.master_customer_id is null
