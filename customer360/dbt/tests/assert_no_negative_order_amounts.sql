-- Returns records where net_total_amount is negative.
-- Under the data contract, order amounts must not be negative.
select
    order_id,
    net_total_amount
from {{ ref('fact_order') }}
where net_total_amount < 0.0
