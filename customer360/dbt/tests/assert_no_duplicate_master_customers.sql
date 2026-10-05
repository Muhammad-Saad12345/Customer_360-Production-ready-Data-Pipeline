-- Returns master_customer_id values that appear more than once in dim_customer.
-- Golden record must guarantee strict 1:1 master customer uniqueness.
select
    master_customer_id,
    count(*) as record_count
from {{ ref('dim_customer') }}
group by master_customer_id
having count(*) > 1
