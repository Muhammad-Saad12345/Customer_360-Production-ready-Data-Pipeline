-- Each source key must map to exactly one master customer; a fan-out would
-- duplicate orders in fact_order and double-count revenue.
select source_system, source_customer_id, count(*) as master_ids
from {{ ref('bridge_customer_identity') }}
group by source_system, source_customer_id
having count(*) > 1
