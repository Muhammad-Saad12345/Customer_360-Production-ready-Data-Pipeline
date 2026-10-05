-- Returns bridge records where match_confidence is outside the valid range [0.0, 1.0].
select
    master_customer_id,
    source_system,
    source_customer_id,
    match_confidence
from {{ ref('bridge_customer_identity') }}
where match_confidence < 0.0 or match_confidence > 1.0
