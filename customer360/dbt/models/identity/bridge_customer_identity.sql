{{ config(
    materialized='table',
    table_type='iceberg'
) }}

/*
Deterministic identity resolution: exactly ONE row per (source_system, source_customer_id).

Rules, strongest first:
  1. HubSpot contact                                   -> itself            1.00 authoritative_crm_primary
  2. source id equals a HubSpot contact id             -> that contact      1.00 authoritative_id_match
  3. the record's own email matches one CRM contact    -> that contact      0.95 exact_email_match
  4. the record's own phone matches one CRM contact    -> that contact      0.90 exact_phone_match
  5. another non-CRM source carries the SAME customer id
     together with an email that matches a CRM contact -> that contact      0.85 shared_customer_id_email_match
     (e.g. a loyalty member C-1001 links through the OMS order of C-1001
      that carries customer1001@...; this is one hop of label propagation)
  6. otherwise                                          -> own singleton     0.50 unresolved_singleton_exception

When an email or phone matches several CRM contacts the lowest contact id
wins, so a key can never fan out into several master ids (which would
double-count revenue in fact_order).
*/

with crm as (
    select hubspot_contact_id, email, phone
    from {{ ref('stg_hubspot_contacts') }}
),

crm_by_email as (
    select email, min(hubspot_contact_id) as contact_id
    from crm where email is not null group by email
),

crm_by_phone as (
    select phone, min(hubspot_contact_id) as contact_id
    from crm where phone is not null and length(phone) >= 7 group by phone
),

keys as (
    -- one row per source key, with the first email/phone seen for it
    select
        source_system,
        source_customer_id,
        min(lower(trim(email))) as email,
        min(trim(phone)) as phone
    from {{ ref('int_customer_identities_unioned') }}
    where source_customer_id is not null
    group by source_system, source_customer_id
),

shared_id_email as (
    -- emails attached to a customer id by any non-CRM source
    select source_customer_id, min(email) as email
    from keys
    where source_system <> 'hubspot' and email is not null
    group by source_customer_id
),

candidates as (
    select
        k.source_system,
        k.source_customer_id,
        case when k.source_system = 'hubspot' then k.source_customer_id end as r1,
        by_id.hubspot_contact_id as r2,
        by_email.contact_id as r3,
        by_phone.contact_id as r4,
        shared_crm.contact_id as r5
    from keys k
    left join (select distinct hubspot_contact_id from crm) by_id
        on k.source_system <> 'hubspot' and k.source_customer_id = by_id.hubspot_contact_id
    left join crm_by_email by_email on k.email = by_email.email
    left join crm_by_phone by_phone on k.phone = by_phone.phone
    left join shared_id_email shared
        on k.source_system <> 'hubspot' and k.source_customer_id = shared.source_customer_id
    left join crm_by_email shared_crm on shared.email = shared_crm.email
),

resolved as (
    select
        source_system,
        source_customer_id,
        case
            when r1 is not null then concat('MCID_', r1)
            when r2 is not null then concat('MCID_', r2)
            when r3 is not null then concat('MCID_', r3)
            when r4 is not null then concat('MCID_', r4)
            when r5 is not null then concat('MCID_', r5)
            else concat('MCID_', upper(source_system), '_', source_customer_id)
        end as master_customer_id,
        case
            when r1 is not null then 'authoritative_crm_primary'
            when r2 is not null then 'authoritative_id_match'
            when r3 is not null then 'exact_email_match'
            when r4 is not null then 'exact_phone_match'
            when r5 is not null then 'shared_customer_id_email_match'
            else 'unresolved_singleton_exception'
        end as match_method,
        case
            when r1 is not null or r2 is not null then 1.00
            when r3 is not null then 0.95
            when r4 is not null then 0.90
            when r5 is not null then 0.85
            else 0.50
        end as match_confidence
    from candidates
)

select
    master_customer_id,
    source_system,
    source_customer_id,
    match_method,
    match_confidence,
    cast(current_timestamp as timestamp) as matched_at,
    'v2.0-deterministic' as rule_version
from resolved
