{{ config(
    materialized='table',
    table_type='iceberg'
) }}

with crm_contacts as (
    select
        hubspot_contact_id,
        email,
        phone,
        first_name,
        last_name
    from {{ ref('stg_hubspot_contacts') }}
),

all_sources as (
    select distinct
        source_system,
        source_customer_id,
        email,
        phone
    from {{ ref('int_customer_identities_unioned') }}
    where source_customer_id is not null
),

resolved as (
    select
        all_sources.source_system,
        all_sources.source_customer_id,
        case
            -- Rule 1: Direct CRM Authoritative Match (HubSpot Contact ID matches source_customer_id)
            when all_sources.source_system = 'hubspot'
                then concat('MCID_', all_sources.source_customer_id)
            when crm_by_id.hubspot_contact_id is not null
                then concat('MCID_', crm_by_id.hubspot_contact_id)

            -- Rule 2: Normalized Exact Email Match
            when all_sources.email is not null and crm_by_email.hubspot_contact_id is not null
                then concat('MCID_', crm_by_email.hubspot_contact_id)

            -- Rule 3: Normalized Exact Phone Match
            when all_sources.phone is not null and crm_by_phone.hubspot_contact_id is not null
                then concat('MCID_', crm_by_phone.hubspot_contact_id)

            -- Rule 4 / Exception: Unresolved Singleton Entity
            else concat('MCID_', upper(all_sources.source_system), '_', all_sources.source_customer_id)
        end as master_customer_id,

        case
            when all_sources.source_system = 'hubspot'
                then 'authoritative_crm_primary'
            when crm_by_id.hubspot_contact_id is not null
                then 'authoritative_id_match'
            when all_sources.email is not null and crm_by_email.hubspot_contact_id is not null
                then 'exact_email_match'
            when all_sources.phone is not null and crm_by_phone.hubspot_contact_id is not null
                then 'exact_phone_match'
            else 'unresolved_singleton_exception'
        end as match_method,

        case
            when all_sources.source_system = 'hubspot' then 1.00
            when crm_by_id.hubspot_contact_id is not null then 1.00
            when all_sources.email is not null and crm_by_email.hubspot_contact_id is not null then 0.95
            when all_sources.phone is not null and crm_by_phone.hubspot_contact_id is not null then 0.90
            else 0.50
        end as match_confidence,

        cast(current_timestamp as timestamp) as matched_at,
        'v1.0-deterministic' as rule_version

    from all_sources
    left join crm_contacts as crm_by_id
        on all_sources.source_customer_id = crm_by_id.hubspot_contact_id
    left join crm_contacts as crm_by_email
        on all_sources.email is not null
        and all_sources.email = crm_by_email.email
    left join crm_contacts as crm_by_phone
        on all_sources.phone is not null
        and all_sources.phone = crm_by_phone.phone
)

select
    master_customer_id,
    source_system,
    source_customer_id,
    match_method,
    match_confidence,
    matched_at,
    rule_version
from resolved
