{{
    config(
        materialized='view',
        tags=['staging', 'core']
    )
}}

with source as (
    select * from {{ source('fintrack_raw', 'raw_tenants') }}
)

select
    tenant_id,
    tenant_code,
    tenant_name,
    country_code,
    devise_locale,
    date_onboarding,
    sla_freshness_hours,
    contract_tier,
    is_active,
    _loaded_at

from source
