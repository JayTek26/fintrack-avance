{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dim']
    )
}}

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
    datediff('day', date_onboarding, current_date()) as anciennete_partenariat_jours

from {{ ref('stg_tenants') }}
