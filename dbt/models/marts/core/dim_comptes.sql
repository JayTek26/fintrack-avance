-- ============================================================
-- DIM COMPTES — snapshot le plus récent
-- ============================================================
-- La version historisée (SCD Type 2) est dans dim_comptes_scd2.
-- Ici on expose uniquement l'état courant.
-- ============================================================

{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dim'],
        cluster_by=['tenant_id']
    )
}}

with comptes as (
    select * from {{ ref('stg_comptes') }}
),

tenants as (
    select
        tenant_id,
        tenant_code,
        tenant_name,
        contract_tier
    from {{ ref('dim_tenants') }}
)

select
    c.compte_id,
    c.tenant_id,
    t.tenant_code,
    t.tenant_name,
    c.numero_compte,
    c.iban,
    c.bic,
    c.nom_client,
    c.prenom_client,
    c.email,
    c.type_compte,
    c.devise,
    c.solde_initial,
    c.solde_actuel,
    c.date_ouverture,
    c.date_derniere_activite,
    c.statut,
    c.kyc_level,
    c.aml_flag,
    c.customer_segment,
    c.is_pep,
    c.risk_score,
    c.pays,
    c.preferred_language,

    -- Champs calculés
    datediff('day', c.date_ouverture, current_date()) as anciennete_jours,
    datediff('day', c.date_derniere_activite, current_date()) as jours_depuis_activite,
    case
        when datediff('day', c.date_derniere_activite, current_date()) <= 30  then 'très_actif'
        when datediff('day', c.date_derniere_activite, current_date()) <= 90  then 'actif'
        when datediff('day', c.date_derniere_activite, current_date()) <= 365 then 'dormant'
        else 'inactif'
    end as segment_activite

from comptes c
left join tenants t on c.tenant_id = t.tenant_id
