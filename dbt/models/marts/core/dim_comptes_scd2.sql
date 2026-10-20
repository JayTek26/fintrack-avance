-- ============================================================
-- DIM COMPTES SCD Type 2 — historisation des changements
-- ============================================================
-- Source : snapshot_comptes (strategy check sur statut, KYC, AML, email,
-- type_compte, segment, PEP). Une ligne = une version d'un compte.
--
--   - compte_version_key : clé de substitution (compte_id, début de validité)
--   - date_fin_validite  : NULL pour la version courante
--   - is_current         : exactement une version courante par compte encore
--                          présent dans la source (test
--                          assert_scd2_une_version_courante)
-- ============================================================

{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dim', 'scd2'],
        cluster_by=['compte_id']
    )
}}

with comptes_historises as (

    select
        compte_id,
        tenant_id,
        numero_compte,
        iban,
        statut,
        kyc_level,
        kyc_date_verification,
        aml_flag,
        email,
        type_compte,
        customer_segment,
        is_pep,
        risk_score,

        dbt_valid_from as date_debut_validite,
        dbt_valid_to   as date_fin_validite,

        row_number() over (
            partition by compte_id
            order by dbt_valid_from
        ) as version_number

    from {{ ref('snapshot_comptes') }}

)

select
    {{ dbt_utils.generate_surrogate_key([
        'compte_id',
        'date_debut_validite'
    ]) }} as compte_version_key,

    compte_id,
    tenant_id,
    numero_compte,
    iban,
    statut,
    kyc_level,
    kyc_date_verification,
    aml_flag,
    email,
    type_compte,
    customer_segment,
    is_pep,
    risk_score,

    date_debut_validite,
    date_fin_validite,

    (date_fin_validite is null) as is_current,
    version_number

from comptes_historises
