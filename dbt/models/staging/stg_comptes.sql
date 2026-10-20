{{
    config(
        materialized='view',
        tags=['staging', 'core']
    )
}}

with source as (
    select * from {{ source('fintrack_raw', 'raw_comptes') }}
),

nettoye as (
    select
        compte_id,
        tenant_id,
        numero_compte,
        iban,
        bic,

        -- PII (à masquer via masking policies dans le projet Sécurité)
        {{ dbt_utils.generate_surrogate_key(['nom_client', 'prenom_client']) }} as client_hash,
        nom_client,
        prenom_client,
        email,
        telephone,
        date_naissance,

        -- Adresse
        adresse_ligne1,
        adresse_ligne2,
        code_postal,
        ville,
        upper(pays) as pays,

        -- Compte
        lower(type_compte) as type_compte,
        upper(devise) as devise,
        solde_initial,
        solde_actuel,
        date_ouverture,
        date_derniere_activite,
        lower(statut) as statut,

        -- KYC / AML
        kyc_level,
        kyc_date_verification,
        aml_flag,
        customer_segment,
        preferred_language,
        consent_marketing,
        consent_data_sharing,
        is_pep,
        risk_score,

        -- Metadata
        created_at,
        updated_at,
        created_by_source_system,
        _loaded_at

    from source
)

select * from nettoye
