-- ============================================================
-- STAGING TRANSACTIONS — INCRÉMENTAL sur volume massif
-- ============================================================
-- Ce modèle est le point d'entrée du volume : ~50-100M lignes.
-- Stratégie : incremental + merge sur transaction_id.
--
-- Attention aux transactions qui changent de statut après insertion
-- (ex : en_attente → validee). On regarde la fenêtre des N derniers
-- jours plutôt que juste les nouvelles lignes.
-- ============================================================

{#- event_time : permet aux modèles microbatch aval de ne lire que la
    fenêtre du batch (sans effet sur le merge de ce modèle). -#}
{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge',
        merge_update_columns=['statut', 'statut_precedent', 'date_traitement',
                              'date_settlement', 'nb_tentatives',
                              'date_derniere_tentative', 'raison_rejet',
                              'code_erreur', 'message_erreur',
                              'is_reconciled', 'date_reconciliation',
                              'reconciliation_batch_id',
                              'aml_flag', 'aml_score', 'aml_reviewed_by',
                              'aml_review_date', 'fraud_score',
                              'fraud_rules_triggered', 'is_flagged_for_review',
                              'is_reversal', 'updated_at', 'loaded_at',
                              '_loaded_at'],
        cluster_by=['tenant_id', "date_trunc('month', date_transaction)"],
        on_schema_change='append_new_columns',
        event_time='date_transaction',
        tags=['staging', 'transactions', 'incremental']
    )
}}

with source as (
    select * from {{ source('fintrack_raw', 'raw_transactions') }}
    {% if is_incremental() %}
        -- Fenêtre glissante pour capturer les mises à jour de statut
        where _loaded_at >= (
            select dateadd('day', -{{ var('incremental_lookback_days', 7) }}, max(_loaded_at))
            from {{ this }}
        )
    {% elif target.name == 'ci' %}
        -- CI (build complet) : seul le dernier mois de données est construit
        -- (la CI vérifie la logique, pas la volumétrie) → CI < 10 min
        where date_transaction >= (
            select dateadd('day', -{{ var('ci_lookback_days', 30) }}, max(date_transaction))
            from {{ source('fintrack_raw', 'raw_transactions') }}
        )
    {% endif %}
),

deduped as (
    -- Une transaction mise à jour apparaît deux fois dans la fenêtre
    -- (ancienne et nouvelle version) : on garde la plus récente,
    -- sinon le MERGE Snowflake est non déterministe.
    select *
    from source
    qualify row_number() over (
        partition by transaction_id
        order by _loaded_at desc, updated_at desc
    ) = 1
),

typage as (
    select
        transaction_id,
        external_transaction_id,
        tenant_id,
        compte_id,
        compte_source_iban,
        compte_dest_iban,

        -- Timestamps
        date_transaction::timestamp_ntz  as date_transaction,
        date_valeur::timestamp_ntz       as date_valeur,
        date_comptabilisation::timestamp_ntz as date_comptabilisation,
        date_reception::timestamp_ntz    as date_reception,
        date_traitement::timestamp_ntz   as date_traitement,
        date_settlement::timestamp_ntz   as date_settlement,

        -- Montants
        montant,
        upper(devise) as devise,
        montant_eur,
        taux_change_applique,
        coalesce(frais, 0) as frais,
        upper(frais_devise) as frais_devise,
        montant_net,

        lower(type_operation) as type_operation,
        lower(sens) as sens,

        case
            when lower(type_operation) = 'credit' then montant
            when lower(type_operation) = 'debit'  then -montant
        end as montant_signe,

        case
            when lower(type_operation) = 'credit' then montant_eur
            when lower(type_operation) = 'debit'  then -montant_eur
        end as montant_signe_eur,

        -- Catégorisation
        categorie_id,
        categorie_auto_detectee,
        confiance_categorie,
        sous_categorie,
        tags,

        -- Marchand
        marchand_nom,
        marchand_id,
        marchand_categorie,
        upper(marchand_pays) as marchand_pays,
        marchand_mcc,
        marchand_siret,

        -- Moyen de paiement
        lower(moyen_paiement) as moyen_paiement,
        carte_id,
        carte_last4,
        carte_type,
        carte_reseau,
        authentification_3ds,

        -- Contexte
        lower(canal) as canal,
        device_type,
        device_id,
        user_agent,
        ip_address,
        session_id,
        geolocation_lat,
        geolocation_lon,

        -- Statut
        lower(statut) as statut,
        statut_precedent,
        nb_tentatives,
        date_derniere_tentative,
        raison_rejet,
        code_erreur,
        message_erreur,

        -- AML/Fraud
        aml_score,
        aml_flag,
        aml_reviewed_by,
        aml_review_date,
        fraud_score,
        fraud_rules_triggered,
        is_flagged_for_review,

        -- Rapprochement
        is_reconciled,
        date_reconciliation,
        reconciliation_batch_id,
        external_reference,
        internal_reference,

        -- Description
        description,
        description_brute,
        description_enrichie,
        libelle_court,

        -- Metadata
        source_system,
        source_file_batch_id,
        created_at,
        updated_at,
        loaded_at,
        processing_version,
        is_test,
        is_reversal,
        original_transaction_id,
        _loaded_at

    from deduped
    where is_test = false  -- on exclut les transactions de test dès staging
)

select * from typage