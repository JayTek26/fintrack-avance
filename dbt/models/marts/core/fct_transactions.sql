-- ============================================================
-- FCT TRANSACTIONS — table de faits principale
-- ============================================================
-- Volume cible : 50-100M lignes.
-- Stratégie : incremental merge sur transaction_id,
-- clustering (tenant_id, mois), colonnes essentielles seulement.
--
-- Grain : une ligne = une transaction validee ou en_attente, non-reversal.
-- Une transaction qui sort de ce périmètre après coup (en_attente → rejetee,
-- annulee…) est supprimée par le post-hook purge_transactions_hors_perimetre :
-- un MERGE seul la laisserait figée en en_attente.
--
-- montant_eur est recalculé au taux du jour de la transaction
-- (int_fx_rates_daily, cotation 1 EUR = taux × devise, donc division).
-- Le montant_eur fourni par le core banking n'est pas fiable (taux aléatoire).
--
-- Search Optimization sur external_transaction_id (lookups support /
-- rapprochement) posée en post-hook, en prod.
-- Microbatch évalué dans fct_transactions_microbatch (cf. docs/PERF_LOG.md).
-- ============================================================

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge',
        merge_update_columns=['statut', 'date_settlement',
                              'is_reconciled', 'reconciliation_batch_id',
                              'aml_flag', 'aml_score', 'fraud_score',
                              'is_flagged_for_review',
                              'montant_eur', 'montant_signe_eur',
                              'updated_at', '_loaded_at'],
        cluster_by=['tenant_id', "date_trunc('month', date_transaction)"],
        on_schema_change='append_new_columns',
        tags=['marts', 'core', 'fct', 'incremental', 'critical'],
        pre_hook=[
            "{{ log_incremental_run('fct_transactions', 'pre') }}"
        ],
        post_hook=[
            "{{ purge_transactions_hors_perimetre() }}",
            "{{ ensure_search_optimization(['external_transaction_id']) }}",
            "{{ log_incremental_run('fct_transactions', 'post') }}"
        ]
    )
}}

with base as (
    select * from {{ ref('int_transactions_normalisees') }}

    {% if is_incremental() %}
        where _loaded_at >= (
            select dateadd('day', -{{ var('incremental_lookback_days', 7) }}, max(_loaded_at))
            from {{ this }}
        )
    {% endif %}
),

fx as (
    select
        date_cotation,
        devise_cible as devise,
        taux
    from {{ ref('int_fx_rates_daily') }}
)

select
    -- Clés
    b.transaction_id,
    b.external_transaction_id,
    b.tenant_id,
    b.compte_id,
    b.categorie_id,

    -- Temporalité
    b.date_transaction,
    b.jour_transaction,
    b.semaine_transaction,
    b.mois_transaction,
    b.date_valeur,
    b.date_settlement,

    -- Montants
    b.montant,
    b.devise,
    -- Recalcul FX depuis notre référentiel (source de vérité) :
    -- taux = nb d'unités de devise pour 1 EUR → on divise.
    -- Pas de repli sur taux_change_applique : un taux manquant doit
    -- faire échouer le test not_null plutôt que fausser les montants.
    round(b.montant / fx.taux, 2)::number(18, 2) as montant_eur,
    b.montant_signe,
    (sign(b.montant_signe) * round(b.montant / fx.taux, 2))::number(18, 2) as montant_signe_eur,
    b.frais,
    b.montant_net,

    -- Type et statut
    b.type_operation,
    b.sens,
    b.statut,
    b.moyen_paiement,
    b.canal,

    -- Enrichissements
    b.type_compte,
    b.customer_segment,
    b.pays_compte,
    b.nom_categorie,
    b.type_categorie,
    b.groupe,

    -- Marchand
    b.marchand_nom,
    b.marchand_categorie,
    b.marchand_pays,
    b.marchand_mcc,

    -- Compliance
    b.aml_score,
    b.aml_flag,
    b.fraud_score,
    b.is_flagged_for_review,

    -- Rapprochement
    b.is_reconciled,
    b.reconciliation_batch_id,

    -- Metadata
    b.source_system,
    b.created_at,
    b.updated_at,
    b._loaded_at

from base b
left join fx
    on  fx.date_cotation = b.jour_transaction
    and fx.devise        = b.devise
where b.statut in ('validee', 'en_attente')
  and b.is_reversal = false