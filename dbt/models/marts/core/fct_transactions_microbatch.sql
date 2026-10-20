-- ============================================================
-- FCT TRANSACTIONS — prototype MICROBATCH (US 3.2, expérimental)
-- ============================================================
-- Même logique que fct_transactions, mais en incremental_strategy
-- 'microbatch' : dbt découpe le traitement en fenêtres d'un jour sur
-- event_time (date_transaction) et remplace chaque fenêtre en entier.
-- Les refs amont qui déclarent un event_time (stg_transactions) sont
-- filtrées automatiquement sur la fenêtre du batch. On lit donc
-- stg_transactions directement : le filtre ne traverse pas le modèle
-- éphémère int_transactions_normalisees (pas d'event_time possible).
--
-- Désactivé par défaut : un premier build traiterait un batch par jour
-- depuis `begin` jusqu'à aujourd'hui. Pour le benchmark :
--   dbt run -s fct_transactions_microbatch --vars '{enable_microbatch: true}' \
--     --event-time-start 2024-12-01 --event-time-end 2024-12-31
--
-- Limite structurante (cf. docs/PERF_LOG.md) : seule la fenêtre
-- [aujourd'hui - lookback, aujourd'hui] est retraitée. Un changement de
-- statut sur une transaction plus ancienne que `lookback` jours n'est
-- jamais capté, alors que le merge sur _loaded_at le capte.
-- ============================================================

{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        event_time='date_transaction',
        batch_size='day',
        lookback=3,
        begin='2023-01-01',
        concurrent_batches=true,
        cluster_by=['tenant_id', "date_trunc('month', date_transaction)"],
        enabled=var('enable_microbatch', false),
        tags=['marts', 'core', 'fct', 'incremental', 'experimental']
    )
}}

with tx as (
    select * from {{ ref('stg_transactions') }}
),

comptes as (
    select compte_id, type_compte, customer_segment, pays as pays_compte
    from {{ ref('stg_comptes') }}
),

categories as (
    select categorie_id, nom_categorie, type_categorie, groupe
    from {{ ref('stg_categories') }}
),

base as (
    select
        tx.*,
        c.type_compte,
        c.customer_segment,
        c.pays_compte,
        cat.nom_categorie,
        cat.type_categorie,
        cat.groupe,
        date_trunc('month', tx.date_transaction)::date as mois_transaction,
        date_trunc('week',  tx.date_transaction)::date as semaine_transaction,
        date_trunc('day',   tx.date_transaction)::date as jour_transaction
    from tx
    left join comptes c on tx.compte_id = c.compte_id
    left join categories cat on tx.categorie_id = cat.categorie_id
),

fx as (
    select
        date_cotation,
        devise_cible as devise,
        taux
    from {{ ref('int_fx_rates_daily') }}
)

select
    b.transaction_id,
    b.external_transaction_id,
    b.tenant_id,
    b.compte_id,
    b.categorie_id,
    b.date_transaction,
    b.jour_transaction,
    b.semaine_transaction,
    b.mois_transaction,
    b.date_valeur,
    b.date_settlement,
    b.montant,
    b.devise,
    round(b.montant / fx.taux, 2)::number(18, 2) as montant_eur,
    b.montant_signe,
    (sign(b.montant_signe) * round(b.montant / fx.taux, 2))::number(18, 2) as montant_signe_eur,
    b.frais,
    b.montant_net,
    b.type_operation,
    b.sens,
    b.statut,
    b.moyen_paiement,
    b.canal,
    b.type_compte,
    b.customer_segment,
    b.pays_compte,
    b.nom_categorie,
    b.type_categorie,
    b.groupe,
    b.marchand_nom,
    b.marchand_categorie,
    b.marchand_pays,
    b.marchand_mcc,
    b.aml_score,
    b.aml_flag,
    b.fraud_score,
    b.is_flagged_for_review,
    b.is_reconciled,
    b.reconciliation_batch_id,
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
