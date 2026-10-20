-- ============================================================
-- MART — Rapprochement bancaire (état de la réconciliation par batch)
-- ============================================================
-- Suit les taux de rapprochement des transactions par lot d'ingestion.
-- Critique pour la finance : toute transaction non-rapprochée > 3 jours
-- est un signal d'alerte.
-- ============================================================

{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'compliance']
    )
}}

with tx as (
    select
        tenant_id,
        reconciliation_batch_id,
        source_system,
        jour_transaction,
        count(*) as nb_transactions,
        sum(case when is_reconciled then 1 else 0 end) as nb_rapprochees,
        sum(case when not is_reconciled then 1 else 0 end) as nb_non_rapprochees,
        sum(montant_eur) as montant_total_eur,
        sum(case when is_reconciled then montant_eur else 0 end) as montant_rapproche_eur,
        min(date_transaction) as first_tx,
        max(date_transaction) as last_tx
    from {{ ref('fct_transactions') }}
    where reconciliation_batch_id is not null
    group by 1, 2, 3, 4
)

select
    tenant_id,
    reconciliation_batch_id,
    source_system,
    jour_transaction,
    nb_transactions,
    nb_rapprochees,
    nb_non_rapprochees,
    round(100.0 * nb_rapprochees / nullif(nb_transactions, 0), 2) as taux_rapprochement_pct,
    montant_total_eur,
    montant_rapproche_eur,
    montant_total_eur - montant_rapproche_eur as ecart_montant_eur,
    first_tx,
    last_tx,
    datediff('day', last_tx, current_timestamp()) as jours_depuis_dernier_tx,
    case
        when datediff('day', last_tx, current_timestamp()) > 3
         and nb_non_rapprochees > 0
        then true
        else false
    end as alerte_reconciliation

from tx
