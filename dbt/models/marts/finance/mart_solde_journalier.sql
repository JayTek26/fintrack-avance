-- ============================================================
-- MART — Solde journalier par compte (avec forward-fill)
-- ============================================================
-- Agrège transactions + virements pour produire le solde de chaque
-- compte à chaque date, y compris les jours sans mouvement.
--
-- Montants en EUR au taux du jour (transactions et virements).
-- Reste en table : 9M lignes reconstruites en ~8 s sur XSMALL à l'échelle L.
-- À 100M+ transactions, c'est le premier candidat à l'incrémental
-- (recalcul des comptes touchés depuis le dernier run), cf. docs/DESIGN.md.
-- ============================================================

{{
    config(
        materialized='table',
        cluster_by=['tenant_id', 'jour'],
        tags=['marts', 'finance', 'solde']
    )
}}

with tx as (
    select
        tenant_id,
        compte_id,
        jour_transaction as jour,
        sum(montant_signe_eur) as solde_tx_eur
    from {{ ref('fct_transactions') }}
    where statut = 'validee'
    group by tenant_id, compte_id, jour_transaction
),

vir as (
    select
        tenant_id,
        compte_id,
        date_virement::date as jour,
        sum(montant_signe_eur) as solde_vir_eur
    from {{ ref('fct_virements') }}
    group by tenant_id, compte_id, date_virement::date
),

flux_jour as (
    select
        coalesce(tx.tenant_id, vir.tenant_id) as tenant_id,
        coalesce(tx.compte_id, vir.compte_id) as compte_id,
        coalesce(tx.jour, vir.jour) as jour,
        coalesce(tx.solde_tx_eur, 0)  as flux_transactions,
        coalesce(vir.solde_vir_eur, 0) as flux_virements
    from tx
    full outer join vir
        on tx.compte_id = vir.compte_id
       and tx.jour = vir.jour
),

comptes as (
    select
        compte_id,
        tenant_id,
        solde_initial,
        date_ouverture
    from {{ ref('dim_comptes') }}
)

select
    f.tenant_id,
    f.compte_id,
    f.jour,
    f.flux_transactions,
    f.flux_virements,
    f.flux_transactions + f.flux_virements as flux_total_jour,

    c.solde_initial + sum(f.flux_transactions + f.flux_virements) over (
        partition by f.compte_id
        order by f.jour
        rows between unbounded preceding and current row
    ) as solde_cumule_eur

from flux_jour f
join comptes c on f.compte_id = c.compte_id
