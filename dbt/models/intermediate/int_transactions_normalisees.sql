-- ============================================================
-- INT — Transactions normalisées (jointure enrichissement, pas d'agrégat)
-- ============================================================
-- Ce modèle est éphémère pour éviter la matérialisation d'une table
-- gigantesque intermédiaire. dbt l'injecte comme CTE dans les modèles aval.
--
-- Mesuré au Sprint 3 : le filtre incrémental de fct_transactions est bien
-- poussé jusqu'à stg_transactions, donc ephemeral reste le bon choix
-- (cf. docs/PERF_LOG.md).
-- ============================================================

{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'transactions']
    )
}}

with tx as (
    select * from {{ ref('stg_transactions') }}
),

comptes as (
    select
        compte_id,
        tenant_id as compte_tenant_id,
        type_compte,
        devise as devise_compte,
        customer_segment,
        pays as pays_compte,
        kyc_level,
        is_pep
    from {{ ref('stg_comptes') }}
),

categories as (
    select
        categorie_id,
        nom_categorie,
        type_categorie,
        groupe
    from {{ ref('stg_categories') }}
)

select
    tx.*,
    c.type_compte,
    c.devise_compte,
    c.customer_segment,
    c.pays_compte,
    c.kyc_level,
    c.is_pep,
    cat.nom_categorie,
    cat.type_categorie,
    cat.groupe,
    date_trunc('month', tx.date_transaction)::date as mois_transaction,
    date_trunc('week',  tx.date_transaction)::date as semaine_transaction,
    date_trunc('day',   tx.date_transaction)::date as jour_transaction

from tx
left join comptes c on tx.compte_id = c.compte_id
left join categories cat on tx.categorie_id = cat.categorie_id
