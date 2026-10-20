-- ============================================================
-- MART — Détection d'anomalies sur transactions (Z-score par compte)
-- ============================================================
-- Une transaction est "anormale" si son montant s'écarte de plus de
-- 3 écarts-types de la moyenne mobile sur 90 jours du compte.
--
-- Choix :
--   - fenêtre temporelle (RANGE 90 jours) et non ROWS N PRECEDING :
--     un compte peu actif aurait sinon une "fenêtre de 90 lignes" couvrant
--     plusieurs années ;
--   - la transaction évaluée est exclue de sa propre référence (on retire
--     x de la somme, de la somme des carrés et du compte), sinon un gros
--     montant gonfle l'écart-type et se masque lui-même ;
--   - minimum 10 transactions d'historique pour calculer un z-score fiable ;
--   - sévérité : low (|z| > 3), medium (|z| > 4), high (|z| > 5).
-- ============================================================

{{
    config(
        materialized='table',
        cluster_by=['tenant_id', 'jour_transaction'],
        tags=['marts', 'analytics', 'anomalies']
    )
}}

{% set seuil_z = 3 %}
{% set historique_min = 10 %}

with tx as (
    select
        transaction_id,
        tenant_id,
        compte_id,
        date_transaction,
        jour_transaction,
        montant_eur
    from {{ ref('fct_transactions') }}
    where statut = 'validee'
      and montant_eur is not null
),

fenetre as (
    {%- set fenetre_90j -%}
        over (
            partition by compte_id
            order by date_transaction
            range between interval '90 days' preceding and current row
        )
    {%- endset %}
    select
        tx.*,
        count(*)                       {{ fenetre_90j }} as n_avec_courante,
        sum(montant_eur)               {{ fenetre_90j }} as somme_avec_courante,
        sum(montant_eur * montant_eur) {{ fenetre_90j }} as somme_carres_avec_courante
    from tx
),

stats as (
    select
        transaction_id,
        tenant_id,
        compte_id,
        date_transaction,
        jour_transaction,
        montant_eur,
        n_avec_courante - 1 as nb_tx_historique,
        (somme_avec_courante - montant_eur) / nullif(n_avec_courante - 1, 0) as moyenne_90j,
        -- variance d'échantillon de l'historique (hors transaction courante)
        greatest(
            (somme_carres_avec_courante - montant_eur * montant_eur
             - (n_avec_courante - 1)
               * square((somme_avec_courante - montant_eur) / nullif(n_avec_courante - 1, 0)))
            / nullif(n_avec_courante - 2, 0),
            0
        ) as variance_90j
    from fenetre
),

scores as (
    select
        *,
        sqrt(variance_90j) as ecart_type_90j,
        case
            when nb_tx_historique >= {{ historique_min }} and variance_90j > 0
            then (montant_eur - moyenne_90j) / sqrt(variance_90j)
        end as z_score
    from stats
)

select
    transaction_id,
    tenant_id,
    compte_id,
    date_transaction,
    jour_transaction,
    montant_eur,
    nb_tx_historique,
    round(moyenne_90j, 2)    as moyenne_90j,
    round(ecart_type_90j, 2) as ecart_type_90j,
    round(z_score, 3)        as z_score,
    coalesce(abs(z_score) > {{ seuil_z }}, false) as is_anomalie,
    case
        when abs(z_score) > 5 then 'high'
        when abs(z_score) > 4 then 'medium'
        when abs(z_score) > {{ seuil_z }} then 'low'
    end as severite

from scores
