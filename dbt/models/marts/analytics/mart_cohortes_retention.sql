-- ============================================================
-- MART — Analyse de cohortes de rétention
-- ============================================================
-- Cohorte = mois d'ouverture du compte (par tenant).
-- Un compte est "actif" un mois donné s'il a au moins 1 transaction
-- validée dans ce mois. On mesure M+0 → M+12.
--
-- Choix :
--   - grille cohorte × offset (0..12) pour faire apparaître les mois à 0 actif ;
--   - la grille s'arrête au dernier mois observé dans les faits : un mois
--     pas encore écoulé n'est pas une rétention de 0 % ;
--   - l'activité antérieure à l'ouverture (données incohérentes du core
--     banking) est ignorée.
-- ============================================================

{{
    config(
        materialized='table',
        cluster_by=['tenant_id', 'cohort_mois'],
        tags=['marts', 'analytics', 'retention']
    )
}}

with comptes as (
    select
        compte_id,
        tenant_id,
        date_trunc('month', date_ouverture)::date as cohort_mois
    from {{ ref('dim_comptes') }}
),

activite as (
    select distinct
        compte_id,
        mois_transaction
    from {{ ref('fct_transactions') }}
    where statut = 'validee'
),

bornes as (
    select max(mois_transaction) as dernier_mois_observe
    from activite
),

offsets as (
    select generated_number - 1 as mois_offset
    from ({{ dbt_utils.generate_series(13) }})
),

taille_cohortes as (
    select
        tenant_id,
        cohort_mois,
        count(*) as nb_utilisateurs_cohorte
    from comptes
    group by tenant_id, cohort_mois
),

grille as (
    select
        t.tenant_id,
        t.cohort_mois,
        o.mois_offset,
        dateadd('month', o.mois_offset, t.cohort_mois)::date as mois_activite,
        t.nb_utilisateurs_cohorte
    from taille_cohortes t
    cross join offsets o
    cross join bornes b
    where dateadd('month', o.mois_offset, t.cohort_mois) <= b.dernier_mois_observe
),

actifs as (
    select
        c.tenant_id,
        c.cohort_mois,
        datediff('month', c.cohort_mois, a.mois_transaction) as mois_offset,
        count(distinct c.compte_id) as nb_utilisateurs_actifs
    from comptes c
    inner join activite a
        on  a.compte_id = c.compte_id
        and a.mois_transaction >= c.cohort_mois
        and a.mois_transaction <  dateadd('month', 13, c.cohort_mois)
    group by 1, 2, 3
)

select
    g.tenant_id,
    g.cohort_mois,
    g.mois_offset,
    g.mois_activite,
    g.nb_utilisateurs_cohorte,
    coalesce(a.nb_utilisateurs_actifs, 0) as nb_utilisateurs_actifs,
    round(100.0 * coalesce(a.nb_utilisateurs_actifs, 0)
          / nullif(g.nb_utilisateurs_cohorte, 0), 2) as taux_retention_pct

from grille g
left join actifs a
    on  a.tenant_id   = g.tenant_id
    and a.cohort_mois = g.cohort_mois
    and a.mois_offset = g.mois_offset
