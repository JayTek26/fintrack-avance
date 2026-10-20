-- ============================================================
-- INT — Taux FX quotidiens densifiés (forward-fill des week-ends)
-- ============================================================
-- Les marchés FX ne cotent pas le week-end : on utilise le dernier taux
-- disponible (last observation carried forward).
-- La grille va jusqu'à aujourd'hui : au-delà de la dernière cotation reçue,
-- le dernier taux connu est reporté. date_cotation_source permet de repérer
-- (et de tester) les taux reportés depuis trop longtemps.
-- ============================================================

{{
    config(
        materialized='table',
        tags=['intermediate', 'fx']
    )
}}

with source as (
    select * from {{ ref('stg_fx_rates') }}
),

date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="dateadd('day', 1, current_date())"
    ) }}
),

devises as (
    select distinct devise_cible from source
),

grille_complete as (
    select
        ds.date_day::date as date_cotation,
        d.devise_cible
    from date_spine ds
    cross join devises d
),

joint as (
    select
        gc.date_cotation,
        gc.devise_cible,
        s.taux,
        -- forward fill : dernier taux non-null par devise
        last_value(s.taux ignore nulls) over (
            partition by gc.devise_cible
            order by gc.date_cotation
            rows between unbounded preceding and current row
        ) as taux_ffill,
        last_value(s.date_cotation ignore nulls) over (
            partition by gc.devise_cible
            order by gc.date_cotation
            rows between unbounded preceding and current row
        ) as date_cotation_source
    from grille_complete gc
    left join source s
        on gc.date_cotation = s.date_cotation
       and gc.devise_cible = s.devise_cible
)

select
    date_cotation,
    'EUR' as devise_source,
    devise_cible,
    taux_ffill as taux,
    date_cotation_source
from joint
where taux_ffill is not null

union all

-- Taux 1:1 pour EUR → EUR
select
    date_day::date as date_cotation,
    'EUR' as devise_source,
    'EUR' as devise_cible,
    1.0 as taux,
    date_day::date as date_cotation_source
from date_spine
