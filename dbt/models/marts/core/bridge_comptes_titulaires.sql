-- ============================================================
-- BRIDGE COMPTES ↔ TITULAIRES (relation many-to-many)
-- ============================================================
-- Un compte peut avoir plusieurs titulaires, un titulaire plusieurs comptes.
-- Sources : raw_titulaires / raw_compte_titulaires (générateur enrichi).
--   - allocation_factor (part / 100) : ventilation des montants par titulaire
--   - date_debut (incluse), date_fin (exclue) : historisation des liens
-- Test : assert_bridge_allocation_somme_a_1 (somme des facteurs actifs = 1).
-- ============================================================

{{
    config(
        materialized='table',
        tags=['marts', 'core', 'bridge']
    )
}}

with liens as (

    select * from {{ ref('stg_compte_titulaires') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['compte_id', 'titulaire_id', 'date_debut']) }} as bridge_key,
    compte_id,
    titulaire_id,
    role_titulaire,
    round(part_pct / 100.0, 4)      as allocation_factor,
    date_debut,
    date_fin,
    (role_titulaire = 'PRINCIPAL')  as is_primary,
    (date_fin is null)              as is_current

from liens