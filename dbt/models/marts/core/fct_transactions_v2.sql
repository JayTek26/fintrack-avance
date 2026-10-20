-- ============================================================
-- FCT TRANSACTIONS v2 — pré-release (US 6.2)
-- ============================================================
-- Ajoute montant_hors_taxes_eur sans dupliquer le stockage : vue au-dessus
-- de la v1. Les consommateurs migrent avec ref('fct_transactions', v=2).
-- Taux de TVA standard par pays du marchand (hypothèse documentée dans
-- _core.yml) ; pays hors liste → 20 %.
-- ============================================================

{{
    config(
        materialized='view',
        tags=['marts', 'core', 'fct']
    )
}}

with v1 as (
    select * from {{ ref('fct_transactions', v=1) }}
),

tva as (
    select
        *,
        case
            when marchand_nom is null then null
            when marchand_pays = 'FR' then 0.2000
            when marchand_pays = 'DE' then 0.1900
            when marchand_pays = 'ES' then 0.2100
            when marchand_pays = 'IT' then 0.2200
            when marchand_pays = 'NL' then 0.2100
            when marchand_pays = 'BE' then 0.2100
            when marchand_pays = 'GB' then 0.2000
            when marchand_pays = 'CH' then 0.0810
            when marchand_pays = 'LU' then 0.1700
            else 0.2000
        end::number(5, 4) as taux_tva_applique
    from v1
)

select
    *,
    round(montant_eur / (1 + taux_tva_applique), 2)::number(18, 2) as montant_hors_taxes_eur
from tva
