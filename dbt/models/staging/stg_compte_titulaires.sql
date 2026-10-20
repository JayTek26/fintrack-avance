{{
    config(
        materialized='view',
        tags=['staging', 'core']
    )
}}

with source as (
    select * from {{ source('fintrack_raw', 'raw_compte_titulaires') }}
)

select
    compte_id,
    titulaire_id,
    role_titulaire,
    part_pct,
    date_debut,
    date_fin,
    _loaded_at

from source