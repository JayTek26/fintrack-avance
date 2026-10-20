{{
    config(
        materialized='view',
        tags=['staging', 'core']
    )
}}

with source as (
    select * from {{ source('fintrack_raw', 'raw_titulaires') }}
)

select
    titulaire_id,
    nom,
    prenom,
    date_naissance,
    pays_residence,
    _loaded_at

from source