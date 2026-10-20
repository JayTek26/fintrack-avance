{{
    config(
        materialized='view',
        tags=['staging', 'core']
    )
}}

with source as (
    select * from {{ source('fintrack_raw', 'raw_categories') }}
)

select
    categorie_id,
    nom_categorie,
    type_categorie,
    groupe,
    categorie_parent_id,
    niveau_hierarchique,
    is_active,
    date_creation

from source
where is_active = true
