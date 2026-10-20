{{
    config(
        materialized='view',
        tags=['staging', 'fx']
    )
}}

with source as (
    select * from {{ source('fintrack_raw', 'raw_fx_rates') }}
)

select
    fx_id,
    devise_source,
    devise_cible,
    date_cotation,
    taux,
    source_provider

from source
