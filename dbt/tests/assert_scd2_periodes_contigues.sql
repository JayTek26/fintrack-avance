-- SCD2 : les versions d'un compte s'enchaînent sans trou ni chevauchement
-- (la fin d'une version = le début de la suivante).
select *
from (
    select
        compte_id,
        version_number,
        date_fin_validite,
        lead(date_debut_validite) over (
            partition by compte_id order by version_number
        ) as debut_version_suivante
    from {{ ref('dim_comptes_scd2') }}
)
where debut_version_suivante is not null
  and date_fin_validite <> debut_version_suivante
