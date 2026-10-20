-- SCD2 : chaque compte présent dans la source a exactement une version
-- courante, et aucun compte n'en a plus d'une (un compte supprimé de la
-- source peut n'en avoir aucune : hard_deletes = 'invalidate').
with versions as (

    select
        compte_id,
        count_if(is_current) as nb_versions_courantes
    from {{ ref('dim_comptes_scd2') }}
    group by compte_id

)

select
    coalesce(v.compte_id, s.compte_id) as compte_id,
    coalesce(v.nb_versions_courantes, 0) as nb_versions_courantes
from versions v
full outer join {{ ref('stg_comptes') }} s
    on v.compte_id = s.compte_id
where coalesce(v.nb_versions_courantes, 0) > 1
   or (s.compte_id is not null and coalesce(v.nb_versions_courantes, 0) <> 1)
