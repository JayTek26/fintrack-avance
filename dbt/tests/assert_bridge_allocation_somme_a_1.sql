-- À chaque date de début de lien, la somme des allocation_factor
-- des liens actifs ce jour-là doit valoir 1
with points as (

    select distinct compte_id, date_debut as jour
    from {{ ref('bridge_comptes_titulaires') }}

),

actifs as (

    select
        p.compte_id,
        p.jour,
        sum(b.allocation_factor) as total
    from points p
    join {{ ref('bridge_comptes_titulaires') }} b
        on  b.compte_id  = p.compte_id
        and b.date_debut <= p.jour
        and (b.date_fin is null or b.date_fin > p.jour)
    group by p.compte_id, p.jour

)

select *
from actifs
where abs(total - 1) > 0.01