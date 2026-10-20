{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dim']
    )
}}

with recursive categories as (

    select
        categorie_id,
        nom_categorie,
        type_categorie,
        groupe,
        categorie_parent_id
    from {{ ref('stg_categories') }}

),

hierarchie as (

    -- Ancrage : les racines (sans parent, ou dont le parent n'existe pas
    -- dans stg_categories car il est inactif)
    select
        c.categorie_id,
        c.nom_categorie,
        c.type_categorie,
        c.groupe,
        c.categorie_parent_id,
        1 as niveau,
        c.categorie_id as id_racine,
        cast(c.nom_categorie as varchar(4000)) as chemin_complet
    from categories c
    left join categories p
        on c.categorie_parent_id = p.categorie_id
    where p.categorie_id is null

    union all

    -- Récursion : on descend d'un niveau à chaque itération
    select
        c.categorie_id,
        c.nom_categorie,
        c.type_categorie,
        c.groupe,
        c.categorie_parent_id,
        h.niveau + 1 as niveau,
        h.id_racine,
        h.chemin_complet || ' > ' || c.nom_categorie as chemin_complet
    from categories c
    inner join hierarchie h
        on c.categorie_parent_id = h.categorie_id
    where h.niveau < 10  -- garde-fou contre les cycles

)

select
    categorie_id,
    nom_categorie,
    type_categorie,
    groupe,
    categorie_parent_id,
    niveau,
    id_racine,
    chemin_complet
from hierarchie