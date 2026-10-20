-- Chaque virement doit avoir exactement 2 écritures dont la somme est nulle
select
    virement_id,
    count(*)            as nb_ecritures,
    sum(montant_signe)  as somme
from {{ ref('fct_virements') }}
group by virement_id
having count(*) <> 2 or abs(sum(montant_signe)) > 0.0001