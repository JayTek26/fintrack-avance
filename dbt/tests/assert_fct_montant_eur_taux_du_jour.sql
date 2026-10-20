-- US 4.2 : montant_eur de fct_transactions doit être calculé au taux du jour
-- de la transaction.
--
-- Recalcul indépendant de int_fx_rates_daily : sur un échantillon fixe de
-- 100 transactions en devise étrangère, on retrouve la dernière cotation
-- disponible à la date de la transaction directement dans stg_fx_rates
-- (ASOF JOIN, donc sans réutiliser le forward-fill du modèle), et on exige
-- un écart relatif < 0,01 %.
{{ config(tags=['fx']) }}

with echantillon as (

    select transaction_id, jour_transaction, devise, montant, montant_eur
    from {{ ref('fct_transactions') }}
    where devise <> 'EUR'
    -- échantillon déterministe (hash) plutôt que SAMPLE, pour un test rejouable
    order by hash(transaction_id)
    limit 100

),

cotations as (

    select devise_cible, date_cotation, taux
    from {{ ref('stg_fx_rates') }}

),

recalcul as (

    select
        e.transaction_id,
        e.devise,
        e.jour_transaction,
        e.montant,
        e.montant_eur,
        c.date_cotation as date_cotation_utilisee,
        round(e.montant / c.taux, 2) as montant_eur_attendu
    from echantillon e
    asof join cotations c
        match_condition (e.jour_transaction >= c.date_cotation)
        on e.devise = c.devise_cible

)

select
    *,
    abs(montant_eur - montant_eur_attendu) / nullif(abs(montant_eur_attendu), 0) as ecart_relatif
from recalcul
where montant_eur_attendu is null
   or abs(montant_eur - montant_eur_attendu) / nullif(abs(montant_eur_attendu), 0) > 0.0001
