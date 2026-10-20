{{
    config(
        materialized='incremental',
        unique_key=['virement_id', 'virement_leg'],
        incremental_strategy='merge',
        cluster_by=['tenant_id', "date_trunc('month', date_virement)"],
        tags=['marts', 'core', 'fct', 'incremental']
    )
}}

with virements as (

    select *
    from {{ ref('stg_virements') }}
    where statut = 'execute'
    {% if is_incremental() %}
        and _loaded_at >= (
            select dateadd('day', -{{ var('incremental_lookback_days', 3) }}, max(_loaded_at))
            from {{ this }}
        )
    {% endif %}

),

fx as (

    select date_cotation, devise_cible as devise, taux
    from {{ ref('int_fx_rates_daily') }}

),

virements_eur as (

    -- Conversion au taux du jour du virement (1 EUR = taux × devise)
    select
        v.*,
        round(v.montant / fx.taux, 2) as montant_eur
    from virements v
    left join fx
        on  fx.date_cotation = v.date_virement::date
        and fx.devise        = v.devise

),

leg_sortante as (

    select
        virement_id,
        tenant_id,
        compte_source_id                as compte_id,
        compte_dest_id                  as contrepartie_compte_id,
        'sortant'                       as virement_leg,
        montant,
        -1 * abs(montant)               as montant_signe,
        devise,
        montant_eur,
        -1 * abs(montant_eur)           as montant_signe_eur,
        date_virement,
        statut,
        _loaded_at
    from virements_eur

),

leg_entrante as (

    select
        virement_id,
        tenant_id,
        compte_dest_id                  as compte_id,
        compte_source_id                as contrepartie_compte_id,
        'entrant'                       as virement_leg,
        montant,
        abs(montant)                    as montant_signe,
        devise,
        montant_eur,
        abs(montant_eur)                as montant_signe_eur,
        date_virement,
        statut,
        _loaded_at
    from virements_eur

)

select * from leg_sortante
union all
select * from leg_entrante