{% test row_count_delta(model, compare_model, max_delta_pct=10) %}
    {#-
        Test générique : vérifie que le row_count d'un modèle ne s'écarte pas
        de plus de max_delta_pct % de celui d'un autre modèle.

        Utile pour détecter des ruptures d'ingestion : si fct_transactions
        a soudainement 30% de lignes en moins que stg_transactions, il y a
        un problème.
    -#}

    with base as (
        select count(*) as n from {{ model }}
    ),
    compare as (
        select count(*) as n from {{ compare_model }}
    ),
    delta as (
        select
            base.n as n_base,
            compare.n as n_compare,
            abs(base.n - compare.n) * 100.0 / nullif(compare.n, 0) as delta_pct
        from base, compare
    )
    select *
    from delta
    where delta_pct > {{ max_delta_pct }}

{% endtest %}
