{% macro ensure_search_optimization(equality_columns) %}
    {#-
        Post-hook : (ré)active la Search Optimization sur les colonnes de
        lookup ponctuel. Nécessaire car un --full-refresh recrée la table et
        perd la SO. L'ALTER est idempotent si la SO existe déjà.

        Activé par défaut en prod uniquement (coût de maintenance à chaque
        merge) ; forçable avec --vars '{enable_search_optimization: true}'.
        Mesures et arbitrage : docs/PERF_LOG.md (Query 1).
    -#}
    {% if var('enable_search_optimization', target.name == 'prod') %}
        alter table {{ this }} add search optimization
            on equality({{ equality_columns | join(', ') }})
    {% else %}
        select 'search optimization désactivée sur ce target' as noop
    {% endif %}
{% endmacro %}
