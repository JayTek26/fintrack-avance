{% macro drop_ci_schema(schema_name=none) %}
    {#-
        Supprime le schéma isolé d'une PR en fin de job CI.

            dbt run-operation drop_ci_schema --target ci
            dbt run-operation drop_ci_schema --args "{schema_name: CI_42}"

        Garde-fou : refuse tout schéma qui ne commence pas par CI_ et toute
        base autre que celle du target (jamais FINTRACK_PROD).
    -#}
    {% set schema_name = (schema_name or target.schema) | upper %}
    {% if not schema_name.startswith('CI_') %}
        {{ exceptions.raise_compiler_error("drop_ci_schema : schéma " ~ schema_name ~ " refusé (préfixe CI_ attendu)") }}
    {% endif %}
    {% if target.database | upper == 'FINTRACK_PROD' %}
        {{ exceptions.raise_compiler_error("drop_ci_schema : interdit sur FINTRACK_PROD") }}
    {% endif %}
    {% do run_query("drop schema if exists " ~ target.database ~ "." ~ schema_name ~ " cascade") %}
    {{ log("Schéma " ~ target.database ~ "." ~ schema_name ~ " supprimé", info=True) }}
{% endmacro %}
