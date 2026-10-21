{% macro set_query_tag() -%}
    {#-
        Positionne un query_tag Snowflake structuré pour tracer les coûts
        par équipe, projet, modèle et environnement.

        Appelé automatiquement :
          - via on-run-start dans dbt_project.yml (tag sans modèle) ;
          - par les materializations dbt-snowflake avant chaque modèle
            (ce macro surcharge celui de l'adapter), ce qui ajoute
            `model=<nom>` et permet d'agréger QUERY_HISTORY par modèle.

        Retourne le tag précédent, comme le macro de l'adapter, pour que
        unset_query_tag puisse le restaurer.
    -#}
    {#- execute : au parsing, dbt Core n'exécute pas de requête (run_query
        renvoie None) ; dbt Fusion le tolère, pas dbt Core (CI). -#}
    {% if target.type == 'snowflake' and execute %}
        {% set tag = 'team=data-eng|project=fintrack-perf-scale|target=' ~ target.name %}
        {% if model is defined and model is mapping and model.get('resource_type') in ('model', 'snapshot', 'seed') %}
            {% set tag = tag ~ '|model=' ~ model['name'] %}
        {% endif %}
        {% set original_query_tag = get_current_query_tag() %}
        {% do run_query("alter session set query_tag = '" ~ tag ~ "'") %}
        {% do log("Query tag set: " ~ tag, info=False) %}
        {{ return(original_query_tag) }}
    {% endif %}
{%- endmacro %}


{% macro unset_query_tag(original_query_tag=none) -%}
    {% if target.type == 'snowflake' and execute %}
        {% if original_query_tag %}
            {% do run_query("alter session set query_tag = '" ~ original_query_tag ~ "'") %}
        {% else %}
            {% do run_query("alter session unset query_tag") %}
        {% endif %}
    {% endif %}
{%- endmacro %}
