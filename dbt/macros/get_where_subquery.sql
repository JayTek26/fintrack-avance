{% macro get_where_subquery(relation) -%}
    {#-
        Surcharge du macro dbt utilisé par tous les tests génériques.

        Sur les grosses tables incrémentales, un test `unique` / `not_null` /
        `relationships` scanne toute la table à chaque run : sur le run
        incrémental mesuré (cf. docs/PERF_LOG.md), les tests représentaient
        ~66 % du temps warehouse. On les restreint à la fenêtre
        incrémentale avec un placeholder dans la config du test :

            config:
              where: "__fenetre_incrementale__"

        → remplacé par `_loaded_at >= max(_loaded_at) - N jours`
          (N = var('tests_lookback_days'), défaut = incremental_lookback_days).

        Le MERGE sur unique_key garantit déjà l'unicité hors fenêtre. Un run
        complet reste possible (job hebdomadaire, CI) avec :
            dbt test --vars '{tests_complets: true}'
    -#}
    {%- set where = config.get('where') -%}
    {%- if where -%}
        {%- if '__fenetre_incrementale__' in where -%}
            {%- if var('tests_complets', false) -%}
                {%- set filtre = '1 = 1' -%}
            {%- else -%}
                {%- set jours = var('tests_lookback_days', var('incremental_lookback_days', 7)) -%}
                {%- set filtre = "_loaded_at >= (select dateadd('day', -" ~ jours ~ ", max(_loaded_at)) from " ~ relation ~ ")" -%}
            {%- endif -%}
            {%- set where = where | replace('__fenetre_incrementale__', filtre) -%}
        {%- endif -%}
        {%- set filtered -%}
            (select * from {{ relation }} where {{ where }}) dbt_subquery
        {%- endset -%}
        {%- do return(filtered) -%}
    {%- else -%}
        {%- do return(relation) -%}
    {%- endif -%}
{%- endmacro %}
