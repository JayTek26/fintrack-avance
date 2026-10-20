{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
        Convention multi-environnement :
          - target.name = 'prod'  → schéma custom tel quel (STAGING, MARTS_CORE, etc.)
          - target.name = 'ci'    → tout dans CI_<PR_ID> (target.schema du profil ci) :
                                    un seul schéma isolé par PR, supprimé en fin
                                    de job (macro drop_ci_schema)
          - target.name = 'dev'   → DEV_<user>_<custom>  (isolé par développeur)
    -#}

    {%- set default_schema = target.schema -%}

    {%- if target.name == 'prod' -%}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}

    {%- elif target.name == 'ci' -%}
        {{ default_schema }}

    {%- else -%}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ default_schema }}_{{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}

{%- endmacro %}
