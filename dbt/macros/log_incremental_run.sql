{% macro log_incremental_run(model_name, phase) %}
    {#-
        Journalise les runs incrémentaux dans FINTRACK_PROD.AUDIT.dbt_run_log.

        Usage dans un modèle :
            pre_hook = "{{ log_incremental_run('fct_transactions', 'pre') }}"
            post_hook = "{{ log_incremental_run('fct_transactions', 'post') }}"

        La table doit être créée au préalable — voir scripts/snowflake/05_audit_tables.sql
    -#}

    {% if execute %}
        {% set query %}
            insert into {{ target.database }}.audit.dbt_run_log (
                run_id, model_name, phase, invocation_id,
                target_name, run_started_at, logged_at
            )
            values (
                '{{ invocation_id }}',
                '{{ model_name }}',
                '{{ phase }}',
                '{{ invocation_id }}',
                '{{ target.name }}',
                '{{ run_started_at }}',
                current_timestamp()
            )
        {% endset %}
        {{ log("Logging " ~ phase ~ " hook for " ~ model_name, info=True) }}
        -- Décommenter une fois la table audit créée :
        -- {{ return(query) }}
        {{ return("select 1 as noop") }}
    {% endif %}
{% endmacro %}
