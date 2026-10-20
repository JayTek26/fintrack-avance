{% macro purge_transactions_hors_perimetre() %}
    {#-
        Post-hook de fct_transactions.

        Le MERGE incrémental met à jour ou insère, mais ne supprime jamais.
        Une transaction déjà présente en `en_attente` qui passe ensuite
        `rejetee` / `annulee` / `en_cours_verification`, ou qui devient un
        reversal, n'est plus renvoyée par le modèle (filtre de grain) : sans ce
        hook, elle resterait figée en `en_attente` dans la table.

        On regarde la même fenêtre que le modèle (lookback sur _loaded_at).
    -#}
    delete from {{ this }} as tgt
    using (
        select transaction_id
        from {{ ref('stg_transactions') }}
        where _loaded_at >= (
            select dateadd('day', -{{ var('incremental_lookback_days', 7) }}, max(_loaded_at))
            from {{ ref('stg_transactions') }}
        )
          and (statut not in ('validee', 'en_attente') or is_reversal)
    ) as hors_perimetre
    where tgt.transaction_id = hors_perimetre.transaction_id
{% endmacro %}
