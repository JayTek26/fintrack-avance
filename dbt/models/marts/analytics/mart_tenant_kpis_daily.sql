-- ============================================================
-- MART — KPIs quotidiens par tenant (banque partenaire)
-- ============================================================
-- Vue macro : chaque tenant voit ses volumes, revenus, alertes.
-- Consommé par le dashboard exécutif.
-- ============================================================

{{
    config(
        materialized='table',
        cluster_by=['jour', 'tenant_id'],
        tags=['marts', 'analytics', 'kpi']
    )
}}

with tx as (
    select * from {{ ref('fct_transactions') }}
),

agg as (
    select
        tenant_id,
        jour_transaction as jour,
        count(*) as nb_transactions,
        count(distinct compte_id) as nb_comptes_actifs,
        count(distinct case when statut = 'validee' then transaction_id end) as nb_transactions_validees,
        count(distinct case when statut = 'rejetee' then transaction_id end) as nb_transactions_rejetees,
        sum(case when type_operation = 'credit' and statut = 'validee' then montant_eur else 0 end) as volume_credit_eur,
        sum(case when type_operation = 'debit'  and statut = 'validee' then montant_eur else 0 end) as volume_debit_eur,
        sum(case when statut = 'validee' then frais else 0 end) as revenus_frais_eur,

        -- Compliance
        sum(case when is_flagged_for_review then 1 else 0 end) as nb_transactions_flagged,
        sum(case when aml_flag = 'suspicious' then 1 else 0 end) as nb_aml_suspicious,
        avg(fraud_score) as fraud_score_moyen,
        max(fraud_score) as fraud_score_max
    from tx
    group by tenant_id, jour_transaction
),

tenants as (
    select tenant_id, tenant_code, tenant_name, contract_tier
    from {{ ref('dim_tenants') }}
)

select
    a.jour,
    a.tenant_id,
    t.tenant_code,
    t.tenant_name,
    t.contract_tier,
    a.nb_transactions,
    a.nb_comptes_actifs,
    a.nb_transactions_validees,
    a.nb_transactions_rejetees,
    round(100.0 * a.nb_transactions_validees / nullif(a.nb_transactions, 0), 2) as taux_validation_pct,
    a.volume_credit_eur,
    a.volume_debit_eur,
    a.volume_credit_eur - a.volume_debit_eur as volume_net_eur,
    a.revenus_frais_eur,
    a.nb_transactions_flagged,
    a.nb_aml_suspicious,
    a.fraud_score_moyen,
    a.fraud_score_max

from agg a
left join tenants t on a.tenant_id = t.tenant_id
