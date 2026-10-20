{% snapshot snapshot_comptes %}

{{
    config(
        schema='snapshots',
        unique_key='compte_id',
        strategy='check',
        check_cols=['statut', 'kyc_level', 'aml_flag', 'email',
                    'type_compte', 'customer_segment', 'is_pep'],
        hard_deletes='invalidate'
    )
}}

select
    compte_id,
    tenant_id,
    numero_compte,
    iban,
    email,
    nom_client,
    prenom_client,
    type_compte,
    devise,
    statut,
    kyc_level,
    kyc_date_verification,
    aml_flag,
    customer_segment,
    is_pep,
    risk_score,
    updated_at

from {{ source('fintrack_raw', 'raw_comptes') }}

{% endsnapshot %}
