-- ============================================================
-- SCRIPT 02 — DDL des tables RAW (schéma large réaliste)
-- ============================================================
-- Reflète la complexité d'un vrai core banking system :
--   - transactions ~80 colonnes
--   - comptes ~35 colonnes avec KYC/AML
--   - fx_rates historisés
--   - tenants (multi-tenancy B2B2C)
--   - virements internes
--
-- Clustering keys posés dès la création pour éviter un reclustering coûteux.
-- ============================================================

USE ROLE FINTRACK_TRANSFORM_ROLE;
USE WAREHOUSE WH_INGESTION;
USE DATABASE FINTRACK_PROD;
USE SCHEMA RAW;

-- ============================================
-- TENANTS
-- ============================================
CREATE OR REPLACE TABLE raw_tenants (
    tenant_id               INTEGER,
    tenant_code             VARCHAR(20),
    tenant_name             VARCHAR(100),
    country_code            VARCHAR(2),
    devise_locale           VARCHAR(3),
    date_onboarding         DATE,
    sla_freshness_hours     INTEGER,
    contract_tier           VARCHAR(20),
    is_active               BOOLEAN,
    _loaded_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- CATÉGORIES (avec hiérarchie)
-- ============================================
CREATE OR REPLACE TABLE raw_categories (
    categorie_id            INTEGER,
    nom_categorie           VARCHAR(100),
    type_categorie          VARCHAR(20),
    groupe                  VARCHAR(50),
    categorie_parent_id     INTEGER,
    niveau_hierarchique     INTEGER,
    is_active               BOOLEAN,
    date_creation           DATE,
    _loaded_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- TAUX FX (historisation quotidienne)
-- ============================================
CREATE OR REPLACE TABLE raw_fx_rates (
    fx_id                   INTEGER,
    devise_source           VARCHAR(3),
    devise_cible            VARCHAR(3),
    date_cotation           DATE,
    taux                    NUMBER(18, 8),
    source_provider         VARCHAR(50),
    loaded_at               TIMESTAMP_NTZ,
    _loaded_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (date_cotation);

-- ============================================
-- COMPTES (35 colonnes — KYC, AML, préférences)
-- ============================================
CREATE OR REPLACE TABLE raw_comptes (
    compte_id                        INTEGER,
    tenant_id                        INTEGER,
    numero_compte                    VARCHAR(50),
    iban                             VARCHAR(34),
    bic                              VARCHAR(11),
    nom_client                       VARCHAR(100),
    prenom_client                    VARCHAR(100),
    email                            VARCHAR(200),
    telephone                        VARCHAR(30),
    date_naissance                   DATE,
    adresse_ligne1                   VARCHAR(200),
    adresse_ligne2                   VARCHAR(200),
    code_postal                      VARCHAR(20),
    ville                            VARCHAR(100),
    pays                             VARCHAR(2),
    type_compte                      VARCHAR(20),
    devise                           VARCHAR(3),
    solde_initial                    NUMBER(15, 2),
    solde_actuel                     NUMBER(15, 2),
    date_ouverture                   DATE,
    date_derniere_activite           DATE,
    statut                           VARCHAR(20),
    kyc_level                        VARCHAR(20),
    kyc_date_verification            DATE,
    aml_flag                         VARCHAR(30),
    customer_segment                 VARCHAR(20),
    preferred_language               VARCHAR(2),
    consent_marketing                BOOLEAN,
    consent_data_sharing             BOOLEAN,
    is_pep                           BOOLEAN,
    risk_score                       NUMBER(5, 2),
    created_at                       TIMESTAMP_NTZ,
    updated_at                       TIMESTAMP_NTZ,
    created_by_source_system         VARCHAR(50),
    _loaded_at                       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (tenant_id, date_ouverture);

-- ============================================
-- TRANSACTIONS (~80 colonnes — table large réaliste)
-- ============================================
CREATE OR REPLACE TABLE raw_transactions (
    -- Identifiants
    transaction_id                  NUMBER(18, 0),
    external_transaction_id         VARCHAR(50),
    tenant_id                       INTEGER,
    compte_id                       INTEGER,
    compte_source_iban              VARCHAR(34),
    compte_dest_iban                VARCHAR(34),
    -- Temporalité multiple (typique en finance)
    date_transaction                TIMESTAMP_NTZ,
    date_valeur                     TIMESTAMP_NTZ,
    date_comptabilisation           TIMESTAMP_NTZ,
    date_reception                  TIMESTAMP_NTZ,
    date_traitement                 TIMESTAMP_NTZ,
    date_settlement                 TIMESTAMP_NTZ,
    -- Montants et devises
    montant                         NUMBER(15, 2),
    devise                          VARCHAR(3),
    montant_eur                     NUMBER(15, 2),
    taux_change_applique            NUMBER(18, 8),
    frais                           NUMBER(10, 2),
    frais_devise                    VARCHAR(3),
    montant_net                     NUMBER(15, 2),
    type_operation                  VARCHAR(10),
    sens                            VARCHAR(10),
    -- Catégorisation
    categorie_id                    INTEGER,
    categorie_auto_detectee         BOOLEAN,
    confiance_categorie             NUMBER(5, 3),
    sous_categorie                  VARCHAR(50),
    tags                            VARCHAR(200),
    -- Marchand
    marchand_nom                    VARCHAR(200),
    marchand_id                     VARCHAR(50),
    marchand_categorie              VARCHAR(50),
    marchand_pays                   VARCHAR(2),
    marchand_mcc                    INTEGER,
    marchand_siret                  VARCHAR(20),
    -- Moyen de paiement
    moyen_paiement                  VARCHAR(20),
    carte_id                        VARCHAR(50),
    carte_last4                     VARCHAR(4),
    carte_type                      VARCHAR(20),
    carte_reseau                    VARCHAR(30),
    authentification_3ds            BOOLEAN,
    -- Contexte technique
    canal                           VARCHAR(30),
    device_type                     VARCHAR(30),
    device_id                       VARCHAR(100),
    user_agent                      VARCHAR(500),
    ip_address                      VARCHAR(45),
    session_id                      VARCHAR(100),
    geolocation_lat                 NUMBER(9, 4),
    geolocation_lon                 NUMBER(9, 4),
    -- Statut et workflow
    statut                          VARCHAR(30),
    statut_precedent                VARCHAR(30),
    nb_tentatives                   INTEGER,
    date_derniere_tentative         TIMESTAMP_NTZ,
    raison_rejet                    VARCHAR(100),
    code_erreur                     VARCHAR(20),
    message_erreur                  VARCHAR(500),
    -- Audit / compliance
    aml_score                       NUMBER(6, 2),
    aml_flag                        VARCHAR(30),
    aml_reviewed_by                 VARCHAR(50),
    aml_review_date                 TIMESTAMP_NTZ,
    fraud_score                     NUMBER(6, 2),
    fraud_rules_triggered           VARCHAR(500),
    is_flagged_for_review           BOOLEAN,
    -- Rapprochement bancaire
    is_reconciled                   BOOLEAN,
    date_reconciliation             TIMESTAMP_NTZ,
    reconciliation_batch_id         VARCHAR(50),
    external_reference              VARCHAR(50),
    internal_reference              VARCHAR(50),
    -- Description
    description                     VARCHAR(500),
    description_brute               VARCHAR(500),
    description_enrichie            VARCHAR(500),
    libelle_court                   VARCHAR(50),
    -- Métadonnées
    source_system                   VARCHAR(50),
    source_file_batch_id            VARCHAR(50),
    created_at                      TIMESTAMP_NTZ,
    updated_at                      TIMESTAMP_NTZ,
    loaded_at                       TIMESTAMP_NTZ,
    processing_version              VARCHAR(20),
    is_test                         BOOLEAN,
    is_reversal                     BOOLEAN,
    original_transaction_id         NUMBER(18, 0),
    _loaded_at                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (tenant_id, DATE_TRUNC('MONTH', date_transaction));

-- ============================================
-- VIREMENTS INTERNES
-- ============================================
CREATE OR REPLACE TABLE raw_virements (
    virement_id             INTEGER,
    tenant_id               INTEGER,
    compte_source_id        INTEGER,
    compte_dest_id          INTEGER,
    montant                 NUMBER(15, 2),
    devise                  VARCHAR(3),
    date_virement           TIMESTAMP_NTZ,
    date_execution          TIMESTAMP_NTZ,
    motif                   VARCHAR(500),
    reference               VARCHAR(50),
    statut                  VARCHAR(20),
    type_virement           VARCHAR(20),
    frais                   NUMBER(10, 2),
    created_at              TIMESTAMP_NTZ,
    updated_at              TIMESTAMP_NTZ,
    _loaded_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (tenant_id, DATE_TRUNC('MONTH', date_virement));

-- ============================================
-- QUERY TAG PAR DÉFAUT
-- ============================================
-- Utile pour tracer les coûts d'ingestion via QUERY_HISTORY
ALTER SESSION SET QUERY_TAG = 'ddl_raw_tables|fintrack_perf_scale';
