-- ============================================================
-- SCRIPT 03 — Stage interne et COPY INTO
-- ============================================================
-- Approche production : les CSV générés par le script Python sont
-- uploadés vers un stage interne Snowflake, puis chargés en bulk
-- avec COPY INTO (parallélisation native, gestion des erreurs).
--
-- Instructions :
--   1. Générer les données : python scripts/python/generate_data.py --scale M
--   2. Exécuter la partie STAGE + FILE FORMAT ci-dessous
--   3. Uploader les fichiers via SnowSQL :
--        PUT file://data/raw/*.csv.gz @FINTRACK_STAGE AUTO_COMPRESS=FALSE;
--   4. Exécuter les COPY INTO
-- ============================================================

USE ROLE FINTRACK_INGESTION_ROLE;
USE WAREHOUSE WH_INGESTION;
USE DATABASE FINTRACK_PROD;
USE SCHEMA RAW;

ALTER SESSION SET QUERY_TAG = 'stage_and_copy|fintrack_perf_scale';

-- ============================================
-- FILE FORMAT réutilisable
-- ============================================
-- Correctif du 20/10/2026 : l'ancien format (SKIP_HEADER = 1, chargement
-- positionnel) laissait _loaded_at à NULL. Le CSV n'a pas cette colonne, et
-- avec ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE Snowflake remplit la colonne
-- manquante avec NULL au lieu d'appliquer le DEFAULT CURRENT_TIMESTAMP().
-- Conséquence : aucun filtre incrémental sur _loaded_at ne fonctionnait.
-- Désormais : en-tête lu (PARSE_HEADER), chargement par nom de colonne et
-- _loaded_at alimenté par INCLUDE_METADATA.
CREATE OR REPLACE FILE FORMAT ff_csv_gz
    TYPE = CSV
    COMPRESSION = GZIP
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    PARSE_HEADER = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE
    ENCODING = 'UTF-8'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    TRIM_SPACE = TRUE;

-- ============================================
-- STAGE INTERNE
-- ============================================
CREATE OR REPLACE STAGE fintrack_stage
    FILE_FORMAT = ff_csv_gz
    COMMENT = 'Stage interne pour les CSV générés par le script Python';

-- ============================================
-- COPY INTO — commandes à exécuter APRÈS le PUT
-- ============================================

-- Tenants (petit fichier)
COPY INTO raw_tenants
FROM @fintrack_stage/raw_tenants.csv.gz
FILE_FORMAT = (FORMAT_NAME = ff_csv_gz)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (_loaded_at = METADATA$START_SCAN_TIME)
ON_ERROR = 'ABORT_STATEMENT';

-- Catégories
COPY INTO raw_categories
FROM @fintrack_stage/raw_categories.csv.gz
FILE_FORMAT = (FORMAT_NAME = ff_csv_gz)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (_loaded_at = METADATA$START_SCAN_TIME)
ON_ERROR = 'ABORT_STATEMENT';

-- FX rates
COPY INTO raw_fx_rates
FROM @fintrack_stage/raw_fx_rates.csv.gz
FILE_FORMAT = (FORMAT_NAME = ff_csv_gz)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (_loaded_at = METADATA$START_SCAN_TIME)
ON_ERROR = 'ABORT_STATEMENT';

-- Comptes
COPY INTO raw_comptes
FROM @fintrack_stage/raw_comptes.csv.gz
FILE_FORMAT = (FORMAT_NAME = ff_csv_gz)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (_loaded_at = METADATA$START_SCAN_TIME)
ON_ERROR = 'CONTINUE'
RETURN_FAILED_ONLY = TRUE;

-- Transactions — gros volume, ON_ERROR CONTINUE pour tolérance
COPY INTO raw_transactions
FROM @fintrack_stage/raw_transactions.csv.gz
FILE_FORMAT = (FORMAT_NAME = ff_csv_gz)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (_loaded_at = METADATA$START_SCAN_TIME)
ON_ERROR = 'CONTINUE'
RETURN_FAILED_ONLY = TRUE;

-- Virements
COPY INTO raw_virements
FROM @fintrack_stage/raw_virements.csv.gz
FILE_FORMAT = (FORMAT_NAME = ff_csv_gz)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (_loaded_at = METADATA$START_SCAN_TIME)
ON_ERROR = 'CONTINUE'
RETURN_FAILED_ONLY = TRUE;

-- Titulaires et liens compte ↔ titulaire (Sprint 2, bridge)
COPY INTO raw_titulaires
FROM @fintrack_stage/raw_titulaires.csv.gz
FILE_FORMAT = (FORMAT_NAME = ff_csv_gz)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (_loaded_at = METADATA$START_SCAN_TIME)
ON_ERROR = 'CONTINUE'
RETURN_FAILED_ONLY = TRUE;

COPY INTO raw_compte_titulaires
FROM @fintrack_stage/raw_compte_titulaires.csv.gz
FILE_FORMAT = (FORMAT_NAME = ff_csv_gz)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (_loaded_at = METADATA$START_SCAN_TIME)
ON_ERROR = 'CONTINUE'
RETURN_FAILED_ONLY = TRUE;

-- ============================================
-- VALIDATION POST-LOAD
-- ============================================
SELECT 'raw_tenants'      AS table_name, COUNT(*) AS row_count FROM raw_tenants
UNION ALL SELECT 'raw_categories',    COUNT(*) FROM raw_categories
UNION ALL SELECT 'raw_fx_rates',      COUNT(*) FROM raw_fx_rates
UNION ALL SELECT 'raw_comptes',       COUNT(*) FROM raw_comptes
UNION ALL SELECT 'raw_transactions',  COUNT(*) FROM raw_transactions
UNION ALL SELECT 'raw_virements',     COUNT(*) FROM raw_virements
UNION ALL SELECT 'raw_titulaires',    COUNT(*) FROM raw_titulaires
UNION ALL SELECT 'raw_compte_titulaires', COUNT(*) FROM raw_compte_titulaires
ORDER BY table_name;

-- Contrôle du correctif : aucune ligne sans _loaded_at
SELECT 'raw_transactions' AS table_name, COUNT(*) - COUNT(_loaded_at) AS nb_sans_loaded_at FROM raw_transactions
UNION ALL SELECT 'raw_comptes',   COUNT(*) - COUNT(_loaded_at) FROM raw_comptes
UNION ALL SELECT 'raw_virements', COUNT(*) - COUNT(_loaded_at) FROM raw_virements;

-- ============================================
-- MONITORING : historique des COPY
-- ============================================
SELECT file_name, status, row_count, row_parsed, error_count, last_load_time
FROM INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW.RAW_TRANSACTIONS',
    START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
)
ORDER BY last_load_time DESC;
