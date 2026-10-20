-- COPY INTO incrémental appelé par le DAG fintrack_hourly_pipeline.
-- Même file format que scripts/snowflake/03_stage_and_copy.sql
-- (PARSE_HEADER + MATCH_BY_COLUMN_NAME + INCLUDE_METADATA pour _loaded_at).
-- RAW est en append-only : une mise à jour de statut arrive comme une
-- nouvelle ligne, dédupliquée dans stg_transactions (QUALIFY). Ne pas faire
-- d'UPDATE en place dans RAW : ça casse le pruning sur _loaded_at
-- (cf. docs/PERF_LOG.md).
-- COPY ne recharge pas un fichier déjà chargé (load metadata, 64 jours) :
-- la tâche est rejouable sans doublon.
USE ROLE FINTRACK_INGESTION_ROLE;
USE WAREHOUSE WH_INGESTION;
USE SCHEMA FINTRACK_PROD.RAW;
ALTER SESSION SET QUERY_TAG = 'team=data-eng|project=fintrack-perf-scale|target=prod|step=copy_into_raw|run={{ run_id }}';

COPY INTO raw_transactions
FROM @fintrack_stage
PATTERN = '.*raw_transactions.*[.]csv[.]gz'
FILE_FORMAT = (FORMAT_NAME = ff_csv_gz)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (_loaded_at = METADATA$START_SCAN_TIME)
ON_ERROR = 'CONTINUE';

COPY INTO raw_virements
FROM @fintrack_stage
PATTERN = '.*raw_virements.*[.]csv[.]gz'
FILE_FORMAT = (FORMAT_NAME = ff_csv_gz)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (_loaded_at = METADATA$START_SCAN_TIME)
ON_ERROR = 'CONTINUE';

COPY INTO raw_comptes
FROM @fintrack_stage
PATTERN = '.*raw_comptes.*[.]csv[.]gz'
FILE_FORMAT = (FORMAT_NAME = ff_csv_gz)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (_loaded_at = METADATA$START_SCAN_TIME)
ON_ERROR = 'CONTINUE';

-- Échoue la tâche (→ retry + Slack) si des lignes ont été rejetées
SELECT 1 / IFF(SUM(error_count) > 0, 0, 1) AS assert_zero_erreur
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW_TRANSACTIONS',
    START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
));
