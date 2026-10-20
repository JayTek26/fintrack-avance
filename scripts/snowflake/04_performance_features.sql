-- ============================================================
-- SCRIPT 04 — Features de performance Snowflake (à activer selon besoin)
-- ============================================================
-- Ce script regroupe les features à activer une fois les données chargées :
--   - Search Optimization Service
--   - Clustering post-load (si non fait à la création)
--   - Materialized Views
--   - Automatic Clustering
--
-- ATTENTION : plusieurs de ces features consomment des crédits en continu.
-- N'activer que ce dont vous avez besoin pour vos tests.
-- ============================================================

USE ROLE FINTRACK_TRANSFORM_ROLE;
USE WAREHOUSE WH_TRANSFORM;
USE DATABASE FINTRACK_PROD;

-- ============================================
-- 1. SEARCH OPTIMIZATION SERVICE
-- ============================================
-- Accélère les recherches ponctuelles (equality, IN, LIKE) sur des colonnes
-- non incluses dans le clustering key. Coûte en fonction du volume indexé.
--
-- À activer sur les colonnes de lookup fréquentes :
--   ALTER TABLE RAW.raw_transactions ADD SEARCH OPTIMIZATION ON EQUALITY(external_transaction_id);
--   ALTER TABLE RAW.raw_transactions ADD SEARCH OPTIMIZATION ON EQUALITY(compte_id);
--   ALTER TABLE RAW.raw_comptes ADD SEARCH OPTIMIZATION ON EQUALITY(iban, email);

-- Vérifier l'état :
-- SELECT SYSTEM$ESTIMATE_SEARCH_OPTIMIZATION_COSTS('RAW.raw_transactions');
-- SHOW TABLES LIKE 'raw_transactions' IN SCHEMA RAW;  -- colonne SEARCH_OPTIMIZATION

-- ============================================
-- 2. AUTOMATIC CLUSTERING
-- ============================================
-- Snowflake reclustered automatiquement les tables avec CLUSTER BY.
-- Vérifier la qualité du clustering :

SELECT SYSTEM$CLUSTERING_INFORMATION('RAW.raw_transactions', '(tenant_id, DATE_TRUNC(''MONTH'', date_transaction))');
SELECT SYSTEM$CLUSTERING_DEPTH('RAW.raw_transactions');

-- Si le clustering se dégrade (depth élevée), on peut suspendre/reprendre :
-- ALTER TABLE RAW.raw_transactions SUSPEND RECLUSTER;
-- ALTER TABLE RAW.raw_transactions RESUME RECLUSTER;

-- ============================================
-- 3. QUERY ACCELERATION SERVICE
-- ============================================
-- Accélère les gros scans pour les warehouses eligibles.
-- ALTER WAREHOUSE WH_REPORTING SET
--     ENABLE_QUERY_ACCELERATION = TRUE
--     QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8;

-- ============================================
-- 4. RESULT CACHE
-- ============================================
-- Activé par défaut au niveau session. Pour désactiver ponctuellement :
-- ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- Pour observer si une requête utilise le cache :
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 10));
-- Voir la colonne EXECUTION_STATUS = 'RESULT_FROM_CACHE'

-- ============================================
-- 5. QUERY TAGS pour tracking des coûts
-- ============================================
-- Convention recommandée : "<équipe>|<projet>|<modèle>|<env>"
-- ALTER SESSION SET QUERY_TAG = 'data-eng|fintrack|fct_transactions|prod';

-- Analyser les coûts par tag :
-- SELECT
--     query_tag,
--     COUNT(*) AS nb_queries,
--     SUM(credits_used_cloud_services) AS credits_cloud,
--     AVG(total_elapsed_time) / 1000 AS avg_elapsed_seconds
-- FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
-- WHERE start_time > DATEADD(day, -7, CURRENT_TIMESTAMP())
--   AND query_tag LIKE 'data-eng|fintrack%'
-- GROUP BY query_tag
-- ORDER BY credits_cloud DESC;

-- ============================================
-- 6. MATERIALIZED VIEW (exemple)
-- ============================================
-- Cas d'usage : requête très fréquente, agrégation stable, faible latence.
-- Coût continu de maintenance sur les updates.

-- CREATE OR REPLACE MATERIALIZED VIEW MARTS_ANALYTICS.mv_transactions_daily_summary
-- AS
-- SELECT
--     tenant_id,
--     DATE(date_transaction) AS jour,
--     COUNT(*) AS nb_transactions,
--     SUM(montant_eur) AS montant_total_eur,
--     COUNT(DISTINCT compte_id) AS nb_comptes_actifs
-- FROM RAW.raw_transactions
-- WHERE statut = 'validee'
-- GROUP BY tenant_id, DATE(date_transaction);

-- ============================================
-- 7. QUERY_PROFILE : à ouvrir depuis la console Snowsight
-- ============================================
-- Pour analyser une requête :
--   1. Onglet History
--   2. Cliquer sur la query
--   3. Onglet "Profile"
--   4. Vérifier :
--      - Partitions scanned vs total (pruning)
--      - % time by node (join, aggregate, sort)
--      - Bytes spilled (mémoire insuffisante)
