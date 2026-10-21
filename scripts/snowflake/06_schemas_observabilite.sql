-- ============================================================
-- SCRIPT 06 — Schémas prod pour l'observabilité dbt (sprint 6)
-- ============================================================
-- À exécuter une fois en ACCOUNTADMIN avant le premier `dbt build --target prod`.
-- Idempotent : rejouable sans effet de bord.
--
-- FINTRACK_TRANSFORM_ROLE n'a que USAGE sur la base : il ne peut pas créer
-- lui-même les schémas ajoutés aux sprints 5-6. Sans ce script, le build prod
-- échoue avec « Schema 'FINTRACK_PROD.AUDIT_TEST_FAILURES' / 'ELEMENTARY'
-- does not exist or not authorized ».
--   - AUDIT_TEST_FAILURES : tables store_failures des tests (prod uniquement)
--   - ELEMENTARY          : tables de résultats / métriques Elementary
--   - ELEMENTARY__TESTS   : tables temporaires des tests d'anomalie Elementary

USE ROLE ACCOUNTADMIN;

CREATE SCHEMA IF NOT EXISTS FINTRACK_PROD.AUDIT_TEST_FAILURES;
CREATE SCHEMA IF NOT EXISTS FINTRACK_PROD.ELEMENTARY;
CREATE SCHEMA IF NOT EXISTS FINTRACK_PROD.ELEMENTARY__TESTS;

GRANT ALL ON SCHEMA FINTRACK_PROD.AUDIT_TEST_FAILURES TO ROLE FINTRACK_TRANSFORM_ROLE;
GRANT ALL ON SCHEMA FINTRACK_PROD.ELEMENTARY TO ROLE FINTRACK_TRANSFORM_ROLE;
GRANT ALL ON SCHEMA FINTRACK_PROD.ELEMENTARY__TESTS TO ROLE FINTRACK_TRANSFORM_ROLE;

-- Si un schéma existait déjà (créé par un autre rôle), les tables qu'il
-- contient restent hors de portée : dbt doit pouvoir les remplacer.
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA FINTRACK_PROD.AUDIT_TEST_FAILURES
    TO ROLE FINTRACK_TRANSFORM_ROLE COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA FINTRACK_PROD.ELEMENTARY
    TO ROLE FINTRACK_TRANSFORM_ROLE COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA FINTRACK_PROD.ELEMENTARY
    TO ROLE FINTRACK_TRANSFORM_ROLE COPY CURRENT GRANTS;

-- Lecture des résultats Elementary par l'équipe (rapport edr)
GRANT USAGE ON SCHEMA FINTRACK_PROD.ELEMENTARY TO ROLE FINTRACK_ADHOC_ROLE;
GRANT USAGE ON SCHEMA FINTRACK_PROD.AUDIT_TEST_FAILURES TO ROLE FINTRACK_ADHOC_ROLE;
GRANT USAGE ON SCHEMA FINTRACK_PROD.ELEMENTARY TO ROLE FINTRACK_DEV_ROLE;
GRANT USAGE ON SCHEMA FINTRACK_PROD.AUDIT_TEST_FAILURES TO ROLE FINTRACK_DEV_ROLE;
