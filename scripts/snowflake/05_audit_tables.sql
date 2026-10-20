-- ============================================================
-- SCRIPT 05 — Tables d'audit pour hooks dbt
-- ============================================================

USE ROLE FINTRACK_TRANSFORM_ROLE;
USE DATABASE FINTRACK_PROD;
USE SCHEMA AUDIT;

CREATE TABLE IF NOT EXISTS dbt_run_log (
    run_id                  VARCHAR(100),
    model_name              VARCHAR(200),
    phase                   VARCHAR(20),          -- 'pre' | 'post'
    invocation_id           VARCHAR(100),
    target_name             VARCHAR(50),
    run_started_at          TIMESTAMP_NTZ,
    logged_at               TIMESTAMP_NTZ,
    max_date_transaction_processed TIMESTAMP_NTZ,
    row_count               INTEGER
);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    metric_date             DATE,
    metric_name             VARCHAR(100),
    metric_value            NUMBER(20, 4),
    context                 VARIANT,
    logged_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
