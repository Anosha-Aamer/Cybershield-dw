-- =====================================================================
-- 00  Database + schemas
-- Run once as the postgres user:
--   psql -U postgres -c "CREATE DATABASE cybershield_dw;"
--   psql -U postgres -d cybershield_dw -f sql/00_create_db_schemas.sql
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS project_staging;    -- bronze (raw API rows) + silver (clean rows)
CREATE SCHEMA IF NOT EXISTS project_dw;         -- gold: star schema (dimensions + facts)
CREATE SCHEMA IF NOT EXISTS project_reporting;  -- KPI views for dashboards
CREATE SCHEMA IF NOT EXISTS project_audit;      -- ETL audit trail, rejects, data-quality results

COMMENT ON SCHEMA project_staging   IS 'Bronze raw_* tables (as received from the API) and silver stg_* tables (cleaned)';
COMMENT ON SCHEMA project_dw        IS 'Star schema: conformed dimensions and three fact tables';
COMMENT ON SCHEMA project_reporting IS 'Reporting views used by BI dashboards';
COMMENT ON SCHEMA project_audit     IS 'ETL run/batch audit, rejected rows, data-quality check results';
