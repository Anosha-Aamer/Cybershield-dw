-- =====================================================================
-- 01  Audit tables  (schema project_audit)
-- Every pipeline run, every batch and every ETL step writes here, so we can
-- answer: what was loaded, when, how many rows, what was rejected and why.
-- =====================================================================

-- one row per pipeline execution
CREATE TABLE IF NOT EXISTS project_audit.etl_run (
    run_id        INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at    TIMESTAMP NOT NULL DEFAULT now(),
    finished_at   TIMESTAMP,
    status        VARCHAR(20) NOT NULL DEFAULT 'RUNNING',      -- RUNNING / SUCCESS / FAILED
    triggered_by  VARCHAR(100) NOT NULL DEFAULT current_user,
    run_args      TEXT
);

-- one row per batch per run: row counts at every layer (used for reconciliation)
CREATE TABLE IF NOT EXISTS project_audit.etl_batch_log (
    batch_log_id        INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id              INT NOT NULL REFERENCES project_audit.etl_run(run_id),
    batch_id            VARCHAR(7) NOT NULL,                    -- YYYY-MM
    source_system       VARCHAR(100) NOT NULL DEFAULT 'CyberShield Incident Source API',
    started_at          TIMESTAMP NOT NULL DEFAULT now(),
    finished_at         TIMESTAMP,
    status              VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    rows_api_reported   INT,     -- total_rows the API said the batch has
    rows_extracted      INT,     -- rows received over HTTP
    rows_bronze         INT,     -- rows written to raw_incident_event
    rows_duplicate      INT,     -- removed as duplicates in silver
    rows_rejected       INT,     -- sent to etl_reject_log
    rows_silver         INT,     -- rows in stg_incident_event for the batch
    rows_fact_activity  INT,     -- rows inserted in fact_response_activity
    incidents_touched   INT,     -- incidents upserted in fact_incident / lifecycle
    error_message       TEXT
);

-- one row per step (extract, bronze, silver, dimensions, facts, dq) - timing + counts
CREATE TABLE IF NOT EXISTS project_audit.etl_step_log (
    step_log_id   INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id        INT NOT NULL REFERENCES project_audit.etl_run(run_id),
    batch_id      VARCHAR(7),
    step_name     VARCHAR(50) NOT NULL,
    started_at    TIMESTAMP NOT NULL DEFAULT now(),
    finished_at   TIMESTAMP,
    status        VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    rows_in       INT,
    rows_out      INT,
    message       TEXT
);

-- rows that could not be repaired; kept whole (JSON) with the reason
CREATE TABLE IF NOT EXISTS project_audit.etl_reject_log (
    reject_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id        INT NOT NULL REFERENCES project_audit.etl_run(run_id),
    batch_id      VARCHAR(7) NOT NULL,
    raw_row_id    BIGINT,
    reject_reason VARCHAR(200) NOT NULL,
    source_row    JSONB NOT NULL,
    rejected_at   TIMESTAMP NOT NULL DEFAULT now()
);

-- every data-quality check result, per run and batch
CREATE TABLE IF NOT EXISTS project_audit.dq_check_result (
    check_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id          INT NOT NULL REFERENCES project_audit.etl_run(run_id),
    batch_id        VARCHAR(7),                 -- NULL = whole-warehouse check
    check_category  VARCHAR(30) NOT NULL,       -- Completeness / Accuracy / Integrity / Validity / Uniqueness
    check_name      VARCHAR(150) NOT NULL,
    table_name      VARCHAR(100),
    expected_value  NUMERIC,
    actual_value    NUMERIC,
    passed          BOOLEAN NOT NULL,
    details         TEXT,
    checked_at      TIMESTAMP NOT NULL DEFAULT now()
);

-- incremental loading: last batch that finished successfully
CREATE TABLE IF NOT EXISTS project_audit.etl_watermark (
    pipeline_name   VARCHAR(50) PRIMARY KEY,
    last_batch_id   VARCHAR(7),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);
INSERT INTO project_audit.etl_watermark (pipeline_name, last_batch_id)
VALUES ('incident_events', NULL) ON CONFLICT DO NOTHING;

CREATE INDEX IF NOT EXISTS ix_reject_batch ON project_audit.etl_reject_log (batch_id);
CREATE INDEX IF NOT EXISTS ix_dq_run       ON project_audit.dq_check_result (run_id);
