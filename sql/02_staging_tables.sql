-- =====================================================================
-- 02  Staging layer (schema project_staging)
--   raw_*  = BRONZE : exactly what the API returned, every column TEXT
--   ref_*  = reference mappings (location -> city, category -> incident type)
--   stg_*  = SILVER : cleaned, typed, de-duplicated, mapped
-- =====================================================================

-- ---------- BRONZE ----------
CREATE TABLE IF NOT EXISTS project_staging.raw_incident_event (
    raw_row_id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id                VARCHAR(7) NOT NULL,
    api_page                INT,
    load_ts                 TIMESTAMP NOT NULL DEFAULT now(),
    number TEXT, incident_state TEXT, active TEXT, reassignment_count TEXT, reopen_count TEXT,
    sys_mod_count TEXT, made_sla TEXT, caller_id TEXT, opened_by TEXT, opened_at TEXT,
    sys_created_by TEXT, sys_created_at TEXT, sys_updated_by TEXT, sys_updated_at TEXT,
    contact_type TEXT, location TEXT, category TEXT, subcategory TEXT, u_symptom TEXT, cmdb_ci TEXT,
    impact TEXT, urgency TEXT, priority TEXT, assignment_group TEXT, assigned_to TEXT, knowledge TEXT,
    u_priority_confirmation TEXT, notify TEXT, problem_id TEXT, rfc TEXT, vendor TEXT, caused_by TEXT,
    closed_code TEXT, resolved_by TEXT, resolved_at TEXT, closed_at TEXT
);
CREATE INDEX IF NOT EXISTS ix_raw_batch ON project_staging.raw_incident_event (batch_id);

-- ---------- REFERENCE MAPPINGS ----------
CREATE TABLE IF NOT EXISTS project_staging.ref_location_map (
    location_code VARCHAR(30) PRIMARY KEY,   -- 'Location 143'
    site_name     VARCHAR(100) NOT NULL,     -- 'Lahore - Head Office'
    city          VARCHAR(50)  NOT NULL,
    province      VARCHAR(50)  NOT NULL,
    region        VARCHAR(50)  NOT NULL,
    country       VARCHAR(50)  NOT NULL
);

CREATE TABLE IF NOT EXISTS project_staging.ref_category_map (
    category_code VARCHAR(30) PRIMARY KEY,   -- 'Category 26'
    category_name VARCHAR(100) NOT NULL,     -- 'Phishing Email'
    attack_family VARCHAR(50)  NOT NULL      -- 'Social Engineering'
);

-- ---------- SILVER: one clean row per update event ----------
CREATE TABLE IF NOT EXISTS project_staging.stg_incident_event (
    stg_event_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id                VARCHAR(7)  NOT NULL,
    raw_row_id              BIGINT      NOT NULL,
    incident_number         VARCHAR(15) NOT NULL,
    incident_state          VARCHAR(30) NOT NULL,
    is_active               BOOLEAN,
    reassignment_count      INT,
    reopen_count            INT,
    sys_mod_count           INT         NOT NULL,
    made_sla                BOOLEAN,
    caller_id               VARCHAR(30),
    opened_at               TIMESTAMP   NOT NULL,
    sys_updated_at          TIMESTAMP   NOT NULL,
    sys_updated_by          VARCHAR(30),
    contact_type            VARCHAR(30),
    location_code           VARCHAR(30),           -- NULL = unknown / not in reference map
    category_code           VARCHAR(30),
    subcategory_code        VARCHAR(30),
    symptom                 VARCHAR(30),
    impact                  VARCHAR(20),
    urgency                 VARCHAR(20),
    priority                VARCHAR(20),
    assignment_group        VARCHAR(30),
    assigned_to             VARCHAR(30),
    knowledge               BOOLEAN,
    priority_confirmation   BOOLEAN,
    closed_code             VARCHAR(20),
    resolved_by             VARCHAR(30),
    resolved_at             TIMESTAMP,
    closed_at               TIMESTAMP,
    dq_flags                VARCHAR(300),          -- what the cleaning changed on this row
    CONSTRAINT uq_stg_event UNIQUE (incident_number, sys_mod_count, sys_updated_at)
);
CREATE INDEX IF NOT EXISTS ix_stg_event_batch    ON project_staging.stg_incident_event (batch_id);
CREATE INDEX IF NOT EXISTS ix_stg_event_incident ON project_staging.stg_incident_event (incident_number, sys_updated_at);

-- ---------- SILVER: one row per incident (input for FactIncident + FactIncidentLifecycle) ----------
CREATE TABLE IF NOT EXISTS project_staging.stg_incident (
    incident_number         VARCHAR(15) PRIMARY KEY,
    caller_id               VARCHAR(30),
    opened_at               TIMESTAMP NOT NULL,
    -- attributes as reported (first event)
    reported_location_code  VARCHAR(30),
    reported_category_code  VARCHAR(30),
    reported_subcategory    VARCHAR(30),
    reported_impact         VARCHAR(20),
    reported_urgency        VARCHAR(20),
    reported_priority       VARCHAR(20),
    contact_type            VARCHAR(30),
    -- state after the latest event
    final_location_code     VARCHAR(30),
    final_category_code     VARCHAR(30),
    final_subcategory       VARCHAR(30),
    final_impact            VARCHAR(20),
    final_urgency           VARCHAR(20),
    final_priority          VARCHAR(20),
    final_state             VARCHAR(30),
    final_assignment_group  VARCHAR(30),
    final_made_sla          BOOLEAN,
    reassignment_count      INT,
    reopen_count            INT,
    event_count             INT,
    -- lifecycle milestones
    first_response_at       TIMESTAMP,
    resolved_at             TIMESTAMP,
    closed_at               TIMESTAMP,
    last_event_at           TIMESTAMP,
    last_batch_id           VARCHAR(7)
);
