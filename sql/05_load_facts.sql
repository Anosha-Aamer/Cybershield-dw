-- =====================================================================
-- 05  Fact load, run once per batch after load_dimensions
--   CALL project_dw.load_facts('2016-03');
-- Business key -> surrogate key: every fact row looks up each dimension by its
-- natural key; no match (missing / unknown value) -> -1 Unknown member.
-- Re-running a batch is safe: incident facts are upserted, activity rows of the
-- batch are deleted and re-inserted.
-- =====================================================================

-- small helpers so the lookups read clearly
CREATE OR REPLACE FUNCTION project_dw.date_key(ts TIMESTAMP) RETURNS INT
LANGUAGE sql IMMUTABLE AS $$ SELECT COALESCE(TO_CHAR(ts, 'YYYYMMDD')::INT, -1) $$;

CREATE OR REPLACE FUNCTION project_dw.time_key(ts TIMESTAMP) RETURNS INT
LANGUAGE sql IMMUTABLE AS $$ SELECT COALESCE((EXTRACT(HOUR FROM ts) * 100 + EXTRACT(MINUTE FROM ts))::INT, -1) $$;

CREATE OR REPLACE FUNCTION project_dw.minutes_between(a TIMESTAMP, b TIMESTAMP) RETURNS INT
LANGUAGE sql IMMUTABLE AS $$ SELECT CASE WHEN b >= a THEN (EXTRACT(EPOCH FROM (b - a)) / 60)::INT END $$;

CREATE OR REPLACE PROCEDURE project_dw.load_facts(p_batch VARCHAR)
LANGUAGE plpgsql AS $$
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tmp_touched (incident_number VARCHAR(15) PRIMARY KEY) ON COMMIT DROP;
    TRUNCATE tmp_touched;
    INSERT INTO tmp_touched
    SELECT DISTINCT incident_number FROM project_staging.stg_incident_event WHERE batch_id = p_batch;

    -- ---------- FactIncident: attributes as reported ----------
    INSERT INTO project_dw.fact_incident AS f (
        incident_number, caller_id, reported_date_key, reported_time_key, location_key, category_key,
        severity_key, contact_type_key, incident_count, reassignment_count, reopen_count, first_batch_id, last_batch_id)
    SELECT s.incident_number, s.caller_id,
           project_dw.date_key(s.opened_at), project_dw.time_key(s.opened_at),
           COALESCE(dl.location_key, -1), COALESCE(dc.category_key, -1),
           COALESCE(ds.severity_key, -1), COALESCE(dct.contact_type_key, -1),
           1, s.reassignment_count, s.reopen_count, p_batch, s.last_batch_id
    FROM project_staging.stg_incident s
    JOIN tmp_touched t USING (incident_number)
    LEFT JOIN project_dw.dim_location dl  ON dl.location_code = s.reported_location_code
    LEFT JOIN project_dw.dim_category dc  ON dc.category_code = s.reported_category_code
                                         AND dc.subcategory_code = COALESCE(s.reported_subcategory, 'Unknown')
    LEFT JOIN project_dw.dim_severity ds  ON ds.priority_level = s.reported_priority
                                         AND ds.impact_level = s.reported_impact AND ds.urgency_level = s.reported_urgency
    LEFT JOIN project_dw.dim_contact_type dct ON dct.contact_type_name = s.contact_type
    ON CONFLICT (incident_number) DO UPDATE SET
        caller_id = EXCLUDED.caller_id, reported_date_key = EXCLUDED.reported_date_key,
        reported_time_key = EXCLUDED.reported_time_key, location_key = EXCLUDED.location_key,
        category_key = EXCLUDED.category_key, severity_key = EXCLUDED.severity_key,
        contact_type_key = EXCLUDED.contact_type_key, reassignment_count = EXCLUDED.reassignment_count,
        reopen_count = EXCLUDED.reopen_count, last_batch_id = EXCLUDED.last_batch_id, dw_updated_at = now();

    -- ---------- FactIncidentLifecycle: accumulating snapshot (current values) ----------
    INSERT INTO project_dw.fact_incident_lifecycle AS f (
        incident_key, incident_number, caller_id,
        reported_date_key, first_response_date_key, resolved_date_key, closed_date_key,
        reported_time_key, first_response_time_key,
        location_key, category_key, severity_key, current_status_key, current_team_key, contact_type_key,
        reported_at, first_response_at, resolved_at, closed_at,
        incident_count, event_count, reassignment_count, reopen_count,
        response_duration_minutes, resolution_duration_minutes, closure_duration_minutes, response_to_resolution_minutes,
        sla_compliance_count, resolved_count, open_count, last_batch_id)
    SELECT fi.incident_key, s.incident_number, s.caller_id,
           project_dw.date_key(s.opened_at), project_dw.date_key(s.first_response_at),
           project_dw.date_key(s.resolved_at), project_dw.date_key(s.closed_at),
           project_dw.time_key(s.opened_at), project_dw.time_key(s.first_response_at),
           COALESCE(dl.location_key, -1), COALESCE(dc.category_key, -1), COALESCE(ds.severity_key, -1),
           COALESCE(st.status_key, -1), COALESCE(tm.team_key, -1), COALESCE(dct.contact_type_key, -1),
           s.opened_at, s.first_response_at, s.resolved_at, s.closed_at,
           1, s.event_count, s.reassignment_count, s.reopen_count,
           project_dw.minutes_between(s.opened_at, s.first_response_at),
           project_dw.minutes_between(s.opened_at, s.resolved_at),
           project_dw.minutes_between(s.opened_at, s.closed_at),
           project_dw.minutes_between(s.first_response_at, s.resolved_at),
           CASE WHEN s.final_made_sla THEN 1 ELSE 0 END,
           CASE WHEN s.final_state IN ('Resolved', 'Closed') THEN 1 ELSE 0 END,
           CASE WHEN s.final_state IN ('Resolved', 'Closed') THEN 0 ELSE 1 END,
           s.last_batch_id
    FROM project_staging.stg_incident s
    JOIN tmp_touched t USING (incident_number)
    JOIN project_dw.fact_incident fi USING (incident_number)
    LEFT JOIN project_dw.dim_location dl  ON dl.location_code = s.final_location_code
    LEFT JOIN project_dw.dim_category dc  ON dc.category_code = s.final_category_code
                                         AND dc.subcategory_code = COALESCE(s.final_subcategory, 'Unknown')
    LEFT JOIN project_dw.dim_severity ds  ON ds.priority_level = s.final_priority
                                         AND ds.impact_level = s.final_impact AND ds.urgency_level = s.final_urgency
    LEFT JOIN project_dw.dim_status st    ON st.status_name = s.final_state
    LEFT JOIN project_dw.dim_response_team tm ON tm.team_code = s.final_assignment_group
    LEFT JOIN project_dw.dim_contact_type dct ON dct.contact_type_name = s.contact_type
    ON CONFLICT (incident_key) DO UPDATE SET
        caller_id = EXCLUDED.caller_id,
        reported_date_key = EXCLUDED.reported_date_key, first_response_date_key = EXCLUDED.first_response_date_key,
        resolved_date_key = EXCLUDED.resolved_date_key, closed_date_key = EXCLUDED.closed_date_key,
        reported_time_key = EXCLUDED.reported_time_key, first_response_time_key = EXCLUDED.first_response_time_key,
        location_key = EXCLUDED.location_key, category_key = EXCLUDED.category_key, severity_key = EXCLUDED.severity_key,
        current_status_key = EXCLUDED.current_status_key, current_team_key = EXCLUDED.current_team_key,
        contact_type_key = EXCLUDED.contact_type_key,
        reported_at = EXCLUDED.reported_at, first_response_at = EXCLUDED.first_response_at,
        resolved_at = EXCLUDED.resolved_at, closed_at = EXCLUDED.closed_at,
        event_count = EXCLUDED.event_count, reassignment_count = EXCLUDED.reassignment_count,
        reopen_count = EXCLUDED.reopen_count,
        response_duration_minutes = EXCLUDED.response_duration_minutes,
        resolution_duration_minutes = EXCLUDED.resolution_duration_minutes,
        closure_duration_minutes = EXCLUDED.closure_duration_minutes,
        response_to_resolution_minutes = EXCLUDED.response_to_resolution_minutes,
        sla_compliance_count = EXCLUDED.sla_compliance_count, resolved_count = EXCLUDED.resolved_count,
        open_count = EXCLUDED.open_count, last_batch_id = EXCLUDED.last_batch_id, dw_updated_at = now();

    -- ---------- FactResponseActivity: one row per event of this batch ----------
    DELETE FROM project_dw.fact_response_activity WHERE batch_id = p_batch;

    INSERT INTO project_dw.fact_response_activity (
        incident_number, incident_key, event_sequence_no, sys_mod_count, activity_at,
        activity_date_key, activity_time_key, status_key, team_key, severity_key, location_key, category_key,
        response_activity_count, minutes_since_previous_activity, batch_id)
    SELECT e.incident_number, fi.incident_key, e.seq_no, e.sys_mod_count, e.sys_updated_at,
           project_dw.date_key(e.sys_updated_at), project_dw.time_key(e.sys_updated_at),
           COALESCE(st.status_key, -1), COALESCE(tm.team_key, -1), COALESCE(ds.severity_key, -1),
           COALESCE(dl.location_key, -1), COALESCE(dc.category_key, -1),
           1, project_dw.minutes_between(e.prev_at, e.sys_updated_at), e.batch_id
    FROM (
        -- sequence and gap are computed over the incident's whole history (all batches)
        SELECT ev.*,
               ROW_NUMBER() OVER w AS seq_no,
               LAG(ev.sys_updated_at) OVER w AS prev_at
        FROM project_staging.stg_incident_event ev
        JOIN tmp_touched t USING (incident_number)
        WINDOW w AS (PARTITION BY ev.incident_number ORDER BY ev.sys_updated_at, ev.sys_mod_count)
    ) e
    JOIN project_dw.fact_incident fi USING (incident_number)
    LEFT JOIN project_dw.dim_status st        ON st.status_name = e.incident_state
    LEFT JOIN project_dw.dim_response_team tm ON tm.team_code = e.assignment_group
    LEFT JOIN project_dw.dim_severity ds      ON ds.priority_level = e.priority
                                             AND ds.impact_level = e.impact AND ds.urgency_level = e.urgency
    LEFT JOIN project_dw.dim_location dl      ON dl.location_code = e.location_code
    LEFT JOIN project_dw.dim_category dc      ON dc.category_code = e.category_code
                                             AND dc.subcategory_code = COALESCE(e.subcategory_code, 'Unknown')
    WHERE e.batch_id = p_batch;
END $$;
