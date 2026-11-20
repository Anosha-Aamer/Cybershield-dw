-- =====================================================================
-- 02b  Silver roll-up: event rows -> one row per incident (stg_incident)
-- Called once per batch, only for incidents that received events in that batch.
-- Business rules (from Phase 1 + review):
--   * Reported attributes = values on the FIRST event (what was reported)
--   * Final attributes    = values on the LAST event  (current state)
--   * made_sla            = flag on the LAST event (the flag turns FALSE when SLA is breached)
--   * First response      = first event whose state is no longer 'New'
--   * Resolved            = only when final state is Resolved/Closed:
--                           resolved_at of last event, else time of first Resolved event
--   * Closed              = only when final state is Closed: closed_at of last event,
--                           else time of the last event
-- =====================================================================
CREATE OR REPLACE PROCEDURE project_staging.refresh_stg_incident(p_batch VARCHAR)
LANGUAGE plpgsql AS $$
BEGIN
    WITH touched AS (
        SELECT DISTINCT incident_number FROM project_staging.stg_incident_event WHERE batch_id = p_batch
    ),
    ev AS (
        SELECT e.*,
               ROW_NUMBER() OVER (PARTITION BY e.incident_number ORDER BY e.sys_updated_at, e.sys_mod_count)          AS rn_first,
               ROW_NUMBER() OVER (PARTITION BY e.incident_number ORDER BY e.sys_updated_at DESC, e.sys_mod_count DESC) AS rn_last
        FROM project_staging.stg_incident_event e
        JOIN touched t USING (incident_number)
    ),
    agg AS (
        SELECT incident_number,
               MIN(opened_at)                                                  AS opened_at,
               COUNT(*)                                                        AS event_count,
               MIN(sys_updated_at) FILTER (WHERE incident_state <> 'New')      AS first_response_at,
               MIN(sys_updated_at) FILTER (WHERE incident_state IN ('Resolved','Closed')) AS first_resolved_event_at,
               MAX(sys_updated_at)                                             AS last_event_at,
               MAX(batch_id)                                                   AS last_batch_id
        FROM ev GROUP BY incident_number
    ),
    f AS (SELECT * FROM ev WHERE rn_first = 1),
    l AS (SELECT * FROM ev WHERE rn_last  = 1)
    INSERT INTO project_staging.stg_incident AS s (
        incident_number, caller_id, opened_at,
        reported_location_code, reported_category_code, reported_subcategory,
        reported_impact, reported_urgency, reported_priority, contact_type,
        final_location_code, final_category_code, final_subcategory,
        final_impact, final_urgency, final_priority,
        final_state, final_assignment_group, final_made_sla,
        reassignment_count, reopen_count, event_count,
        first_response_at, resolved_at, closed_at, last_event_at, last_batch_id)
    SELECT a.incident_number,
           COALESCE(l.caller_id, f.caller_id),
           a.opened_at,
           f.location_code, f.category_code, f.subcategory_code,
           f.impact, f.urgency, f.priority, f.contact_type,
           l.location_code, l.category_code, l.subcategory_code,
           l.impact, l.urgency, l.priority,
           l.incident_state, l.assignment_group, l.made_sla,
           l.reassignment_count, l.reopen_count, a.event_count,
           -- response can't be before the incident was opened
           CASE WHEN a.first_response_at >= a.opened_at THEN a.first_response_at END,
           CASE WHEN l.incident_state IN ('Resolved','Closed')
                THEN CASE WHEN l.resolved_at >= a.opened_at AND l.resolved_at <= a.last_event_at
                          THEN l.resolved_at ELSE a.first_resolved_event_at END END,
           CASE WHEN l.incident_state = 'Closed'
                THEN CASE WHEN l.closed_at >= a.opened_at AND l.closed_at <= a.last_event_at
                          THEN l.closed_at ELSE a.last_event_at END END,
           a.last_event_at, a.last_batch_id
    FROM agg a JOIN f USING (incident_number) JOIN l USING (incident_number)
    ON CONFLICT (incident_number) DO UPDATE SET
        caller_id = EXCLUDED.caller_id, opened_at = EXCLUDED.opened_at,
        reported_location_code = EXCLUDED.reported_location_code, reported_category_code = EXCLUDED.reported_category_code,
        reported_subcategory = EXCLUDED.reported_subcategory, reported_impact = EXCLUDED.reported_impact,
        reported_urgency = EXCLUDED.reported_urgency, reported_priority = EXCLUDED.reported_priority,
        contact_type = EXCLUDED.contact_type,
        final_location_code = EXCLUDED.final_location_code, final_category_code = EXCLUDED.final_category_code,
        final_subcategory = EXCLUDED.final_subcategory, final_impact = EXCLUDED.final_impact,
        final_urgency = EXCLUDED.final_urgency, final_priority = EXCLUDED.final_priority,
        final_state = EXCLUDED.final_state, final_assignment_group = EXCLUDED.final_assignment_group,
        final_made_sla = EXCLUDED.final_made_sla, reassignment_count = EXCLUDED.reassignment_count,
        reopen_count = EXCLUDED.reopen_count, event_count = EXCLUDED.event_count,
        first_response_at = EXCLUDED.first_response_at, resolved_at = EXCLUDED.resolved_at,
        closed_at = EXCLUDED.closed_at, last_event_at = EXCLUDED.last_event_at, last_batch_id = EXCLUDED.last_batch_id;
END $$;
