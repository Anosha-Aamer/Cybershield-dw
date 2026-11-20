-- =====================================================================
-- 07  Data-quality validation
--   CALL project_audit.run_dq_checks(<run_id>, '2016-03');   -- batch + warehouse checks
-- Results go to project_audit.dq_check_result (one row per check).
-- Categories: Completeness, Accuracy (reconciliation), Integrity (FK), Uniqueness, Validity
-- =====================================================================

CREATE OR REPLACE PROCEDURE project_audit.add_check(
    p_run INT, p_batch VARCHAR, p_cat VARCHAR, p_name VARCHAR, p_table VARCHAR,
    p_expected NUMERIC, p_actual NUMERIC, p_passed BOOLEAN, p_details TEXT DEFAULT NULL)
LANGUAGE sql AS $$
    INSERT INTO project_audit.dq_check_result
        (run_id, batch_id, check_category, check_name, table_name, expected_value, actual_value, passed, details)
    VALUES (p_run, p_batch, p_cat, p_name, p_table, p_expected, p_actual, p_passed, p_details);
$$;

CREATE OR REPLACE PROCEDURE project_audit.run_dq_checks(p_run INT, p_batch VARCHAR)
LANGUAGE plpgsql AS $$
DECLARE
    b        project_audit.etl_batch_log%ROWTYPE;
    n_raw    BIGINT;  n_stg  BIGINT;  n_fact BIGINT;  v NUMERIC;  tot NUMERIC;
    r        RECORD;
BEGIN
    SELECT * INTO b FROM project_audit.etl_batch_log
    WHERE run_id = p_run AND batch_id = p_batch ORDER BY batch_log_id DESC LIMIT 1;

    SELECT COUNT(*) INTO n_raw  FROM project_staging.raw_incident_event   WHERE batch_id = p_batch;
    SELECT COUNT(*) INTO n_stg  FROM project_staging.stg_incident_event   WHERE batch_id = p_batch;
    SELECT COUNT(*) INTO n_fact FROM project_dw.fact_response_activity    WHERE batch_id = p_batch;

    -- ================= ACCURACY: row reconciliation through every layer =================
    CALL project_audit.add_check(p_run, p_batch, 'Accuracy', 'API rows received = rows API reported', 'API',
        b.rows_api_reported, b.rows_extracted, b.rows_api_reported = b.rows_extracted);
    CALL project_audit.add_check(p_run, p_batch, 'Accuracy', 'Bronze rows = API rows received', 'raw_incident_event',
        b.rows_extracted, n_raw, b.rows_extracted = n_raw);
    CALL project_audit.add_check(p_run, p_batch, 'Accuracy', 'Bronze = Silver + duplicates + rejects', 'stg_incident_event',
        n_raw, n_stg + b.rows_duplicate + b.rows_rejected, n_raw = n_stg + b.rows_duplicate + b.rows_rejected,
        format('silver %s + duplicates %s + rejected %s', n_stg, b.rows_duplicate, b.rows_rejected));
    CALL project_audit.add_check(p_run, p_batch, 'Accuracy', 'Silver events = FactResponseActivity rows', 'fact_response_activity',
        n_stg, n_fact, n_stg = n_fact);
    SELECT COUNT(*) INTO v FROM project_audit.etl_reject_log WHERE batch_id = p_batch AND run_id = p_run;
    CALL project_audit.add_check(p_run, p_batch, 'Accuracy', 'Rejected rows logged = rejects counted', 'etl_reject_log',
        b.rows_rejected, v, b.rows_rejected = v);

    -- ================= COMPLETENESS: missing values in the clean batch =================
    FOR r IN SELECT * FROM (VALUES
            ('incident_number', 0.0), ('opened_at', 0.0), ('sys_updated_at', 0.0), ('incident_state', 0.0),
            ('contact_type', 0.0), ('location_code', 2.0), ('category_code', 2.0), ('priority', 1.0),
            ('caller_id', 1.0), ('assignment_group', 25.0)) AS t(col, max_pct)
    LOOP
        EXECUTE format('SELECT COUNT(*) FILTER (WHERE %I IS NULL) FROM project_staging.stg_incident_event WHERE batch_id = $1', r.col)
            INTO v USING p_batch;
        CALL project_audit.add_check(p_run, p_batch, 'Completeness', format('%% missing %s (max %s%%)', r.col, r.max_pct),
            'stg_incident_event', r.max_pct, ROUND(100.0 * v / NULLIF(n_stg, 0), 2),
            100.0 * v / NULLIF(n_stg, 0) <= r.max_pct, format('%s of %s rows missing', v, n_stg));
    END LOOP;

    -- ================= VALIDITY: values that are impossible =================
    SELECT COUNT(*) INTO v FROM project_staging.stg_incident_event
    WHERE batch_id = p_batch AND (reassignment_count < 0 OR reopen_count < 0 OR sys_mod_count < 0);
    CALL project_audit.add_check(p_run, p_batch, 'Validity', 'Negative counts in silver = 0', 'stg_incident_event', 0, v, v = 0);

    SELECT COUNT(*) INTO v FROM project_staging.stg_incident_event
    WHERE batch_id = p_batch AND TO_CHAR(sys_updated_at, 'YYYY-MM') <> p_batch;
    CALL project_audit.add_check(p_run, p_batch, 'Validity', 'Event time inside batch month', 'stg_incident_event', 0, v, v = 0);

    SELECT COUNT(*) INTO v FROM project_staging.stg_incident_event
    WHERE batch_id = p_batch AND resolved_at < opened_at;
    CALL project_audit.add_check(p_run, p_batch, 'Validity', 'resolved_at before opened_at = 0', 'stg_incident_event', 0, v, v = 0);

    -- ================= WAREHOUSE-LEVEL CHECKS =================
    -- Integrity: every fact foreign key must find its dimension row ("fact rows without matching dimension = 0")
    FOR r IN SELECT * FROM (VALUES
            ('fact_incident', 'reported_date_key', 'dim_date', 'date_key'),
            ('fact_incident', 'reported_time_key', 'dim_time', 'time_key'),
            ('fact_incident', 'location_key', 'dim_location', 'location_key'),
            ('fact_incident', 'category_key', 'dim_category', 'category_key'),
            ('fact_incident', 'severity_key', 'dim_severity', 'severity_key'),
            ('fact_incident', 'contact_type_key', 'dim_contact_type', 'contact_type_key'),
            ('fact_incident_lifecycle', 'reported_date_key', 'dim_date', 'date_key'),
            ('fact_incident_lifecycle', 'first_response_date_key', 'dim_date', 'date_key'),
            ('fact_incident_lifecycle', 'resolved_date_key', 'dim_date', 'date_key'),
            ('fact_incident_lifecycle', 'closed_date_key', 'dim_date', 'date_key'),
            ('fact_incident_lifecycle', 'first_response_time_key', 'dim_time', 'time_key'),
            ('fact_incident_lifecycle', 'location_key', 'dim_location', 'location_key'),
            ('fact_incident_lifecycle', 'category_key', 'dim_category', 'category_key'),
            ('fact_incident_lifecycle', 'severity_key', 'dim_severity', 'severity_key'),
            ('fact_incident_lifecycle', 'current_status_key', 'dim_status', 'status_key'),
            ('fact_incident_lifecycle', 'current_team_key', 'dim_response_team', 'team_key'),
            ('fact_response_activity', 'activity_date_key', 'dim_date', 'date_key'),
            ('fact_response_activity', 'status_key', 'dim_status', 'status_key'),
            ('fact_response_activity', 'team_key', 'dim_response_team', 'team_key'),
            ('fact_response_activity', 'location_key', 'dim_location', 'location_key'),
            ('fact_response_activity', 'category_key', 'dim_category', 'category_key'),
            ('fact_response_activity', 'severity_key', 'dim_severity', 'severity_key')) AS t(fact, fk, dim, pk)
    LOOP
        EXECUTE format('SELECT COUNT(*) FROM project_dw.%I f LEFT JOIN project_dw.%I d ON d.%I = f.%I WHERE d.%I IS NULL',
                       r.fact, r.dim, r.pk, r.fk, r.pk) INTO v;
        CALL project_audit.add_check(p_run, NULL, 'Integrity', format('%s.%s rows without %s = 0', r.fact, r.fk, r.dim),
            r.fact, 0, v, v = 0);
    END LOOP;

    -- Completeness in the warehouse: share of facts pointing to an Unknown member
    FOR r IN SELECT * FROM (VALUES ('location_key', 2.0), ('category_key', 2.0), ('severity_key', 1.0)) AS t(fk, max_pct)
    LOOP
        EXECUTE format('SELECT COUNT(*) FILTER (WHERE %I = -1), COUNT(*) FROM project_dw.fact_incident', r.fk) INTO v, tot;
        CALL project_audit.add_check(p_run, NULL, 'Completeness', format('%% fact_incident with Unknown %s (max %s%%)', r.fk, r.max_pct),
            'fact_incident', r.max_pct, ROUND(100.0 * v / NULLIF(tot, 0), 2), 100.0 * v / NULLIF(tot, 0) <= r.max_pct);
    END LOOP;

    -- Accuracy: totals agree across silver and the three facts
    SELECT COUNT(*) INTO v   FROM project_dw.fact_incident;
    SELECT COUNT(*) INTO tot FROM project_staging.stg_incident;
    CALL project_audit.add_check(p_run, NULL, 'Accuracy', 'Silver incidents = FactIncident rows', 'fact_incident', tot, v, tot = v);
    SELECT COUNT(*) INTO tot FROM project_dw.fact_incident_lifecycle;
    CALL project_audit.add_check(p_run, NULL, 'Accuracy', 'FactIncident rows = FactIncidentLifecycle rows', 'fact_incident_lifecycle', v, tot, v = tot);
    SELECT COALESCE(SUM(event_count), 0) INTO v FROM project_dw.fact_incident_lifecycle;
    SELECT COUNT(*) INTO tot FROM project_dw.fact_response_activity;
    CALL project_audit.add_check(p_run, NULL, 'Accuracy', 'SUM(lifecycle.event_count) = FactResponseActivity rows', 'fact_response_activity', v, tot, v = tot);
    SELECT COUNT(*) INTO v FROM project_staging.stg_incident_event;
    CALL project_audit.add_check(p_run, NULL, 'Accuracy', 'Silver events (all batches) = FactResponseActivity rows', 'fact_response_activity', v, tot, v = tot);

    -- Uniqueness of business keys in the facts
    SELECT COUNT(*) - COUNT(DISTINCT incident_number) INTO v FROM project_dw.fact_incident;
    CALL project_audit.add_check(p_run, NULL, 'Uniqueness', 'Duplicate incident_number in FactIncident = 0', 'fact_incident', 0, v, v = 0);
    SELECT COUNT(*) INTO v FROM (SELECT 1 FROM project_dw.fact_response_activity
                                 GROUP BY incident_number, event_sequence_no HAVING COUNT(*) > 1) x;
    CALL project_audit.add_check(p_run, NULL, 'Uniqueness', 'Duplicate (incident, sequence) in FactResponseActivity = 0', 'fact_response_activity', 0, v, v = 0);

    -- Validity of lifecycle logic
    SELECT COUNT(*) INTO v FROM project_dw.fact_incident_lifecycle
    WHERE response_duration_minutes < 0 OR resolution_duration_minutes < 0 OR closure_duration_minutes < 0;
    CALL project_audit.add_check(p_run, NULL, 'Validity', 'Negative durations = 0', 'fact_incident_lifecycle', 0, v, v = 0);
    SELECT COUNT(*) INTO v FROM project_dw.fact_incident_lifecycle WHERE open_count = 1 AND resolved_date_key <> -1;
    CALL project_audit.add_check(p_run, NULL, 'Validity', 'Open incidents with a resolved date = 0', 'fact_incident_lifecycle', 0, v, v = 0);
    SELECT COUNT(*) INTO v FROM project_dw.fact_incident_lifecycle WHERE resolved_count + open_count <> 1;
    CALL project_audit.add_check(p_run, NULL, 'Validity', 'Each incident is either resolved or open', 'fact_incident_lifecycle', 0, v, v = 0);
END $$;
