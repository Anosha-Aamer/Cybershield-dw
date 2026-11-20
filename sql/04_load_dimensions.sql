-- =====================================================================
-- 04  Dimension load (SCD Type 1 upsert), run once per batch
--   CALL project_dw.load_dimensions('2016-03');
-- New business keys get a new surrogate key from the IDENTITY column;
-- existing ones keep their key and only descriptive attributes are overwritten.
-- =====================================================================
CREATE OR REPLACE PROCEDURE project_dw.load_dimensions(p_batch VARCHAR)
LANGUAGE plpgsql AS $$
BEGIN
    -- Location: the full reference list (149 sites), names may be corrected over time
    INSERT INTO project_dw.dim_location (location_code, site_name, city, province, region, country)
    SELECT location_code, site_name, city, province, region, country
    FROM project_staging.ref_location_map
    ON CONFLICT (location_code) DO UPDATE SET
        site_name = EXCLUDED.site_name, city = EXCLUDED.city, province = EXCLUDED.province,
        region = EXCLUDED.region, country = EXCLUDED.country;

    -- Category: category + subcategory pairs seen in this batch, named through the reference map
    INSERT INTO project_dw.dim_category (category_code, subcategory_code, attack_family, category_name, subcategory_name)
    SELECT DISTINCT e.category_code, COALESCE(e.subcategory_code, 'Unknown'), r.attack_family, r.category_name,
           r.category_name || ' - ' || COALESCE(e.subcategory_code, 'Unknown')
    FROM project_staging.stg_incident_event e
    JOIN project_staging.ref_category_map r ON r.category_code = e.category_code
    WHERE e.batch_id = p_batch
    ON CONFLICT (category_code, subcategory_code) DO UPDATE SET
        attack_family = EXCLUDED.attack_family, category_name = EXCLUDED.category_name,
        subcategory_name = EXCLUDED.subcategory_name;

    -- Severity: priority x impact x urgency combinations
    INSERT INTO project_dw.dim_severity (priority_level, impact_level, urgency_level, priority_number, is_high_severity)
    SELECT DISTINCT priority, impact, urgency, LEFT(priority, 1)::SMALLINT, LEFT(priority, 1) IN ('1', '2')
    FROM project_staging.stg_incident_event
    WHERE batch_id = p_batch AND priority IS NOT NULL AND impact IS NOT NULL AND urgency IS NOT NULL
    ON CONFLICT (priority_level, impact_level, urgency_level) DO NOTHING;

    -- Response team
    INSERT INTO project_dw.dim_response_team (team_code, team_name)
    SELECT DISTINCT assignment_group, 'Response Team ' || SPLIT_PART(assignment_group, ' ', 2)
    FROM project_staging.stg_incident_event
    WHERE batch_id = p_batch AND assignment_group IS NOT NULL
    ON CONFLICT (team_code) DO UPDATE SET team_name = EXCLUDED.team_name;

    -- Contact type (reporting channel)
    INSERT INTO project_dw.dim_contact_type (contact_type_name)
    SELECT DISTINCT contact_type FROM project_staging.stg_incident_event
    WHERE batch_id = p_batch AND contact_type IS NOT NULL
    ON CONFLICT (contact_type_name) DO NOTHING;

    -- Status: fixed list, but add any unexpected state so facts never lose rows
    INSERT INTO project_dw.dim_status (status_name, status_group, is_open_flag)
    SELECT DISTINCT incident_state, 'Other', TRUE FROM project_staging.stg_incident_event
    WHERE batch_id = p_batch
    ON CONFLICT (status_name) DO NOTHING;
END $$;
