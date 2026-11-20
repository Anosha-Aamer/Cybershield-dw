-- =====================================================================
-- 06  Reporting layer (schema project_reporting): KPI views for dashboards
-- =====================================================================

-- KPI summary card
CREATE OR REPLACE VIEW project_reporting.vw_kpi_summary AS
SELECT COUNT(*)                                                         AS total_incidents,
       ROUND(AVG(response_duration_minutes), 1)                         AS avg_response_minutes,
       ROUND(AVG(resolution_duration_minutes) / 60.0, 1)                AS avg_resolution_hours,
       ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY resolution_duration_minutes) / 60.0)::NUMERIC, 1)
                                                                        AS median_resolution_hours,
       ROUND(100.0 * SUM(sla_compliance_count) / COUNT(*), 1)           AS sla_compliance_pct,
       SUM(CASE WHEN sv.is_high_severity THEN 1 ELSE 0 END)             AS high_severity_incidents,
       SUM(open_count)                                                  AS open_backlog,
       ROUND(100.0 * SUM(resolved_count) / COUNT(*), 1)                 AS resolution_rate_pct,
       ROUND(100.0 * SUM(CASE WHEN reopen_count > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS reopen_rate_pct
FROM project_dw.fact_incident_lifecycle f
JOIN project_dw.dim_severity sv USING (severity_key);

-- Q1, Q8: volume per month / week and change vs previous period
CREATE OR REPLACE VIEW project_reporting.vw_incidents_by_month AS
SELECT d.year_month, COUNT(*) AS incidents,
       COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY d.year_month) AS change_vs_prev_month
FROM project_dw.fact_incident f JOIN project_dw.dim_date d ON d.date_key = f.reported_date_key
GROUP BY d.year_month;

CREATE OR REPLACE VIEW project_reporting.vw_incidents_by_week AS
SELECT d.year_week, MIN(d.full_date) AS week_start, COUNT(*) AS incidents
FROM project_dw.fact_incident f JOIN project_dw.dim_date d ON d.date_key = f.reported_date_key
GROUP BY d.year_week;

-- Q2, Q5: volume and resolution time by category
CREATE OR REPLACE VIEW project_reporting.vw_category_performance AS
SELECT c.attack_family, c.category_name, COUNT(*) AS incidents,
       ROUND(AVG(f.resolution_duration_minutes) / 60.0, 1) AS avg_resolution_hours,
       ROUND(100.0 * SUM(f.sla_compliance_count) / COUNT(*), 1) AS sla_compliance_pct
FROM project_dw.fact_incident_lifecycle f JOIN project_dw.dim_category c USING (category_key)
GROUP BY c.attack_family, c.category_name;

-- Q3, Q12: location volume and high-severity incidents
CREATE OR REPLACE VIEW project_reporting.vw_location_incidents AS
SELECT l.region, l.province, l.city, l.site_name, COUNT(*) AS incidents,
       SUM(CASE WHEN s.is_high_severity THEN 1 ELSE 0 END) AS high_severity_incidents
FROM project_dw.fact_incident f
JOIN project_dw.dim_location l USING (location_key)
JOIN project_dw.dim_severity s USING (severity_key)
GROUP BY l.region, l.province, l.city, l.site_name;

-- Q6: incidents by severity
CREATE OR REPLACE VIEW project_reporting.vw_severity_distribution AS
SELECT s.priority_level, s.is_high_severity, COUNT(*) AS incidents
FROM project_dw.fact_incident f JOIN project_dw.dim_severity s USING (severity_key)
GROUP BY s.priority_level, s.is_high_severity;

-- Q9: team workload (incidents touched and activities performed)
CREATE OR REPLACE VIEW project_reporting.vw_team_workload AS
SELECT t.team_name, COUNT(DISTINCT a.incident_number) AS incidents_handled, COUNT(*) AS activities
FROM project_dw.fact_response_activity a JOIN project_dw.dim_response_team t USING (team_key)
GROUP BY t.team_name;

-- Q4, Q10, Q11, Q14: response / resolution / SLA by month of report
CREATE OR REPLACE VIEW project_reporting.vw_response_sla_by_month AS
SELECT d.year_month, COUNT(*) AS incidents,
       ROUND(AVG(f.response_duration_minutes), 1)            AS avg_response_minutes,
       ROUND(AVG(f.resolution_duration_minutes) / 60.0, 1)   AS avg_resolution_hours,
       ROUND(AVG(f.response_to_resolution_minutes) / 60.0, 1) AS avg_response_to_resolution_hours,
       SUM(f.sla_compliance_count)                           AS met_sla,
       ROUND(100.0 * SUM(f.sla_compliance_count) / COUNT(*), 1) AS sla_compliance_pct
FROM project_dw.fact_incident_lifecycle f JOIN project_dw.dim_date d ON d.date_key = f.reported_date_key
GROUP BY d.year_month;

-- Q13: reports by weekday
CREATE OR REPLACE VIEW project_reporting.vw_incidents_by_weekday AS
SELECT d.day_of_week_number, d.day_name, COUNT(*) AS incidents
FROM project_dw.fact_incident f JOIN project_dw.dim_date d ON d.date_key = f.reported_date_key
GROUP BY d.day_of_week_number, d.day_name;

-- Q7, Q15: backlog as of each day (opened by that day and not yet resolved)
CREATE OR REPLACE VIEW project_reporting.vw_backlog_by_day AS
SELECT d.full_date,
       (SELECT COUNT(*) FROM project_dw.fact_incident_lifecycle f
         WHERE f.reported_at < d.full_date + 1
           AND (f.resolved_at IS NULL OR f.resolved_at >= d.full_date + 1)) AS open_backlog,
       (SELECT COUNT(*) FROM project_dw.fact_incident_lifecycle f
         WHERE f.resolved_date_key = d.date_key)                            AS resolved_that_day,
       (SELECT COUNT(*) FROM project_dw.fact_incident_lifecycle f
         WHERE f.reported_date_key = d.date_key)                            AS reported_that_day
FROM project_dw.dim_date d
WHERE d.full_date BETWEEN (SELECT MIN(reported_at)::DATE FROM project_dw.fact_incident_lifecycle)
                      AND (SELECT MAX(COALESCE(closed_at, resolved_at, reported_at))::DATE FROM project_dw.fact_incident_lifecycle);

-- Audit view: per-batch load summary of the latest run of each batch
CREATE OR REPLACE VIEW project_reporting.vw_etl_batch_summary AS
SELECT DISTINCT ON (batch_id) batch_id, run_id, status, rows_api_reported, rows_extracted, rows_bronze,
       rows_duplicate, rows_rejected, rows_silver, rows_fact_activity, incidents_touched,
       finished_at - started_at AS duration
FROM project_audit.etl_batch_log
ORDER BY batch_id, batch_log_id DESC;
