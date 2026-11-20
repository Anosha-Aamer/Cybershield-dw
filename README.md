# CyberShield DW – Phase 2: Physical Data Warehouse & ETL Pipeline

Cybersecurity incident data warehouse on PostgreSQL. The data comes from a source **API**, arrives in **monthly batches**,
and moves through **bronze → silver → gold → reporting**, with a full **audit trail** and **data-quality checks**.

```
generator/ (extend 3 weeks -> 1 year, inject data issues)
      │
data/source/events_YYYY-MM.csv ──► FastAPI source system (api/app.py)
                                         │  HTTP, paginated, one batch per month
                                         ▼
project_staging.raw_incident_event      BRONZE  as received, all TEXT
                                         │  etl/transform_silver.py  (11 cleaning rules)
project_staging.stg_incident_event      SILVER  clean + typed  ──► data/silver/stg_incident_event_YYYY-MM.csv
project_staging.stg_incident            SILVER  one row per incident (lifecycle rules)
                                         │  sql/04 + sql/05  (business key -> surrogate key)
project_dw  (8 dims, 3 facts)           GOLD    star schema
project_reporting (views)               KPIs for dashboards
project_audit                           runs, batches, steps, rejected rows, DQ results, watermark
```

## Data
| | Rows |
|---|---|
| Original UCI log (3 weeks) | 43,479 events / 6,373 incidents |
| Extended year (Mar 2016 – Feb 2017), 12 batches | 525,249 events / 78,975 incidents |
| Injected issues (dirty source) | +7,879 duplicate rows, ~697k damaged cells (see `injected_issues_manifest.csv`) |
| Cleaned out by the ETL | 7,870 duplicates removed, 423 rows rejected to `etl_reject_log` |
| fact_response_activity / fact_incident / fact_incident_lifecycle | 524,835 / 78,975 / 78,975 |
| dim_location / dim_category / dim_date / dim_time | 150 / 647 / 1,097 / 1,441 |

Mappings: `Location n` → Pakistani office site / city / province / region (`data/reference/location_map.csv`),
`Category n` → cyber incident type / attack family (`data/reference/category_map.csv`).

## Run it
```bash
pip install -r requirements.txt
copy .env.example .env          # then put your PostgreSQL password in PGPASSWORD
python generator/build_reference_maps.py
python generator/extend_data.py        # -> data/generated (clean year)
python generator/inject_issues.py      # -> data/source (dirty) + injected_issues_manifest.csv
uvicorn api.app:app --port 8000        # keep running in its own terminal; docs at /docs
python -m etl.run_pipeline --init      # database cybershield_dw, schemas, tables, procedures
python -m etl.run_pipeline --all       # incremental: every batch after the watermark
python -m etl.run_pipeline --batch 2016-07   # reload one batch (idempotent)
```

## Files → deliverables
| Deliverable | File |
|---|---|
| Database + schemas | `sql/00_create_db_schemas.sql` |
| Audit tables | `sql/01_audit_tables.sql` |
| Staging schema SQL | `sql/02_staging_tables.sql`, `sql/02b_silver_incident_rollup.sql` |
| DW schema SQL | `sql/03_dw_tables.sql` |
| Dimension load SQL | `sql/04_load_dimensions.sql` (`CALL project_dw.load_dimensions('2016-03')`) |
| Fact load SQL | `sql/05_load_facts.sql` (`CALL project_dw.load_facts('2016-03')`) |
| Reporting views | `sql/06_reporting_views.sql` |
| Data quality checks | `sql/07_data_quality_checks.sql` → `project_audit.dq_check_result` |
| ETL scripts (Python) | `etl/extract_api.py`, `etl/transform_silver.py`, `etl/run_pipeline.py` |
| Source API | `api/app.py` |
| Data Cleaning Report PDF | `docs/Data_Cleaning_Report.pdf` |
| Data Quality Report PDF | `docs/Data_Quality_Report.pdf` (rebuild both with `python docs/build_reports.py`) |

## Key rules
* Surrogate keys: IDENTITY per dimension (location 1001+, category 2001+, …, incident 100001+); `-1` = Unknown.
* Reported attributes = first event; current attributes, status and `made_sla` = last event.
* First response = first event no longer `New`; resolved/closed only when the final state says so.
* Events after 28 Feb 2017 are not in the source yet, so 2,417 incidents are still open (real backlog).

## Useful queries
```sql
SELECT * FROM project_reporting.vw_etl_batch_summary ORDER BY batch_id;
SELECT check_category, check_name, expected_value, actual_value, passed
FROM project_audit.dq_check_result WHERE run_id = (SELECT MAX(run_id) FROM project_audit.etl_run);
SELECT reject_reason, COUNT(*) FROM project_audit.etl_reject_log GROUP BY 1;
SELECT * FROM project_reporting.vw_kpi_summary;
```
