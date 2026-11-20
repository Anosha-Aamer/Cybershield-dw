"""CyberShield ETL orchestrator.

    python -m etl.run_pipeline --init            create database, schemas, tables, procedures, load reference maps
    python -m etl.run_pipeline --all             load every batch after the watermark (incremental)
    python -m etl.run_pipeline --all --full      reload every batch the API offers
    python -m etl.run_pipeline --batch 2016-05   (re)load one batch

Per batch:  API -> BRONZE -> SILVER (+ silver CSV) -> stg_incident -> dimensions -> facts -> DQ checks
Every run / batch / step is written to the project_audit tables.
The source API must be running:  uvicorn api.app:app --port 8000
"""
import argparse
import sys
import time

import pandas as pd
from psycopg2 import sql as psql

from etl import audit
from etl.config import PG, PIPELINE_NAME, REFERENCE_DIR, SQL_DIR, connect
from etl.extract_api import fetch_batch, list_batches, load_bronze
from etl.transform_silver import save_silver, transform

SQL_FILES = ["00_create_db_schemas.sql", "01_audit_tables.sql", "02_staging_tables.sql",
             "02b_silver_incident_rollup.sql", "03_dw_tables.sql", "04_load_dimensions.sql",
             "05_load_facts.sql", "06_reporting_views.sql", "07_data_quality_checks.sql"]


def init_database() -> None:
    admin = connect("postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (PG["dbname"],))
        if not cur.fetchone():
            cur.execute(psql.SQL("CREATE DATABASE {}").format(psql.Identifier(PG["dbname"])))
            print(f"created database {PG['dbname']}")
    admin.close()

    conn = connect()
    with conn.cursor() as cur:
        for f in SQL_FILES:
            cur.execute((SQL_DIR / f).read_text(encoding="utf-8"))
            print(f"ran {f}")
        load_reference(cur)
    conn.commit()
    conn.close()


def load_reference(cur) -> None:
    loc = pd.read_csv(REFERENCE_DIR / "location_map.csv")
    cat = pd.read_csv(REFERENCE_DIR / "category_map.csv")
    cur.execute("TRUNCATE project_staging.ref_location_map, project_staging.ref_category_map")
    cur.executemany("INSERT INTO project_staging.ref_location_map VALUES (%s, %s, %s, %s, %s, %s)",
                    loc.itertuples(index=False, name=None))
    cur.executemany("INSERT INTO project_staging.ref_category_map VALUES (%s, %s, %s)",
                    cat.itertuples(index=False, name=None))
    print(f"loaded reference maps: {len(loc)} locations, {len(cat)} categories")


def read_df(conn, query: str, params=()) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(query, params)
        return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])


def scalar(conn, query: str, params=()):
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()[0]


def run_batch(conn, run_id: int, batch: str) -> None:
    t0 = time.time()
    log_id = audit.start_batch(conn, run_id, batch)
    try:
        with audit.step(conn, run_id, batch, "extract_api") as s:
            rows, total = fetch_batch(batch)
            s.update(rows_in=total, rows_out=len(rows))
        audit.update_batch(conn, log_id, rows_api_reported=total, rows_extracted=len(rows))

        with audit.step(conn, run_id, batch, "load_bronze") as s:
            n_bronze = load_bronze(conn, batch, rows)
            s.update(rows_in=len(rows), rows_out=n_bronze)
        audit.update_batch(conn, log_id, rows_bronze=n_bronze)

        with audit.step(conn, run_id, batch, "transform_silver") as s:
            raw = read_df(conn, "SELECT * FROM project_staging.raw_incident_event WHERE batch_id = %s", (batch,))
            raw = raw.astype({c: "string" for c in raw.columns if c not in ("raw_row_id", "api_page", "load_ts")})
            raw["raw_row_id"] = raw["raw_row_id"].astype(int)
            locs = set(read_df(conn, "SELECT location_code FROM project_staging.ref_location_map")["location_code"])
            cats = set(read_df(conn, "SELECT category_code FROM project_staging.ref_category_map")["category_code"])
            silver, rejects, stats = transform(raw, batch, locs, cats)
            save_silver(conn, run_id, batch, silver, rejects)
            s.update(rows_in=stats["rows_in"], rows_out=stats["rows_silver"],
                     message=f"rules={stats['rule_counts']} rejects={stats['reject_reasons']}")
        audit.update_batch(conn, log_id, rows_duplicate=stats["rows_duplicate"], rows_rejected=stats["rows_rejected"],
                           rows_silver=stats["rows_silver"])

        with audit.step(conn, run_id, batch, "silver_incident_rollup") as s:
            with conn.cursor() as cur:
                cur.execute("CALL project_staging.refresh_stg_incident(%s)", (batch,))
                cur.execute("ANALYZE project_staging.stg_incident")
            conn.commit()

        with audit.step(conn, run_id, batch, "load_dimensions"):
            with conn.cursor() as cur:
                cur.execute("CALL project_dw.load_dimensions(%s)", (batch,))
            conn.commit()

        with audit.step(conn, run_id, batch, "load_facts") as s:
            with conn.cursor() as cur:
                cur.execute("CALL project_dw.load_facts(%s)", (batch,))
                for t in ("fact_incident", "fact_incident_lifecycle", "fact_response_activity"):
                    cur.execute(f"ANALYZE project_dw.{t}")
            conn.commit()
            n_fact = scalar(conn, "SELECT COUNT(*) FROM project_dw.fact_response_activity WHERE batch_id = %s", (batch,))
            n_inc = scalar(conn, "SELECT COUNT(DISTINCT incident_number) FROM project_staging.stg_incident_event "
                                 "WHERE batch_id = %s", (batch,))
            s.update(rows_in=stats["rows_silver"], rows_out=n_fact)
        audit.update_batch(conn, log_id, rows_fact_activity=n_fact, incidents_touched=n_inc)

        with audit.step(conn, run_id, batch, "data_quality") as s:
            with conn.cursor() as cur:
                cur.execute("CALL project_audit.run_dq_checks(%s, %s)", (run_id, batch))
            conn.commit()
            failed = scalar(conn, "SELECT COUNT(*) FROM project_audit.dq_check_result "
                                  "WHERE run_id = %s AND (batch_id = %s OR batch_id IS NULL) AND NOT passed "
                                  "AND checked_at >= (SELECT started_at FROM project_audit.etl_batch_log WHERE batch_log_id = %s)",
                            (run_id, batch, log_id))
            s.update(message=f"{failed} failed checks")

        audit.update_batch(conn, log_id, status="SUCCESS" if failed == 0 else "SUCCESS_WITH_DQ_WARNINGS",
                           finished_at=pd.Timestamp.now().to_pydatetime())
        with conn.cursor() as cur:
            cur.execute("UPDATE project_audit.etl_watermark SET last_batch_id = GREATEST(COALESCE(last_batch_id, ''), %s), "
                        "updated_at = now() WHERE pipeline_name = %s", (batch, PIPELINE_NAME))
        conn.commit()
        print(f"  {batch}: api {total:,} | bronze {n_bronze:,} | dup {stats['rows_duplicate']:,} | "
              f"rejected {stats['rows_rejected']:,} | silver {stats['rows_silver']:,} | fact {n_fact:,} | "
              f"DQ failed {failed} | {time.time() - t0:.0f}s")
    except Exception as exc:
        conn.rollback()
        audit.update_batch(conn, log_id, status="FAILED", error_message=str(exc)[:2000],
                           finished_at=pd.Timestamp.now().to_pydatetime())
        raise


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--init", action="store_true", help="create database objects and load reference maps")
    ap.add_argument("--all", action="store_true", help="load all batches after the watermark")
    ap.add_argument("--full", action="store_true", help="with --all: ignore the watermark")
    ap.add_argument("--batch", help="load a single batch, e.g. 2016-05")
    args = ap.parse_args()

    if args.init:
        init_database()
    if not (args.all or args.batch):
        return 0

    conn = connect()
    if args.batch:
        batches = [args.batch]
    else:
        available = [b["batch"] for b in list_batches()]
        last = None if args.full else scalar(conn, "SELECT last_batch_id FROM project_audit.etl_watermark "
                                                   "WHERE pipeline_name = %s", (PIPELINE_NAME,))
        batches = [b for b in available if last is None or b > last]
    if not batches:
        print("nothing to load - warehouse is up to date")
        return 0

    run_id = audit.start_run(conn, " ".join(sys.argv[1:]))
    print(f"run {run_id}: loading {len(batches)} batch(es)")
    try:
        for b in batches:
            run_batch(conn, run_id, b)
    except Exception as exc:
        audit.finish_run(conn, run_id, "FAILED")
        print(f"run {run_id} FAILED: {exc}")
        return 1
    audit.finish_run(conn, run_id, "SUCCESS")
    print(f"run {run_id} finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())
