"""Small helpers that write to the project_audit tables."""
from contextlib import contextmanager


def start_run(conn, args: str) -> int:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO project_audit.etl_run (run_args) VALUES (%s) RETURNING run_id", (args,))
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def finish_run(conn, run_id: int, status: str) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE project_audit.etl_run SET finished_at = now(), status = %s WHERE run_id = %s", (status, run_id))
    conn.commit()


def start_batch(conn, run_id: int, batch: str) -> int:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO project_audit.etl_batch_log (run_id, batch_id) VALUES (%s, %s) RETURNING batch_log_id",
                    (run_id, batch))
        log_id = cur.fetchone()[0]
    conn.commit()
    return log_id


def update_batch(conn, log_id: int, **cols) -> None:
    sets = ", ".join(f"{k} = %s" for k in cols)
    with conn.cursor() as cur:
        cur.execute(f"UPDATE project_audit.etl_batch_log SET {sets} WHERE batch_log_id = %s", (*cols.values(), log_id))
    conn.commit()


@contextmanager
def step(conn, run_id: int, batch: str | None, name: str):
    """Log one ETL step. Usage: with step(conn, run, batch, 'silver') as s: ...; s['rows_in'] = n"""
    with conn.cursor() as cur:
        cur.execute("INSERT INTO project_audit.etl_step_log (run_id, batch_id, step_name) VALUES (%s, %s, %s) "
                    "RETURNING step_log_id", (run_id, batch, name))
        step_id = cur.fetchone()[0]
    conn.commit()
    info: dict = {}
    try:
        yield info
    except Exception as exc:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("UPDATE project_audit.etl_step_log SET finished_at = now(), status = 'FAILED', message = %s "
                        "WHERE step_log_id = %s", (str(exc)[:2000], step_id))
        conn.commit()
        raise
    with conn.cursor() as cur:
        cur.execute("UPDATE project_audit.etl_step_log SET finished_at = now(), status = 'SUCCESS', rows_in = %s, "
                    "rows_out = %s, message = %s WHERE step_log_id = %s",
                    (info.get("rows_in"), info.get("rows_out"), info.get("message"), step_id))
    conn.commit()
