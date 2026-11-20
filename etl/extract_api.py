"""EXTRACT: pull one monthly batch from the source API and land it in BRONZE as-is.

project_staging.raw_incident_event keeps every value as TEXT exactly as received,
plus batch_id / api_page / load_ts. Re-loading a batch first deletes its old rows.
"""
import csv
import io

import requests

from etl.config import API_PAGE_SIZE, API_URL

SOURCE_COLUMNS = [
    "number", "incident_state", "active", "reassignment_count", "reopen_count", "sys_mod_count", "made_sla",
    "caller_id", "opened_by", "opened_at", "sys_created_by", "sys_created_at", "sys_updated_by", "sys_updated_at",
    "contact_type", "location", "category", "subcategory", "u_symptom", "cmdb_ci", "impact", "urgency", "priority",
    "assignment_group", "assigned_to", "knowledge", "u_priority_confirmation", "notify", "problem_id", "rfc",
    "vendor", "caused_by", "closed_code", "resolved_by", "resolved_at", "closed_at",
]


def list_batches() -> list[dict]:
    resp = requests.get(f"{API_URL}/batches", timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_batch(batch: str) -> tuple[list[tuple[int, dict]], int]:
    """Return [(page, row), ...] and the total the API reported."""
    rows, page, total, pages = [], 1, 0, 1
    while page <= pages:
        resp = requests.get(f"{API_URL}/events", params={"batch": batch, "page": page, "size": API_PAGE_SIZE}, timeout=120)
        resp.raise_for_status()
        body = resp.json()
        total, pages = body["total_rows"], body["total_pages"]
        rows.extend((page, r) for r in body["rows"])
        page += 1
    return rows, total


def load_bronze(conn, batch: str, rows: list[tuple[int, dict]]) -> int:
    buf = io.StringIO()
    writer = csv.writer(buf)
    for page, r in rows:
        writer.writerow([batch, page] + [r.get(c, "") for c in SOURCE_COLUMNS])
    buf.seek(0)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM project_staging.raw_incident_event WHERE batch_id = %s", (batch,))
        cur.copy_expert(
            f"COPY project_staging.raw_incident_event (batch_id, api_page, {', '.join(SOURCE_COLUMNS)}) "
            "FROM STDIN WITH (FORMAT csv)", buf)
        cur.execute("SELECT COUNT(*) FROM project_staging.raw_incident_event WHERE batch_id = %s", (batch,))
        n = cur.fetchone()[0]
    conn.commit()
    return n
