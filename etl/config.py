"""Settings for the ETL. Values come from the .env file in the project root (see .env.example)."""
import os
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "sql"
DATA_DIR = ROOT / "data"
SILVER_DIR = DATA_DIR / "silver"
REFERENCE_DIR = DATA_DIR / "reference"


def _load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


_load_env(ROOT / ".env")

PG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "cybershield_dw"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", ""),
}
API_URL = os.getenv("API_URL", "http://127.0.0.1:8000")
API_PAGE_SIZE = int(os.getenv("API_PAGE_SIZE", "5000"))
PIPELINE_NAME = "incident_events"


def connect(dbname: str | None = None):
    params = dict(PG)
    if dbname:
        params["dbname"] = dbname
    return psycopg2.connect(**params)
