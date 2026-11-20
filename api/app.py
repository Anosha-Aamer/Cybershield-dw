"""CyberShield source-system API.

Plays the role of the company's incident-management system. The ETL never reads the CSV
files directly; it pulls them over HTTP, one monthly batch at a time, page by page.

Endpoints
  GET /health                               -> service status
  GET /batches                              -> list of available monthly batches + row counts
  GET /events?batch=2016-03&page=1&size=5000 -> one page of raw events (all values as text)

Run:  uvicorn api.app:app --port 8000        (from the project root)
Docs: http://127.0.0.1:8000/docs
"""
from functools import lru_cache
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, Query

SOURCE_DIR = Path(__file__).resolve().parents[1] / "data" / "source"

app = FastAPI(title="CyberShield Incident Source API", version="1.0")


def batch_file(batch: str) -> Path:
    path = SOURCE_DIR / f"events_{batch}.csv"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"batch '{batch}' not found")
    return path


@lru_cache(maxsize=4)
def load_batch(batch: str) -> pd.DataFrame:
    # everything stays text: the API hands over the data exactly as the source holds it
    return pd.read_csv(batch_file(batch), dtype=str, keep_default_na=False)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "batches": len(list(SOURCE_DIR.glob("events_*.csv")))}


@app.get("/batches")
def batches() -> list[dict]:
    out = []
    for f in sorted(SOURCE_DIR.glob("events_*.csv")):
        batch = f.stem.split("_", 1)[1]
        with f.open(encoding="utf-8") as fh:
            rows = sum(1 for _ in fh) - 1
        out.append({"batch": batch, "rows": rows})
    return out


@app.get("/events")
def events(batch: str = Query(..., pattern=r"^\d{4}-\d{2}$", examples=["2016-03"]),
           page: int = Query(1, ge=1), size: int = Query(5000, ge=1, le=20000)) -> dict:
    df = load_batch(batch)
    total = len(df)
    pages = max(1, -(-total // size))
    start = (page - 1) * size
    chunk = df.iloc[start:start + size]
    return {"batch": batch, "page": page, "size": size, "total_rows": total, "total_pages": pages,
            "rows": chunk.to_dict(orient="records")}
