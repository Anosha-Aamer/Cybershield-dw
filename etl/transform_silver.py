"""TRANSFORM: BRONZE -> SILVER for one batch.

Cleaning rules (each one fixes an issue type found in the source, see the Data Cleaning Report):
  R1  trim spaces, normalise case                          ' location 143 ' -> 'Location 143'
  R2  NULL spellings ('', NULL, N/A, -, ?) -> NULL          later mapped to Unknown member (-1)
  R3  parse every date format seen in the source -> TIMESTAMP
  R4  reject rows whose event time is unparseable or in the future (etl_reject_log)
  R5  priority / impact / urgency written many ways -> '<n> - <Name>'
  R6  missing priority rebuilt from impact x urgency (priority matrix learned from clean rows)
  R7  booleans (true/1/Yes/Y...) -> BOOLEAN
  R8  numbers stored as text ('7.0') -> INT; negative counts -> ABS()
  R9  resolved_at / closed_at earlier than opened_at -> NULL
  R10 location / category codes not in the reference map -> NULL (Unknown)
  R11 duplicates removed: one row per (number, sys_mod_count, sys_updated_at)
Output: project_staging.stg_incident_event + data/silver/stg_incident_event_<batch>.csv
"""
import io
import json
import re

import pandas as pd

from etl.config import SILVER_DIR

NULL_TOKENS = {"", "?", "null", "n/a", "na", "-", "none", "nan"}
DATE_FORMATS = ["%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%d-%b-%Y %H:%M", "%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M"]
STATES = {s.lower(): s for s in ["New", "Active", "Awaiting User Info", "Awaiting Problem", "Awaiting Vendor",
                                 "Awaiting Evidence", "Resolved", "Closed"]}
PRIORITY = {"1": "1 - Critical", "2": "2 - High", "3": "3 - Moderate", "4": "4 - Low"}
IMPACT = {"1": "1 - High", "2": "2 - Medium", "3": "3 - Low"}           # urgency uses the same scale
BOOL = {"true": True, "1": True, "yes": True, "y": True, "false": False, "0": False, "no": False, "n": False}
CODE_RE = re.compile(r"^\s*([A-Za-z]+)\s*(\d+)\s*$")

SILVER_COLUMNS = [
    "batch_id", "raw_row_id", "incident_number", "incident_state", "is_active", "reassignment_count", "reopen_count",
    "sys_mod_count", "made_sla", "caller_id", "opened_at", "sys_updated_at", "sys_updated_by", "contact_type",
    "location_code", "category_code", "subcategory_code", "symptom", "impact", "urgency", "priority",
    "assignment_group", "assigned_to", "knowledge", "priority_confirmation", "closed_code", "resolved_by",
    "resolved_at", "closed_at", "dq_flags",
]


class Flags:
    """Collects what was changed on each row -> dq_flags column + per-rule counts."""

    def __init__(self, index):
        self.flags = pd.Series([[] for _ in range(len(index))], index=index, dtype=object)
        self.counts: dict[str, int] = {}

    def add(self, mask: pd.Series, name: str) -> None:
        mask = mask.fillna(False)
        n = int(mask.sum())
        if n:
            self.counts[name] = self.counts.get(name, 0) + n
            for i in mask[mask].index:
                self.flags.at[i].append(name)


def nullify(s: pd.Series) -> pd.Series:
    stripped = s.astype("string").str.strip()
    return stripped.mask(stripped.str.lower().isin(NULL_TOKENS))


def parse_dates(s: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Returns (timestamps, mask of values that were not in the standard source format)."""
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    standard = pd.to_datetime(s, format=DATE_FORMATS[0], errors="coerce")
    out = out.fillna(standard)
    for fmt in DATE_FORMATS[1:]:
        out = out.fillna(pd.to_datetime(s.where(out.isna()), format=fmt, errors="coerce"))
    return out, standard.isna() & out.notna()


def clean_code(s: pd.Series, word: str) -> tuple[pd.Series, pd.Series]:
    def fix(v):
        if pd.isna(v):
            return pd.NA
        m = CODE_RE.match(v)
        return f"{word} {int(m.group(2))}" if m and m.group(1).lower() == word.lower() else pd.NA
    cleaned = s.map(fix).astype("string")
    return cleaned, s.notna() & (cleaned != s).fillna(True)


def clean_scale(s: pd.Series, scale: dict) -> pd.Series:
    by_name = {v.split(" - ")[1].lower(): v for v in scale.values()}

    def fix(v):
        if pd.isna(v):
            return pd.NA
        digits = re.search(r"\d", v)
        if digits and digits.group() in scale:
            return scale[digits.group()]
        return by_name.get(re.sub(r"[^a-z]", "", v.lower()), pd.NA)
    return s.map(fix).astype("string")


def clean_bool(s: pd.Series) -> pd.Series:
    return s.str.lower().map(BOOL).astype("boolean")


def to_int(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").round().astype("Int64")


def transform(raw: pd.DataFrame, batch: str, ref_locations: set, ref_categories: set):
    """Returns (silver_df, rejects_df, stats)."""
    df = raw.copy()
    text_cols = [c for c in df.columns if c not in ("raw_row_id", "batch_id", "api_page", "load_ts")]
    fl = Flags(df.index)

    for c in text_cols:                                                   # R1 + R2
        orig = df[c].astype("string")
        df[c] = nullify(orig)
        fl.add(df[c].notna() & (orig != df[c]), f"trimmed_whitespace:{c}")
        fl.add(orig.notna() & df[c].isna() & (orig.str.strip() != "?"), f"null_token:{c}")

    num = df["number"].str.upper()                                        # R1 incident number
    fl.add(num != df["number"], "fixed_number_format")
    df["number"] = num

    for col, word in [("location", "Location"), ("category", "Category"), ("subcategory", "Subcategory"),
                      ("assignment_group", "Group")]:
        df[col], changed = clean_code(df[col], word)
        fl.add(changed, f"fixed_text:{col}")
    ct = df["contact_type"].str.title()
    fl.add(ct != df["contact_type"], "fixed_text:contact_type")
    df["contact_type"] = ct
    st = df["incident_state"].str.lower().str.replace(r"\s+", " ", regex=True).map(STATES)
    fl.add(st != df["incident_state"], "fixed_text:incident_state")
    df["incident_state"] = st

    for c in ["opened_at", "sys_updated_at", "resolved_at", "closed_at"]:  # R3
        parsed, reformatted = parse_dates(df[c])
        fl.add(reformatted, f"fixed_date_format:{c}")
        df[c + "_ts"] = parsed

    for c, scale in [("priority", PRIORITY), ("impact", IMPACT), ("urgency", IMPACT)]:   # R5
        fixed = clean_scale(df[c], scale)
        fl.add(df[c].notna() & (fixed != df[c]).fillna(True), f"fixed_code_format:{c}")
        df[c] = fixed
    matrix = (df.dropna(subset=["priority", "impact", "urgency"])                         # R6
                .groupby(["impact", "urgency"])["priority"].agg(lambda x: x.mode().iat[0]).to_dict())
    rebuilt = df.apply(lambda r: matrix.get((r["impact"], r["urgency"])) if pd.isna(r["priority"]) else r["priority"], axis=1)
    fl.add(df["priority"].isna() & rebuilt.notna(), "rebuilt_priority")
    df["priority"] = rebuilt.astype("string")

    for c in ["active", "made_sla", "knowledge", "u_priority_confirmation"]:              # R7
        fl.add(df[c].notna() & ~df[c].isin(["TRUE", "FALSE"]), f"fixed_boolean:{c}")
        df[c + "_b"] = clean_bool(df[c])

    for c in ["reassignment_count", "reopen_count", "sys_mod_count"]:                     # R8
        n = to_int(df[c])
        fl.add(df[c].notna() & ~df[c].str.fullmatch(r"-?\d+").fillna(False), f"fixed_numeric_text:{c}")
        fl.add(n < 0, f"fixed_negative:{c}")
        df[c + "_i"] = n.abs()

    # opened_at must be the same on every event; fill gaps from the incident's other events
    opened = df.groupby("number")["opened_at_ts"].transform("min")
    fl.add(df["opened_at_ts"].isna() & opened.notna(), "filled_opened_at")
    df["opened_at_ts"] = df["opened_at_ts"].fillna(opened)

    for c in ["resolved_at", "closed_at"]:                                                 # R9
        bad = df[c + "_ts"] < df["opened_at_ts"]
        fl.add(bad, f"nulled_{c}_before_opened")
        df.loc[bad, c + "_ts"] = pd.NaT

    for col, ref in [("location", ref_locations), ("category", ref_categories)]:           # R10
        orphan = df[col].notna() & ~df[col].isin(ref)
        fl.add(orphan, f"orphan_code:{col}")
        df.loc[orphan, col] = pd.NA
    has_sub = df["subcategory"].notna() & df["category"].isna()
    df.loc[has_sub, "subcategory"] = pd.NA   # a subcategory without its category can't be placed

    # ---------- R4: rows we cannot repair ----------
    batch_start = pd.Timestamp(f"{batch}-01")
    batch_end = batch_start + pd.offsets.MonthBegin(1)
    reasons = pd.Series(pd.NA, index=df.index, dtype="string")
    checks = [
        (df["number"].isna() | ~df["number"].str.fullmatch(r"INC\d+").fillna(False), "invalid or missing incident number"),
        (df["sys_updated_at_ts"].isna(), "invalid or missing event time (sys_updated_at)"),
        (df["sys_updated_at_ts"] >= batch_end, "event time in the future / after batch period"),
        (df["sys_updated_at_ts"] < batch_start, "event time before batch period"),
        (df["opened_at_ts"].isna(), "missing opened_at"),
        (df["sys_mod_count_i"].isna(), "missing sys_mod_count"),
        (df["incident_state"].isna(), "unknown incident_state"),
    ]
    for mask, reason in checks:
        reasons = reasons.mask(reasons.isna() & mask.fillna(False), reason)
    rejected_mask = reasons.notna()
    rejects = raw.loc[rejected_mask, :].assign(reject_reason=reasons[rejected_mask])
    df = df.loc[~rejected_mask]

    # ---------- R11: duplicates ----------
    df = df.sort_values("raw_row_id")
    dup_mask = df.duplicated(["number", "sys_mod_count_i", "sys_updated_at_ts"], keep="first")
    n_dup = int(dup_mask.sum())
    df = df.loc[~dup_mask]

    silver = pd.DataFrame({
        "batch_id": batch, "raw_row_id": df["raw_row_id"], "incident_number": df["number"],
        "incident_state": df["incident_state"], "is_active": df["active_b"],
        "reassignment_count": df["reassignment_count_i"], "reopen_count": df["reopen_count_i"],
        "sys_mod_count": df["sys_mod_count_i"], "made_sla": df["made_sla_b"], "caller_id": df["caller_id"],
        "opened_at": df["opened_at_ts"], "sys_updated_at": df["sys_updated_at_ts"],
        "sys_updated_by": df["sys_updated_by"], "contact_type": df["contact_type"],
        "location_code": df["location"], "category_code": df["category"], "subcategory_code": df["subcategory"],
        "symptom": df["u_symptom"], "impact": df["impact"], "urgency": df["urgency"], "priority": df["priority"],
        "assignment_group": df["assignment_group"], "assigned_to": df["assigned_to"], "knowledge": df["knowledge_b"],
        "priority_confirmation": df["u_priority_confirmation_b"], "closed_code": df["closed_code"],
        "resolved_by": df["resolved_by"], "resolved_at": df["resolved_at_ts"], "closed_at": df["closed_at_ts"],
        "dq_flags": fl.flags.loc[df.index].map(lambda f: ";".join(sorted(set(f))) or None),
    })[SILVER_COLUMNS]

    stats = {"rows_in": len(raw), "rows_rejected": int(rejected_mask.sum()), "rows_duplicate": n_dup,
             "rows_silver": len(silver), "rule_counts": fl.counts,
             "reject_reasons": rejects["reject_reason"].value_counts().to_dict()}
    return silver, rejects, stats


def save_silver(conn, run_id: int, batch: str, silver: pd.DataFrame, rejects: pd.DataFrame) -> None:
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    silver.to_csv(SILVER_DIR / f"stg_incident_event_{batch}.csv", index=False, date_format="%Y-%m-%d %H:%M:%S")

    buf = io.StringIO()
    silver.to_csv(buf, index=False, header=False, date_format="%Y-%m-%d %H:%M:%S")
    buf.seek(0)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM project_staging.stg_incident_event WHERE batch_id = %s", (batch,))
        cur.execute("DELETE FROM project_audit.etl_reject_log WHERE batch_id = %s AND run_id = %s", (batch, run_id))
        cur.copy_expert(f"COPY project_staging.stg_incident_event ({', '.join(SILVER_COLUMNS)}) "
                        "FROM STDIN WITH (FORMAT csv)", buf)
        if len(rejects):
            payload = [(run_id, batch, int(r["raw_row_id"]), r["reject_reason"],
                        json.dumps({k: (None if pd.isna(v) else str(v)) for k, v in r.items()
                                    if k not in ("reject_reason", "load_ts")}))
                       for r in rejects.to_dict(orient="records")]
            cur.executemany("INSERT INTO project_audit.etl_reject_log (run_id, batch_id, raw_row_id, reject_reason, source_row) "
                            "VALUES (%s, %s, %s, %s, %s)", payload)
        # fresh statistics after a bulk load, otherwise the planner picks very slow joins
        cur.execute("ANALYZE project_staging.stg_incident_event")
    conn.commit()
