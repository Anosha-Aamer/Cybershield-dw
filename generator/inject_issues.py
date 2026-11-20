"""Add realistic data-quality problems to the generated monthly files.

The clean year of data (data/generated) is too tidy to show real cleaning work, so this
script damages a small share of rows in the ways real operational exports go wrong.
Every change is counted in data/source/injected_issues_manifest.csv, which the
Data Cleaning Report and the reconciliation checks use.

Input : data/generated/events_YYYY-MM.csv   (clean)
Output: data/source/events_YYYY-MM.csv      (dirty, what the API serves)
Run   : python generator/inject_issues.py
"""
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
IN_DIR = ROOT / "data" / "generated"
OUT_DIR = ROOT / "data" / "source"
SEED = 7

NULL_TOKENS = ["", "NULL", "null", "N/A", "-", "?"]
BOOL_TRUE = ["true", "1", "Yes", "Y", "True"]
BOOL_FALSE = ["false", "0", "No", "N", "False"]


class Injector:
    def __init__(self, df: pd.DataFrame, batch: str, rng: np.random.Generator):
        self.df, self.batch, self.rng = df, batch, rng
        self.log: list[dict] = []

    def pick(self, rate: float, mask: pd.Series | None = None) -> pd.Index:
        idx = self.df.index if mask is None else self.df.index[mask]
        n = int(round(len(idx) * rate))
        return pd.Index(self.rng.choice(idx, size=n, replace=False)) if n else pd.Index([])

    def apply(self, issue: str, column: str, rows: pd.Index, func, rule: str) -> None:
        if len(rows) == 0:
            return
        before = self.df.loc[rows[0], column]
        self.df.loc[rows, column] = [func(v) for v in self.df.loc[rows, column]]
        self.log.append({"batch": self.batch, "issue_type": issue, "column_name": column,
                         "rows_affected": len(rows), "example_before": before,
                         "example_after": self.df.loc[rows[0], column], "cleaning_rule": rule})

    def choice(self, values):
        return lambda _v: values[self.rng.integers(len(values))]


def reformat_date(v: str, fmt: str) -> str:
    try:
        return pd.to_datetime(v, format="%d/%m/%Y %H:%M").strftime(fmt)
    except (ValueError, TypeError):
        return v


def code_variant(rng):
    def f(v: str) -> str:
        if v in ("?", ""):
            return v
        word, num = v.rsplit(" ", 1)
        return [f" {word.lower()} {num} ", f"{word.upper()} {num}", f"{word}{num}", f"{word}  {num} "][rng.integers(4)]
    return f


def priority_variant(rng):
    def f(v: str) -> str:
        if " - " not in v:
            return v
        n, name = v.split(" - ")
        return [name, n, f"P{n}", f"{n}-{name.lower()}", name.upper()][rng.integers(5)]
    return f


def dirty_batch(df: pd.DataFrame, batch: str, rng: np.random.Generator) -> tuple[pd.DataFrame, list[dict]]:
    # 1) duplicates: re-sent rows (exact copies) - later dirt makes some of them near-duplicates
    dup_rows = df.sample(frac=0.015, random_state=int(rng.integers(1e9)))
    df = pd.concat([df, dup_rows], ignore_index=True)
    log = [{"batch": batch, "issue_type": "duplicate_row", "column_name": "(all)", "rows_affected": len(dup_rows),
            "example_before": dup_rows.iloc[0]["number"], "example_after": "row sent twice",
            "cleaning_rule": "keep one row per (number, sys_mod_count, sys_updated_at)"}]
    inj = Injector(df, batch, rng)
    is_q = lambda c: inj.df[c] == "?"

    # 2) messy text in codes / labels
    inj.apply("inconsistent_case_spacing", "number", inj.pick(0.005),
              lambda v: [f" {v}", v.lower(), f"{v} "][rng.integers(3)], "TRIM + UPPER")
    for col in ["location", "category", "subcategory", "assignment_group"]:
        inj.apply("inconsistent_case_spacing", col, inj.pick(0.03), code_variant(rng),
                  "TRIM, collapse spaces, Title case, 'Word n' pattern")
    inj.apply("inconsistent_case_spacing", "contact_type", inj.pick(0.03),
              lambda v: [v.lower(), v.upper() + " ", f" {v}"][rng.integers(3)], "TRIM + Title case")
    inj.apply("inconsistent_case_spacing", "incident_state", inj.pick(0.02),
              lambda v: [v.lower(), v.upper(), f"{v} "][rng.integers(3)], "TRIM + map to standard state name")

    # 3) many spellings of 'missing' + some new gaps
    for col in ["location", "category", "subcategory", "assignment_group", "caller_id", "resolved_at", "u_symptom"]:
        inj.apply("null_token_variant", col, inj.pick(0.5, is_q(col)), inj.choice(NULL_TOKENS),
                  "map '', NULL, N/A, -, ? to NULL -> Unknown member (-1)")
    for col, rate in [("location", 0.004), ("category", 0.003), ("priority", 0.002), ("assignment_group", 0.005)]:
        inj.apply("missing_value", col, inj.pick(rate, ~is_q(col)), inj.choice(NULL_TOKENS),
                  "NULL -> Unknown member (-1); priority rebuilt from impact x urgency")

    # 4) mixed date formats + impossible dates
    for col, fmt in [("opened_at", "%Y-%m-%d %H:%M:%S"), ("sys_updated_at", "%d-%b-%Y %H:%M"),
                     ("resolved_at", "%Y/%m/%d %H:%M"), ("closed_at", "%Y-%m-%d %H:%M:%S")]:
        inj.apply("mixed_date_format", col, inj.pick(0.08, ~is_q(col)), lambda v, f=fmt: reformat_date(v, f),
                  "parse all known formats -> TIMESTAMP")
    inj.apply("invalid_date", "sys_updated_at", inj.pick(0.0005), inj.choice(["31/2/2016 10:00", "0/13/2016 25:61", "##/##/####"]),
              "unparseable event time -> reject row to etl_reject_log")
    inj.apply("future_date", "sys_updated_at", inj.pick(0.0003),
              lambda v: v.replace("2016", "2030").replace("2017", "2031"), "event time after load date -> reject row")

    # 5) priority / impact / urgency written many ways
    for col in ["priority", "impact", "urgency"]:
        inj.apply("inconsistent_code_format", col, inj.pick(0.05), priority_variant(rng),
                  "map to '<n> - <Name>' using lookup")

    # 6) booleans written many ways
    for col in ["active", "made_sla", "knowledge", "u_priority_confirmation"]:
        rows = inj.pick(0.10)
        inj.apply("inconsistent_boolean", col, rows,
                  lambda v: (BOOL_TRUE if v == "TRUE" else BOOL_FALSE)[rng.integers(5)], "map to BOOLEAN")

    # 7) logically wrong values
    inj.apply("negative_count", "reassignment_count", inj.pick(0.01, inj.df["reassignment_count"] != "0"),
              lambda v: f"-{v}", "negative count (sign error) -> ABS()")
    inj.apply("resolved_before_opened", "resolved_at", inj.pick(0.003, ~is_q("resolved_at")),
              lambda v: "1/1/2015 00:00", "resolved_at < opened_at -> NULL + flag")
    inj.apply("numeric_as_text", "sys_mod_count", inj.pick(0.003), lambda v: f"{v}.0",
              "cast text -> INT")

    # 8) codes that are not in the reference mapping (orphans)
    inj.apply("orphan_code", "location", inj.pick(0.001), lambda v: "Location 999",
              "not in location_map -> Unknown member (-1)")
    inj.apply("orphan_code", "category", inj.pick(0.001), lambda v: "Category 88",
              "not in category_map -> Unknown member (-1)")

    out = inj.df.sample(frac=1, random_state=int(rng.integers(1e9))).reset_index(drop=True)  # source order is not guaranteed
    return out, log + inj.log


def main() -> None:
    rng = np.random.default_rng(SEED)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest = []
    for f in sorted(IN_DIR.glob("events_*.csv")):
        batch = f.stem.split("_")[1]
        clean = pd.read_csv(f, dtype=str, keep_default_na=False)
        dirty, log = dirty_batch(clean, batch, rng)
        dirty.to_csv(OUT_DIR / f.name, index=False)
        manifest += log
        print(f"{f.name}: {len(clean):,} clean -> {len(dirty):,} dirty rows, {sum(l['rows_affected'] for l in log):,} cell issues")
    pd.DataFrame(manifest).to_csv(OUT_DIR / "injected_issues_manifest.csv", index=False)


if __name__ == "__main__":
    main()
