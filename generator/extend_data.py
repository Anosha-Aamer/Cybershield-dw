"""Grow the 3-week incident log into one year of data (Mar 2016 - Feb 2017).

How it works
------------
* Every real incident in data/raw is a *template*: its full chain of update events
  (New -> Active -> Resolved -> Closed ...) with the real gaps between them.
* For each calendar day we decide how many incidents are reported
  (base volume x weekday pattern x month seasonality x random noise), sample that many
  templates and replay them on that day: all timestamps are shifted by the same number of
  days, so time-of-day, durations and state flows stay realistic.
* Each replayed incident gets a new number (INC1000001 ...). Part of them get a different
  location or category/subcategory pair so the year is not a copy of the 3 weeks.
* Events are split into monthly batches by the month the *event* happened (sys_updated_at),
  like a real monthly extract. Events after 28 Feb 2017 are not yet in the source, so
  incidents still running at the end stay open -> realistic backlog.

Output: data/generated/events_YYYY-MM.csv (clean, same 36 columns as the original).
Run:    python generator/extend_data.py
"""
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "incident_event_log.csv"
OUT = ROOT / "data" / "generated"

SEED = 2016
START, END = pd.Timestamp("2016-03-01"), pd.Timestamp("2017-02-28")
CUTOFF = pd.Timestamp("2017-02-28 23:59")
BASE_PER_DAY = 210                      # avg incidents per weekday-adjusted day
# month seasonality (Mar..Feb) - slow growth plus peaks around Ramadan/Eid and year end
MONTH_FACTOR = {3: 0.90, 4: 0.93, 5: 0.97, 6: 1.08, 7: 1.10, 8: 0.98,
                9: 1.00, 10: 1.04, 11: 1.07, 12: 1.15, 1: 1.05, 2: 1.10}
P_NEW_LOCATION = 0.30
P_NEW_CATEGORY = 0.25
DATE_COLS = ["opened_at", "sys_created_at", "sys_updated_at", "resolved_at", "closed_at"]
FMT = "%d/%m/%Y %H:%M"


def to_source_format(s: pd.Series) -> pd.Series:
    """Datetime -> '29/2/2016 01:16' (no zero padding on day/month, like the source)."""
    out = (s.dt.day.astype("Int64").astype(str) + "/" + s.dt.month.astype("Int64").astype(str) + "/"
           + s.dt.year.astype("Int64").astype(str) + " " + s.dt.strftime("%H:%M"))
    return out.where(s.notna(), "?")


def main() -> None:
    rng = np.random.default_rng(SEED)
    src = pd.read_csv(RAW, dtype=str, keep_default_na=False)
    for c in DATE_COLS:
        src[c + "_dt"] = pd.to_datetime(src[c].replace("?", None), format=FMT, errors="coerce")

    # one row per template incident
    inc = src.groupby("number").agg(opened=("opened_at_dt", "first"),
                                    location=("location", "first"),
                                    category=("category", "first"),
                                    subcategory=("subcategory", "first")).reset_index()
    inc["weekday"] = inc["opened"].dt.weekday

    # weekday pattern taken from the real data (incidents per weekday / number of such days)
    days = pd.date_range(src["opened_at_dt"].min().normalize(), src["opened_at_dt"].max().normalize())
    per_wd = inc["weekday"].value_counts() / pd.Series(days.weekday).value_counts()
    wd_factor = (per_wd / per_wd.mean()).to_dict()

    locations = inc.loc[inc["location"] != "?", "location"].to_numpy()
    cat_pairs = inc.loc[inc["category"] != "?", ["category", "subcategory"]].to_numpy()
    by_wd = {wd: g["number"].to_numpy() for wd, g in inc.groupby("weekday")}

    # ---- decide which template is replayed on which day
    plan = []
    for day in pd.date_range(START, END):
        n = rng.poisson(BASE_PER_DAY * wd_factor[day.weekday()] * MONTH_FACTOR[day.month])
        picks = rng.choice(by_wd[day.weekday()], size=n, replace=True)
        plan.append(pd.DataFrame({"template": picks, "day": day}))
    plan = pd.concat(plan, ignore_index=True).merge(inc[["number", "opened", "location", "category", "subcategory"]],
                                                    left_on="template", right_on="number")
    plan["delta"] = plan["day"] - plan["opened"].dt.normalize()
    plan = plan.sort_values(["day", "opened"]).reset_index(drop=True)  # opened is time-of-day order within day
    plan["new_number"] = [f"INC{1000001 + i}" for i in range(len(plan))]

    move_loc = rng.random(len(plan)) < P_NEW_LOCATION
    plan["new_location"] = np.where(move_loc, rng.choice(locations, len(plan)), plan["location"])
    move_cat = rng.random(len(plan)) < P_NEW_CATEGORY
    pair_idx = rng.integers(0, len(cat_pairs), len(plan))
    plan["new_category"] = np.where(move_cat, cat_pairs[pair_idx, 0], plan["category"])
    plan["new_subcategory"] = np.where(move_cat, cat_pairs[pair_idx, 1], plan["subcategory"])

    # ---- replay events
    ev = plan[["template", "new_number", "delta", "location", "category", "subcategory",
               "new_location", "new_category", "new_subcategory"]] \
        .merge(src, left_on="template", right_on="number", suffixes=("_tpl", ""))
    ev["number"] = ev["new_number"]
    for c in DATE_COLS:
        ev[c] = to_source_format(ev[c + "_dt"] + ev["delta"])
        ev[c + "_dt"] = ev[c + "_dt"] + ev["delta"]
    # only move the values that equal the template's reported value (later re-classifications stay)
    for col in ["location", "category", "subcategory"]:
        same = ev[col] == ev[col + "_tpl"]
        ev.loc[same, col] = ev.loc[same, "new_" + col]

    ev = ev[ev["sys_updated_at_dt"] <= CUTOFF]
    ev["batch"] = ev["sys_updated_at_dt"].dt.strftime("%Y-%m")
    ev = ev.sort_values(["sys_updated_at_dt", "number", "sys_mod_count"])

    OUT.mkdir(parents=True, exist_ok=True)
    cols = list(src.columns[:36])
    for batch, g in ev.groupby("batch"):
        g[cols].to_csv(OUT / f"events_{batch}.csv", index=False)
        print(f"events_{batch}.csv  {len(g):>7,} events  {g['number'].nunique():>6,} incidents")
    print(f"TOTAL {len(ev):,} events, {ev['number'].nunique():,} incidents")


if __name__ == "__main__":
    main()
