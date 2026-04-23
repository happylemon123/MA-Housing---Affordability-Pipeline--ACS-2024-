#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


MISSING_SENTINELS = {"-666666666", "-555555555", "-222222222"}

# Same core set as the MA exporter, plus vacancy components.
ACS_VARS = [
    "B01003_001",  # Total population
    "B11001_001",  # Households
    "B19013_001",  # Median household income
    "B19301_001",  # Per capita income
    "B17001_001",  # Poverty universe
    "B17001_002",  # Below poverty
    "B25001_001",  # Housing units
    "B25002_001",  # Occupancy universe (total units)
    "B25002_002",  # Occupied units
    "B25002_003",  # Vacant units
    "B25003_001",  # Tenure universe (occupied units)
    "B25003_002",  # Owner occupied
    "B25003_003",  # Renter occupied
    "B25077_001",  # Median home value
    "B25064_001",  # Median gross rent
]

DERIVED_FIELDS = ["poverty_rate", "owner_rate", "renter_rate", "vacant_units", "vacancy_rate"]


@dataclass(frozen=True)
class GeoSpec:
    name: str
    for_clause: str
    in_clause: str | None
    geoid_parts: list[str]


STATE_GEO = GeoSpec(name="state", for_clause="state:*", in_clause=None, geoid_parts=["state"])


def _http_get_json(url: str, timeout_s: int = 60, retries: int = 6) -> Any:
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "state-acs-tableau/1.0"})
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                data = resp.read()
            return json.loads(data.decode("utf-8"))
        except Exception as e:
            last_err = e
            time.sleep(min(30.0, 1.5**attempt) + (0.05 * attempt))
    raise RuntimeError(f"HTTP GET failed after {retries} attempts: {url}\nLast error: {last_err}")


def _build_url(*, base: str, get_fields: list[str], for_clause: str, in_clause: str | None, key: str | None) -> str:
    qs = [("get", ",".join(get_fields)), ("for", for_clause)]
    url = f"{base}?{urllib.parse.urlencode(qs)}"
    if in_clause:
        url += f"&in={in_clause}"
    if key:
        url += f"&key={urllib.parse.quote(key)}"
    return url


def _parse_rows(raw: list[list[str]]) -> list[dict[str, str]]:
    if not raw or len(raw) < 2:
        return []
    header = raw[0]
    out: list[dict[str, str]] = []
    for row in raw[1:]:
        if len(row) != len(header):
            continue
        out.append(dict(zip(header, row)))
    return out


def _clean_value(value: str | None) -> str:
    if value is None:
        return ""
    v = value.strip()
    if v in MISSING_SENTINELS:
        return ""
    return v


def _as_float(value: str | None) -> float | None:
    if value is None:
        return None
    value = value.strip()
    if value == "" or value.lower() in {"null", "nan"}:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _safe_div(n: float | None, d: float | None) -> float | None:
    if n is None or d is None or d == 0:
        return None
    return n / d


def _format_rate(x: float | None) -> str:
    if x is None or math.isnan(x):
        return ""
    return f"{x:.6f}"


def _compute_geoid(row: dict[str, str], parts: list[str]) -> str:
    return "".join([row.get(p, "") for p in parts])


def _selected_fields() -> tuple[list[str], list[str]]:
    est = [f"{v}E" for v in ACS_VARS]
    moe = [f"{v}M" for v in ACS_VARS]
    return est, moe


def _write_csv(path: str, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def _base_for_year(year: int, *, use_acs5_for_all_years: bool) -> tuple[str, str]:
    if use_acs5_for_all_years or year == 2020:
        return (f"https://api.census.gov/data/{year}/acs/acs5", "acs5")
    return (f"https://api.census.gov/data/{year}/acs/acs1", "acs1")


def _rows_for_year_state(year: int, *, dataset: str, parsed: list[dict[str, str]]) -> list[dict[str, Any]]:
    out_rows: list[dict[str, Any]] = []
    for r in parsed:
        out: dict[str, Any] = {}
        out["geography"] = "state"
        out["year"] = year
        out["dataset"] = dataset
        out["NAME"] = r.get("NAME", "")
        out["state_fips"] = r.get("state", "")
        out["GEOID"] = _compute_geoid(r, ["state"])

        for base in ACS_VARS:
            out[f"{base}E"] = _clean_value(r.get(f"{base}E"))
            out[f"{base}M"] = _clean_value(r.get(f"{base}M"))

        pov_num = _as_float(r.get("B17001_002E"))
        pov_den = _as_float(r.get("B17001_001E"))
        out["poverty_rate"] = _format_rate(_safe_div(pov_num, pov_den))

        owner = _as_float(r.get("B25003_002E"))
        renter = _as_float(r.get("B25003_003E"))
        occupied_tenure = _as_float(r.get("B25003_001E"))
        occupied_eff = occupied_tenure if occupied_tenure is not None else ((owner or 0.0) + (renter or 0.0))
        out["owner_rate"] = _format_rate(_safe_div(owner, occupied_eff))
        out["renter_rate"] = _format_rate(_safe_div(renter, occupied_eff))

        vacant_units = _as_float(r.get("B25002_003E"))
        occupancy_universe = _as_float(r.get("B25002_001E"))
        out["vacant_units"] = "" if vacant_units is None else f"{vacant_units:.0f}"
        out["vacancy_rate"] = _format_rate(_safe_div(vacant_units, occupancy_universe))

        out_rows.append(out)
    return out_rows


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(
        description="Download ACS 2020–2024 time series for one or more states (state-level rows), Tableau-friendly."
    )
    p.add_argument("--outdir", default="census_ma_2024_tableau/out", help="Output directory")
    p.add_argument("--key", default=os.environ.get("CENSUS_API_KEY", ""), help="Census API key (optional)")
    p.add_argument("--states", default="25,23", help="Comma-separated state FIPS codes (e.g. 25=MA,23=ME)")
    p.add_argument("--start-year", type=int, default=2020)
    p.add_argument("--end-year", type=int, default=2024)
    p.add_argument("--acs5-all-years", action="store_true")
    args = p.parse_args(argv)

    api_key = args.key.strip() or None
    state_fips = [s.strip() for s in args.states.split(",") if s.strip()]
    if not state_fips:
        raise SystemExit("No states provided")

    est_fields, moe_fields = _selected_fields()
    get_fields = ["NAME", *est_fields, *moe_fields]

    rows_all: list[dict[str, Any]] = []
    for year in range(args.start_year, args.end_year + 1):
        base, dataset = _base_for_year(year, use_acs5_for_all_years=args.acs5_all_years)
        for st in state_fips:
            url = _build_url(
                base=base,
                get_fields=get_fields,
                for_clause=f"state:{st}",
                in_clause=None,
                key=api_key,
            )
            raw = _http_get_json(url)
            parsed = _parse_rows(raw)
            rows_all.extend(_rows_for_year_state(year, dataset=dataset, parsed=parsed))

    fieldnames = [
        "geography",
        "year",
        "dataset",
        "GEOID",
        "NAME",
        "state_fips",
        *[f"{v}E" for v in ACS_VARS],
        *[f"{v}M" for v in ACS_VARS],
        *DERIVED_FIELDS,
    ]

    out_path = os.path.join(args.outdir, "states_acs_2020_2024_state_timeseries_wide.csv")
    _write_csv(out_path, rows_all, fieldnames)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

