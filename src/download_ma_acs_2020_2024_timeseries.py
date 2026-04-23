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


MA_STATE_FIPS = "25"
MISSING_SENTINELS = {"-666666666", "-555555555", "-222222222"}

# Keep this in sync with download_ma_acs_2024.py to make joining easy in Tableau.
ACS_VARS = [
    # Population + households basics
    "B01003_001",  # Total population
    "B11001_001",  # Households
    # Income
    "B19013_001",  # Median household income
    "B19301_001",  # Per capita income
    # Poverty
    "B17001_001",  # Poverty universe
    "B17001_002",  # Below poverty
    # Housing
    "B25001_001",  # Housing units
    "B25002_001",  # Occupancy status (universe)
    "B25002_002",  # Occupied
    "B25002_003",  # Vacant
    "B25003_001",  # Occupied housing units (tenure universe)
    "B25003_002",  # Owner occupied
    "B25003_003",  # Renter occupied
    # Optional: common housing medians
    "B25077_001",  # Median value (dollars)
    "B25064_001",  # Median gross rent (dollars)
    # Owner costs + mortgage status (owner-occupied stock)
    "B25081_001",  # Mortgage status universe
    "B25081_002",  # With a mortgage / similar debt
    "B25081_009",  # Without a mortgage
    "B25088_002",  # Median selected monthly owner costs: with a mortgage
    "B25088_003",  # Median selected monthly owner costs: without a mortgage
    # Burden distributions (proportions, not medians)
    # Gross rent as % of household income (renters paying cash rent)
    "B25070_001",  # Total
    "B25070_007",  # 30.0 to 34.9 percent
    "B25070_008",  # 35.0 to 39.9 percent
    "B25070_009",  # 40.0 to 49.9 percent
    "B25070_010",  # 50.0 percent or more
    # Owner costs as % of household income (with mortgage)
    "B25091_002",  # Housing units with a mortgage: total
    "B25091_008",  # 30.0 to 34.9 percent
    "B25091_009",  # 35.0 to 39.9 percent
    "B25091_010",  # 40.0 to 49.9 percent
    "B25091_011",  # 50.0 percent or more
]

DERIVED_FIELDS = [
    "poverty_rate",
    "owner_rate",
    "renter_rate",
    "vacant_units",
    "vacancy_rate",
    # Affordability pressure indices (simple 30% rule)
    "affordable_monthly_30pct_income",
    "rent_pressure",
    "mortgage_share",
    "owner_cost_pressure_with_mortgage",
    "owner_cost_pressure_without_mortgage",
    # Burden rates (shares)
    "rent_burden_30p",
    "rent_burden_50p",
    "mortgage_burden_30p",
    "mortgage_burden_50p",
]


@dataclass(frozen=True)
class GeoSpec:
    name: str
    for_clause: str
    in_clause: str | None
    geoid_parts: list[str]


COUNTY_GEO = GeoSpec(
    name="county",
    for_clause="county:*",
    in_clause=f"state:{MA_STATE_FIPS}",
    geoid_parts=["state", "county"],
)


def _http_get_json(url: str, timeout_s: int = 60, retries: int = 6) -> Any:
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "ma-acs-tableau/1.1"})
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


def _chunk_list(items: list[str], chunk_size: int) -> list[list[str]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def _geo_key(row: dict[str, str], geo: GeoSpec) -> tuple[str, ...]:
    # Use the raw geography-part fields returned by the API response.
    return tuple((row.get(p, "") or "") for p in geo.geoid_parts)


def _fetch_merged_rows(
    *,
    base: str,
    geo: GeoSpec,
    value_fields: list[str],
    key: str | None,
    chunk_size: int = 40,
) -> list[dict[str, str]]:
    """
    Census API can return HTTP 400 if `get=` includes too many fields.
    Split into chunks and merge by geography keys.
    """
    merged: dict[tuple[str, ...], dict[str, str]] = {}
    for chunk in _chunk_list(value_fields, chunk_size):
        get_fields = ["NAME", *chunk]
        url = _build_url(base=base, get_fields=get_fields, for_clause=geo.for_clause, in_clause=geo.in_clause, key=key)
        raw = _http_get_json(url)
        parsed = _parse_rows(raw)
        for r in parsed:
            k = _geo_key(r, geo)
            prev = merged.get(k)
            if prev is None:
                merged[k] = dict(r)
            else:
                for kk, vv in r.items():
                    if kk not in prev or prev[kk] == "":
                        prev[kk] = vv
    return list(merged.values())


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
    # Note: Standard 2020 ACS 1-year estimates are not available on the API.
    if use_acs5_for_all_years or year == 2020:
        return (f"https://api.census.gov/data/{year}/acs/acs5", "acs5")
    return (f"https://api.census.gov/data/{year}/acs/acs1", "acs1")


def _wide_rows_for_year(year: int, *, dataset: str, geo: GeoSpec, parsed: list[dict[str, str]]) -> list[dict[str, Any]]:
    wide: list[dict[str, Any]] = []
    for r in parsed:
        out: dict[str, Any] = {}
        out["geography"] = geo.name
        out["year"] = year
        out["dataset"] = dataset
        out["NAME"] = r.get("NAME", "")
        out["GEOID"] = _compute_geoid(r, geo.geoid_parts)
        out["state"] = r.get("state", "")
        out["county"] = r.get("county", "")
        for base in ACS_VARS:
            out[f"{base}E"] = _clean_value(r.get(f"{base}E"))
            out[f"{base}M"] = _clean_value(r.get(f"{base}M"))

        pov_num = _as_float(r.get("B17001_002E"))
        pov_den = _as_float(r.get("B17001_001E"))
        out["poverty_rate"] = _format_rate(_safe_div(pov_num, pov_den))

        owner = _as_float(r.get("B25003_002E"))
        renter = _as_float(r.get("B25003_003E"))
        occupied = _as_float(r.get("B25003_001E"))
        occupied_eff = occupied if occupied is not None else ((owner or 0.0) + (renter or 0.0))
        out["owner_rate"] = _format_rate(_safe_div(owner, occupied_eff))
        out["renter_rate"] = _format_rate(_safe_div(renter, occupied_eff))

        vacant_units = _as_float(r.get("B25002_003E"))
        occupancy_universe = _as_float(r.get("B25002_001E"))
        if vacant_units is None:
            # Fallback: total housing units minus occupied (tenure universe).
            total_units = _as_float(r.get("B25001_001E"))
            occupied_units = _as_float(r.get("B25003_001E"))
            if total_units is not None and occupied_units is not None:
                vacant_units = total_units - occupied_units
        out["vacant_units"] = "" if vacant_units is None else f"{vacant_units:.0f}"
        out["vacancy_rate"] = _format_rate(_safe_div(vacant_units, occupancy_universe))

        # Pressure indices (rent/mortgage compared to 30% income rule).
        affordable_monthly = None if out["B19013_001E"] == "" else (0.30 * float(out["B19013_001E"]) / 12.0)
        out["affordable_monthly_30pct_income"] = "" if affordable_monthly is None else f"{affordable_monthly:.2f}"

        median_rent = _as_float(r.get("B25064_001E"))
        out["rent_pressure"] = _format_rate(_safe_div(median_rent, affordable_monthly))

        mort_univ = _as_float(r.get("B25081_001E"))
        mort_with = _as_float(r.get("B25081_002E"))
        out["mortgage_share"] = _format_rate(_safe_div(mort_with, mort_univ))

        owner_cost_mort = _as_float(r.get("B25088_002E"))
        owner_cost_nomort = _as_float(r.get("B25088_003E"))
        out["owner_cost_pressure_with_mortgage"] = _format_rate(_safe_div(owner_cost_mort, affordable_monthly))
        out["owner_cost_pressure_without_mortgage"] = _format_rate(_safe_div(owner_cost_nomort, affordable_monthly))

        # Proportion-based burden metrics (more interpretable than pressure indices).
        rent_univ = _as_float(r.get("B25070_001E"))
        rent_30_34 = _as_float(r.get("B25070_007E"))
        rent_35_39 = _as_float(r.get("B25070_008E"))
        rent_40_49 = _as_float(r.get("B25070_009E"))
        rent_50p = _as_float(r.get("B25070_010E"))
        rent_30p = (rent_30_34 or 0.0) + (rent_35_39 or 0.0) + (rent_40_49 or 0.0) + (rent_50p or 0.0)
        out["rent_burden_30p"] = _format_rate(_safe_div(rent_30p, rent_univ))
        out["rent_burden_50p"] = _format_rate(_safe_div(rent_50p, rent_univ))

        mort_univ2 = _as_float(r.get("B25091_002E"))
        mort_30_34 = _as_float(r.get("B25091_008E"))
        mort_35_39 = _as_float(r.get("B25091_009E"))
        mort_40_49 = _as_float(r.get("B25091_010E"))
        mort_50p = _as_float(r.get("B25091_011E"))
        mort_30p = (mort_30_34 or 0.0) + (mort_35_39 or 0.0) + (mort_40_49 or 0.0) + (mort_50p or 0.0)
        out["mortgage_burden_30p"] = _format_rate(_safe_div(mort_30p, mort_univ2))
        out["mortgage_burden_50p"] = _format_rate(_safe_div(mort_50p, mort_univ2))

        wide.append(out)
    return wide


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(
        description="Download MA ACS 2020–2024 (county) and format for Tableau.\n"
        "Uses ACS 1-year for 2021–2024 and ACS 5-year for 2020 by default."
    )
    p.add_argument("--outdir", default="census_ma_2024_tableau/out", help="Output directory")
    p.add_argument("--key", default=os.environ.get("CENSUS_API_KEY", ""), help="Census API key (optional)")
    p.add_argument("--start-year", type=int, default=2020)
    p.add_argument("--end-year", type=int, default=2024)
    p.add_argument(
        "--acs5-all-years",
        action="store_true",
        help="Use ACS 5-year for all years (rolling 5-year estimates; more geographic coverage).",
    )
    args = p.parse_args(argv)

    api_key = args.key.strip() or None
    outdir = args.outdir

    start_year = args.start_year
    end_year = args.end_year
    if start_year > end_year:
        raise SystemExit("--start-year must be <= --end-year")

    est_fields, moe_fields = _selected_fields()
    value_fields = [*est_fields, *moe_fields]

    all_rows: list[dict[str, Any]] = []
    for year in range(start_year, end_year + 1):
        base, dataset = _base_for_year(year, use_acs5_for_all_years=args.acs5_all_years)
        parsed = _fetch_merged_rows(base=base, geo=COUNTY_GEO, value_fields=value_fields, key=api_key, chunk_size=40)
        all_rows.extend(_wide_rows_for_year(year, dataset=dataset, geo=COUNTY_GEO, parsed=parsed))

    fieldnames = [
        "geography",
        "year",
        "dataset",
        "GEOID",
        "NAME",
        "state",
        "county",
        *[f"{v}E" for v in ACS_VARS],
        *[f"{v}M" for v in ACS_VARS],
        *DERIVED_FIELDS,
    ]

    out_path = os.path.join(outdir, "ma_acs_2020_2024_county_timeseries_wide.csv")
    _write_csv(out_path, all_rows, fieldnames)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
