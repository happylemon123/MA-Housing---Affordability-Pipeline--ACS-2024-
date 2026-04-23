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
from typing import Any, Iterable


ACS5_2024_BASE = "https://api.census.gov/data/2024/acs/acs5"
MA_STATE_FIPS = "25"
# Census API sometimes uses sentinel values for missing / not applicable fields.
MISSING_SENTINELS = {"-666666666", "-555555555", "-222222222"}


# A compact, Tableau-friendly set of commonly used metrics.
# Each variable has two fields in the API: Estimate (E) and Margin of Error (M).
ACS_VARS = [
    # Population + age/households basics
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
    "B25003_001",  # Occupied housing units (tenure universe)
    "B25003_002",  # Owner occupied
    "B25003_003",  # Renter occupied
    # Race (alone) + Hispanic
    "B02001_002",  # White alone
    "B02001_003",  # Black or African American alone
    "B02001_005",  # Asian alone
    "B02001_007",  # Some other race alone
    "B02001_008",  # Two or more races
    "B03003_003",  # Hispanic or Latino
]


DERIVED_FIELDS = [
    "poverty_rate",
    "owner_rate",
    "renter_rate",
]


@dataclass(frozen=True)
class GeoSpec:
    name: str
    for_clause: str
    in_clause: str | None
    geoid_parts: list[str]


GEOS_DEFAULT: list[GeoSpec] = [
    GeoSpec(
        name="county",
        for_clause="county:*",
        in_clause=f"state:{MA_STATE_FIPS}",
        geoid_parts=["state", "county"],
    ),
    # New England towns are typically county subdivisions (MCDs), not "places".
    GeoSpec(
        name="county_subdivision",
        for_clause="county subdivision:*",
        in_clause=f"state:{MA_STATE_FIPS}&in=county:*",
        geoid_parts=["state", "county", "county subdivision"],
    ),
    GeoSpec(
        name="place",
        for_clause="place:*",
        in_clause=f"state:{MA_STATE_FIPS}",
        geoid_parts=["state", "place"],
    ),
    GeoSpec(
        name="tract",
        for_clause="tract:*",
        in_clause=f"state:{MA_STATE_FIPS}&in=county:*",
        geoid_parts=["state", "county", "tract"],
    ),
]


GEOS_BLOCK_GROUP: GeoSpec = GeoSpec(
    name="block_group",
    for_clause="block group:*",
    in_clause=f"state:{MA_STATE_FIPS}&in=county:*&in=tract:*",
    geoid_parts=["state", "county", "tract", "block group"],
)


def _http_get_json(url: str, timeout_s: int = 60, retries: int = 6) -> Any:
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "ma-acs-tableau/1.0"})
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                data = resp.read()
            return json.loads(data.decode("utf-8"))
        except Exception as e:
            last_err = e
            sleep_s = min(30.0, 1.5 ** attempt) + (0.05 * attempt)
            time.sleep(sleep_s)
    raise RuntimeError(f"HTTP GET failed after {retries} attempts: {url}\nLast error: {last_err}")


def _build_url(
    *,
    base: str,
    get_fields: list[str],
    for_clause: str,
    in_clause: str | None,
    key: str | None,
) -> str:
    # Note: `in` clauses are appended as raw "in=...&in=..." segments in this API style.
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


def _clean_value(value: str | None) -> str:
    if value is None:
        return ""
    v = value.strip()
    if v in MISSING_SENTINELS:
        return ""
    return v


def _safe_div(n: float | None, d: float | None) -> float | None:
    if n is None or d is None or d == 0:
        return None
    return n / d


def _format_rate(x: float | None) -> str:
    if x is None or math.isnan(x):
        return ""
    return f"{x:.6f}"


def _compute_geoid(row: dict[str, str], parts: list[str]) -> str:
    vals = []
    for p in parts:
        v = row.get(p, "")
        vals.append(v)
    return "".join(vals)


def _selected_fields() -> tuple[list[str], list[str]]:
    base_vars = list(ACS_VARS)
    est = [f"{v}E" for v in base_vars]
    moe = [f"{v}M" for v in base_vars]
    return est, moe


def _write_csv(path: str, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def _norm_geo_key(key: str) -> str:
    # Tableau users tend to prefer snake_case, and API geo keys contain spaces.
    return key.replace(" ", "_")


def _geo_id_fields(geo: GeoSpec) -> list[str]:
    # Include the component fields if present, plus computed GEOID.
    # Some geos won't have all fields, but DictWriter will leave blanks.
    fields = ["geography", "year", "dataset", "GEOID", "NAME"]
    for k in geo.geoid_parts:
        nk = _norm_geo_key(k)
        if nk not in fields:
            fields.append(nk)
    return fields


def _build_wide_rows(geo: GeoSpec, rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    wide: list[dict[str, Any]] = []
    for r in rows:
        out: dict[str, Any] = {}
        out["geography"] = geo.name
        out["year"] = 2024
        out["dataset"] = "acs5"
        out["NAME"] = r.get("NAME", "")
        out["GEOID"] = _compute_geoid(r, geo.geoid_parts)
        for k in geo.geoid_parts:
            out[_norm_geo_key(k)] = r.get(k, "")
        # Raw variables (E/M)
        for base in ACS_VARS:
            out[f"{base}E"] = _clean_value(r.get(f"{base}E"))
            out[f"{base}M"] = _clean_value(r.get(f"{base}M"))
        # Derived rates
        pov_num = _as_float(r.get("B17001_002E"))
        pov_den = _as_float(r.get("B17001_001E"))
        out["poverty_rate"] = _format_rate(_safe_div(pov_num, pov_den))

        owner = _as_float(r.get("B25003_002E"))
        renter = _as_float(r.get("B25003_003E"))
        occupied = _as_float(r.get("B25003_001E"))
        # If occupied is missing, fall back to owner+renter.
        occupied_eff = occupied if occupied is not None else ((owner or 0.0) + (renter or 0.0))
        out["owner_rate"] = _format_rate(_safe_div(owner, occupied_eff))
        out["renter_rate"] = _format_rate(_safe_div(renter, occupied_eff))

        wide.append(out)
    return wide


def _build_long_rows(geo: GeoSpec, rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    long_rows: list[dict[str, Any]] = []
    for r in rows:
        base_meta = {
            "geography": geo.name,
            "year": 2024,
            "dataset": "acs5",
            "NAME": r.get("NAME", ""),
            "GEOID": _compute_geoid(r, geo.geoid_parts),
        }
        for k in geo.geoid_parts:
            base_meta[_norm_geo_key(k)] = r.get(k, "")
        for base in ACS_VARS:
            long_rows.append(
                {
                    **base_meta,
                    "variable": base,
                    "estimate": _clean_value(r.get(f"{base}E")),
                    "moe": _clean_value(r.get(f"{base}M")),
                }
            )
        # Derived metrics in the same long file for convenience (no MOE).
        pov_num = _as_float(r.get("B17001_002E"))
        pov_den = _as_float(r.get("B17001_001E"))
        owner = _as_float(r.get("B25003_002E"))
        renter = _as_float(r.get("B25003_003E"))
        occupied = _as_float(r.get("B25003_001E"))
        occupied_eff = occupied if occupied is not None else ((owner or 0.0) + (renter or 0.0))
        derived = {
            "poverty_rate": _safe_div(pov_num, pov_den),
            "owner_rate": _safe_div(owner, occupied_eff),
            "renter_rate": _safe_div(renter, occupied_eff),
        }
        for k, v in derived.items():
            long_rows.append({**base_meta, "variable": k, "estimate": "" if v is None else f"{v:.6f}", "moe": ""})
    return long_rows


def _write_dictionary(out_path: str, key: str | None) -> None:
    url = f"{ACS5_2024_BASE}/variables.json"
    if key:
        url += f"?key={urllib.parse.quote(key)}"
    data = _http_get_json(url)
    vars_obj = data.get("variables", {})

    rows: list[dict[str, str]] = []
    for base in ACS_VARS:
        for suffix in ("E", "M"):
            var = f"{base}{suffix}"
            meta = vars_obj.get(var, {})
            rows.append(
                {
                    "variable": var,
                    "label": str(meta.get("label", "")),
                    "concept": str(meta.get("concept", "")),
                    "group": str(meta.get("group", "")),
                    "predicateType": str(meta.get("predicateType", "")),
                }
            )

    _write_csv(
        out_path,
        rows,
        fieldnames=["variable", "label", "concept", "group", "predicateType"],
    )


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="Download MA ACS 2024 (5-year) and format for Tableau.")
    p.add_argument("--outdir", default="census_ma_2024_tableau/out", help="Output directory")
    p.add_argument("--key", default=os.environ.get("CENSUS_API_KEY", ""), help="Census API key")
    p.add_argument("--include-block-groups", action="store_true", help="Include block group geography (large)")
    args = p.parse_args(argv)

    api_key = args.key.strip() or None
    outdir = args.outdir

    est_fields, moe_fields = _selected_fields()
    get_fields = ["NAME", *est_fields, *moe_fields]

    geos: list[GeoSpec] = list(GEOS_DEFAULT)
    if args.include_block_groups:
        geos.append(GEOS_BLOCK_GROUP)

    # Write dictionary first so Tableau users can join labels regardless of geo.
    _write_dictionary(os.path.join(outdir, "data_dictionary.csv"), api_key)

    all_wide: list[dict[str, Any]] = []
    all_long: list[dict[str, Any]] = []

    for geo in geos:
        url = _build_url(
            base=ACS5_2024_BASE,
            get_fields=get_fields,
            for_clause=geo.for_clause,
            in_clause=geo.in_clause,
            key=api_key,
        )
        raw = _http_get_json(url)
        parsed = _parse_rows(raw)
        if geo.name == "county_subdivision":
            # Drop "county subdivisions not defined" placeholder rows.
            parsed = [r for r in parsed if r.get("county subdivision") not in {"", "00000"}]

        wide_rows = _build_wide_rows(geo, parsed)
        long_rows = _build_long_rows(geo, parsed)

        wide_fields = _geo_id_fields(geo) + [f"{v}E" for v in ACS_VARS] + [f"{v}M" for v in ACS_VARS] + DERIVED_FIELDS
        long_fields = _geo_id_fields(geo) + ["variable", "estimate", "moe"]

        _write_csv(os.path.join(outdir, f"ma_acs5_2024_{geo.name}_wide.csv"), wide_rows, wide_fields)
        _write_csv(os.path.join(outdir, f"ma_acs5_2024_{geo.name}_long.csv"), long_rows, long_fields)

        all_wide.extend(wide_rows)
        all_long.extend(long_rows)

    # Combined files (use the union of geo id parts).
    union_geo_parts: list[str] = []
    for g in geos:
        for part in g.geoid_parts:
            np = _norm_geo_key(part)
            if np not in union_geo_parts:
                union_geo_parts.append(np)

    wide_fields_all = ["geography", "year", "dataset", "GEOID", "NAME", *union_geo_parts]
    wide_fields_all += [f"{v}E" for v in ACS_VARS] + [f"{v}M" for v in ACS_VARS] + DERIVED_FIELDS
    long_fields_all = ["geography", "year", "dataset", "GEOID", "NAME", *union_geo_parts, "variable", "estimate", "moe"]

    _write_csv(os.path.join(outdir, "ma_acs5_2024_all_wide.csv"), all_wide, wide_fields_all)
    _write_csv(os.path.join(outdir, "ma_acs5_2024_all_long.csv"), all_long, long_fields_all)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
