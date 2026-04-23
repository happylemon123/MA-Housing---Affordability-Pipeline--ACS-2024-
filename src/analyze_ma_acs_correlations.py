#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import os
from dataclasses import dataclass
from typing import Any


def _fnum(x: str | None) -> float | None:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 3:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    if vx == 0 or vy == 0:
        return None
    return cov / math.sqrt(vx * vy)


def _rankdata(vals: list[float]) -> list[float]:
    # Average ranks for ties (1..n).
    # Good enough for Spearman without pulling in scipy.
    indexed = list(enumerate(vals))
    indexed.sort(key=lambda t: t[1])
    ranks = [0.0] * len(vals)
    i = 0
    n = len(vals)
    while i < n:
        j = i
        while j + 1 < n and indexed[j + 1][1] == indexed[i][1]:
            j += 1
        avg_rank = (i + 1 + j + 1) / 2.0
        for k in range(i, j + 1):
            ranks[indexed[k][0]] = avg_rank
        i = j + 1
    return ranks


def _spearman(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3:
        return None
    rx = _rankdata(xs)
    ry = _rankdata(ys)
    return _pearson(rx, ry)


def _safe_div(n: float | None, d: float | None) -> float | None:
    if n is None or d is None or d == 0:
        return None
    return n / d


@dataclass(frozen=True)
class Metric:
    key: str
    label: str


METRICS: list[Metric] = [
    Metric("poverty_rate", "Poverty rate"),
    Metric("vacancy_rate", "Vacancy rate"),
    Metric("owner_rate", "Owner-occupied share"),
    Metric("renter_rate", "Renter-occupied share"),
    Metric("median_household_income_est", "Median household income ($)"),
    Metric("per_capita_income_est", "Per capita income ($)"),
    Metric("median_home_value_est", "Median home value ($)"),
    Metric("median_gross_rent_est", "Median gross rent ($)"),
    # Crowding proxies (not rooms/person; these are people per unit/household).
    Metric("people_per_occupied_unit", "People per occupied housing unit (proxy)"),
    Metric("people_per_household", "People per household (proxy)"),
]


def _extract_metrics(row: dict[str, str]) -> dict[str, float | None]:
    out: dict[str, float | None] = {}
    for m in METRICS:
        out[m.key] = _fnum(row.get(m.key))

    pop = _fnum(row.get("total_population_est"))
    occupied = _fnum(row.get("occupied_units_est"))  # B25002_002E
    households = _fnum(row.get("total_households_est"))  # B11001_001E
    out["people_per_occupied_unit"] = _safe_div(pop, occupied)
    out["people_per_household"] = _safe_div(pop, households)
    return out


def _corr_rows(rows: list[dict[str, str]], *, scope: str, year: int | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    metric_vals: dict[str, list[float | None]] = {m.key: [] for m in METRICS}

    # Build aligned arrays by metric per row.
    extracted = [_extract_metrics(r) for r in rows]
    for m in METRICS:
        metric_vals[m.key] = [e.get(m.key) for e in extracted]

    keys = [m.key for m in METRICS]
    labels = {m.key: m.label for m in METRICS}

    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            kx, ky = keys[i], keys[j]
            xs: list[float] = []
            ys: list[float] = []
            for a, b in zip(metric_vals[kx], metric_vals[ky]):
                if a is None or b is None:
                    continue
                xs.append(float(a))
                ys.append(float(b))
            pr = _pearson(xs, ys)
            sr = _spearman(xs, ys)
            out.append(
                {
                    "scope": scope,
                    "year": "" if year is None else year,
                    "metric_x": kx,
                    "metric_y": ky,
                    "metric_x_label": labels[kx],
                    "metric_y_label": labels[ky],
                    "n": len(xs),
                    "pearson_r": "" if pr is None else f"{pr:.6f}",
                    "spearman_r": "" if sr is None else f"{sr:.6f}",
                }
            )
    return out


def main() -> int:
    p = argparse.ArgumentParser(description="Correlation analysis for MA ACS Tableau exports.")
    p.add_argument(
        "--input",
        default="census_ma_2024_tableau/out/ma_acs_2020_24_county_friendly.csv",
        help="Input CSV (friendly columns).",
    )
    p.add_argument("--out", default="census_ma_2024_tableau/out/ma_acs_correlations_long.csv")
    p.add_argument("--dataset", default="acs1", help="Filter to dataset=acs1 by default for comparability.")
    p.add_argument("--min-year", type=int, default=2021)
    p.add_argument("--max-year", type=int, default=2024)
    args = p.parse_args()

    with open(args.input, newline="", encoding="utf-8") as f:
        dr = csv.DictReader(f)
        data = list(dr)

    # Filter.
    filtered: list[dict[str, str]] = []
    for r in data:
        ds = (r.get("dataset") or "").strip()
        y = int((r.get("year") or "0").strip() or "0")
        if args.dataset and ds != args.dataset:
            continue
        if y < args.min_year or y > args.max_year:
            continue
        filtered.append(r)

    # Overall + by-year.
    out_rows: list[dict[str, Any]] = []
    out_rows.extend(_corr_rows(filtered, scope=f"{args.dataset}_{args.min_year}_{args.max_year}", year=None))

    years = sorted({int(r["year"]) for r in filtered if r.get("year")})
    for y in years:
        yr = [r for r in filtered if int(r["year"]) == y]
        out_rows.extend(_corr_rows(yr, scope=f"{args.dataset}_{args.min_year}_{args.max_year}", year=y))

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    fieldnames = [
        "scope",
        "year",
        "metric_x",
        "metric_y",
        "metric_x_label",
        "metric_y_label",
        "n",
        "pearson_r",
        "spearman_r",
    ]
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(out_rows)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

