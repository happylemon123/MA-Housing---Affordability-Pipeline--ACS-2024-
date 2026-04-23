#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
from dataclasses import dataclass
from pathlib import Path


def fnum(x: str | None) -> float | None:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def safe_div(n: float | None, d: float | None) -> float | None:
    if n is None or d is None or d == 0:
        return None
    return n / d


def short_county_name(name: str) -> str:
    s = (name or "").strip().strip('"')
    s = s.replace(" County, Massachusetts", "")
    s = s.replace(" County, MA", "")
    s = s.replace(", Massachusetts", "")
    s = s.replace(", MA", "")
    return s


@dataclass(frozen=True)
class Row:
    year: int
    county: str
    geoid: str
    poverty_rate: float | None
    median_hh_income: float | None
    median_gross_rent: float | None
    affordable_rent_30pct: float | None
    rent_pressure: float | None
    median_owner_cost_with_mortgage: float | None
    owner_cost_pressure_with_mortgage: float | None


def load_rows(path: Path, *, dataset: str, min_year: int, max_year: int) -> list[Row]:
    out: list[Row] = []
    with path.open(newline="", encoding="utf-8") as f:
        dr = csv.DictReader(f)
        for r in dr:
            ds = (r.get("dataset") or "").strip()
            if ds != dataset:
                continue
            y = int((r.get("year") or "0").strip() or "0")
            if y < min_year or y > max_year:
                continue

            pr = fnum(r.get("poverty_rate"))
            inc = fnum(r.get("median_household_income_est"))
            rent = fnum(r.get("median_gross_rent_est"))
            owner_cost_mort = fnum(r.get("median_owner_cost_with_mortgage_est"))
            affordable = None if inc is None else (0.30 * inc / 12.0)
            pressure = safe_div(rent, affordable)
            owner_pressure = safe_div(owner_cost_mort, affordable)

            out.append(
                Row(
                    year=y,
                    county=short_county_name(str(r.get("county_name") or r.get("NAME") or "").strip()),
                    geoid=str(r.get("geoid") or r.get("GEOID") or "").strip(),
                    poverty_rate=pr,
                    median_hh_income=inc,
                    median_gross_rent=rent,
                    affordable_rent_30pct=affordable,
                    rent_pressure=pressure,
                    median_owner_cost_with_mortgage=owner_cost_mort,
                    owner_cost_pressure_with_mortgage=owner_pressure,
                )
            )
    return out


def percentile(vals: list[float], p: float) -> float:
    # p in [0,100]
    if not vals:
        raise ValueError("empty vals")
    s = sorted(vals)
    k = (len(s) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    d = k - f
    return s[f] * (1 - d) + s[c] * d


def categorize(rows: list[Row]) -> tuple[float, float, dict[str, str]]:
    prs = [r.poverty_rate for r in rows if r.poverty_rate is not None]
    rps = [r.rent_pressure for r in rows if r.rent_pressure is not None]
    pr_cut = percentile([float(x) for x in prs], 60.0)  # slightly "high poverty"
    rp_cut = percentile([float(x) for x in rps], 60.0)  # slightly "high pressure"

    labels: dict[str, str] = {}
    for r in rows:
        key = f"{r.year}:{r.geoid}"
        pr = r.poverty_rate
        rp = r.rent_pressure
        if pr is None or rp is None:
            labels[key] = "unknown"
        elif rp >= rp_cut and pr < pr_cut:
            labels[key] = "price-driven"
        elif rp < rp_cut and pr >= pr_cut:
            labels[key] = "income-driven"
        elif rp >= rp_cut and pr >= pr_cut:
            labels[key] = "both-high"
        else:
            labels[key] = "both-low"
    return pr_cut, rp_cut, labels


def main() -> int:
    ap = argparse.ArgumentParser(description="Make a simple price-driven vs income-driven demo (MA counties).")
    ap.add_argument(
        "--input",
        default="census_ma_2024_tableau/out/ma_acs_2020_24_county_friendly.csv",
        help="Input friendly county CSV",
    )
    ap.add_argument("--outdir", default="census_ma_2024_tableau/out/plots")
    ap.add_argument("--dataset", default="acs1")
    ap.add_argument("--min-year", type=int, default=2021)
    ap.add_argument("--max-year", type=int, default=2024)
    ap.add_argument(
        "--label",
        default="latest",
        choices=["none", "latest", "all"],
        help="Label counties on plots (latest=only latest year to reduce clutter).",
    )
    args = ap.parse_args()

    # Avoid matplotlib cache issues on locked-down home dirs.
    os.environ.setdefault("MPLCONFIGDIR", str(Path("/tmp") / "mplconfig"))
    os.environ.setdefault("XDG_CACHE_HOME", str(Path("/tmp") / "xdg-cache"))
    os.environ.setdefault("FC_CACHEDIR", str(Path("/tmp") / "fontconfig-cache"))

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    rows = load_rows(Path(args.input), dataset=args.dataset, min_year=args.min_year, max_year=args.max_year)
    if not rows:
        raise SystemExit("No rows loaded (check dataset/year filters).")

    pr_cut, rp_cut, labels = categorize(rows)
    latest_year = max(r.year for r in rows)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Scatter: rent pressure vs poverty rate, colored by category.
    colors = {
        "price-driven": "#1b9e77",
        "income-driven": "#d95f02",
        "both-high": "#7570b3",
        "both-low": "#999999",
        "unknown": "#cccccc",
    }

    xs, ys, cs = [], [], []
    for r in rows:
        if r.rent_pressure is None or r.poverty_rate is None:
            continue
        key = f"{r.year}:{r.geoid}"
        cat = labels.get(key, "unknown")
        xs.append(r.rent_pressure)
        ys.append(r.poverty_rate)
        cs.append(colors.get(cat, "#cccccc"))

    plt.figure(figsize=(10, 6))
    plt.scatter(xs, ys, c=cs, alpha=0.85, edgecolors="none")
    plt.axhline(pr_cut, color="#444444", linewidth=1.0, linestyle="--")
    plt.axvline(rp_cut, color="#444444", linewidth=1.0, linestyle="--")
    plt.title("MA counties (ACS1 2021–2024): price-driven vs income-driven (proxy)")
    plt.xlabel("Rent pressure = median_gross_rent / (0.30 * median_income / 12)")
    plt.ylabel("Poverty rate")
    if args.label != "none":
        for r in rows:
            if r.rent_pressure is None or r.poverty_rate is None:
                continue
            if args.label == "latest" and r.year != latest_year:
                continue
            plt.annotate(
                r.county,
                (r.rent_pressure, r.poverty_rate),
                xytext=(3, 3),
                textcoords="offset points",
                fontsize=7,
                alpha=0.9,
            )
    plt.tight_layout()
    scatter_path = outdir / "ma_price_vs_income_quadrants.png"
    plt.savefig(scatter_path, dpi=180)
    plt.close()

    # Scatter: owner cost pressure (with mortgage) vs poverty rate.
    xs, ys, cs = [], [], []
    for r in rows:
        if r.owner_cost_pressure_with_mortgage is None or r.poverty_rate is None:
            continue
        key = f"{r.year}:{r.geoid}"
        cat = labels.get(key, "unknown")
        xs.append(r.owner_cost_pressure_with_mortgage)
        ys.append(r.poverty_rate)
        cs.append(colors.get(cat, "#cccccc"))

    plt.figure(figsize=(10, 6))
    plt.scatter(xs, ys, c=cs, alpha=0.85, edgecolors="none")
    plt.axhline(pr_cut, color="#444444", linewidth=1.0, linestyle="--")
    plt.title("MA counties (ACS1 2021–2024): owner-cost pressure (with mortgage) vs poverty (proxy)")
    plt.xlabel("Owner-cost pressure = median_owner_cost_with_mortgage / (0.30 * median_income / 12)")
    plt.ylabel("Poverty rate")
    if args.label != "none":
        for r in rows:
            if r.owner_cost_pressure_with_mortgage is None or r.poverty_rate is None:
                continue
            if args.label == "latest" and r.year != latest_year:
                continue
            plt.annotate(
                r.county,
                (r.owner_cost_pressure_with_mortgage, r.poverty_rate),
                xytext=(3, 3),
                textcoords="offset points",
                fontsize=7,
                alpha=0.9,
            )
    plt.tight_layout()
    owner_scatter_path = outdir / "ma_owner_cost_pressure_vs_poverty.png"
    plt.savefig(owner_scatter_path, dpi=180)
    plt.close()

    # Proof table: pick examples from latest year.
    latest = max(r.year for r in rows)
    latest_rows = [r for r in rows if r.year == latest and r.rent_pressure is not None and r.poverty_rate is not None]
    latest_rows.sort(key=lambda r: (r.rent_pressure or 0.0), reverse=True)

    def pick(cat: str) -> list[Row]:
        picked = []
        for r in latest_rows:
            k = f"{r.year}:{r.geoid}"
            if labels.get(k) == cat:
                picked.append(r)
            if len(picked) >= 3:
                break
        return picked

    examples = []
    for cat in ["price-driven", "income-driven", "both-high", "both-low"]:
        examples.extend(pick(cat))

    table_path = Path(args.outdir) / "ma_price_income_examples.csv"
    with table_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "year",
                "category",
                "county_name",
                "geoid",
                "poverty_rate",
                "median_household_income_est",
                "median_gross_rent_est",
                "median_owner_cost_with_mortgage_est",
                "affordable_rent_30pct",
                "rent_pressure",
                "owner_cost_pressure_with_mortgage",
            ],
        )
        w.writeheader()
        for r in examples:
            k = f"{r.year}:{r.geoid}"
            w.writerow(
                {
                    "year": r.year,
                    "category": labels.get(k, "unknown"),
                    "county_name": r.county,
                    "geoid": r.geoid,
                    "poverty_rate": "" if r.poverty_rate is None else f"{r.poverty_rate:.6f}",
                    "median_household_income_est": "" if r.median_hh_income is None else f"{r.median_hh_income:.0f}",
                    "median_gross_rent_est": "" if r.median_gross_rent is None else f"{r.median_gross_rent:.0f}",
                    "median_owner_cost_with_mortgage_est": ""
                    if r.median_owner_cost_with_mortgage is None
                    else f"{r.median_owner_cost_with_mortgage:.0f}",
                    "affordable_rent_30pct": "" if r.affordable_rent_30pct is None else f"{r.affordable_rent_30pct:.2f}",
                    "rent_pressure": "" if r.rent_pressure is None else f"{r.rent_pressure:.6f}",
                    "owner_cost_pressure_with_mortgage": ""
                    if r.owner_cost_pressure_with_mortgage is None
                    else f"{r.owner_cost_pressure_with_mortgage:.6f}",
                }
            )

    print("wrote", scatter_path)
    print("wrote", owner_scatter_path)
    print("wrote", table_path)
    print("cuts", f"poverty_rate_p60={pr_cut:.6f}", f"rent_pressure_p60={rp_cut:.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
