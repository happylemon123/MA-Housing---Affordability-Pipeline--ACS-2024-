#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
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


def main() -> int:
    ap = argparse.ArgumentParser(description="Box plots for affordability metrics across counties (by year).")
    ap.add_argument(
        "--input",
        default="out/countried related update.csv",
        help="Friendly county CSV",
    )
    ap.add_argument("--dataset", default="acs1", help="acs1 or acs5")
    ap.add_argument("--min-year", type=int, default=2021)
    ap.add_argument("--max-year", type=int, default=2024)
    ap.add_argument("--outdir", default="census_ma_2024_tableau/out/plots")
    args = ap.parse_args()

    os.environ.setdefault("MPLCONFIGDIR", str(Path("/tmp") / "mplconfig"))
    os.environ.setdefault("XDG_CACHE_HOME", str(Path("/tmp") / "xdg-cache"))
    os.environ.setdefault("FC_CACHEDIR", str(Path("/tmp") / "fontconfig-cache"))
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    path = Path(args.input)
    rows: list[dict[str, str]] = []
    with path.open(newline="", encoding="utf-8") as f:
        dr = csv.DictReader(f)
        for r in dr:
            if (r.get("dataset") or "").strip() != args.dataset:
                continue
            y = int((r.get("year") or "0").strip() or "0")
            if y < args.min_year or y > args.max_year:
                continue
            rows.append(r)

    years = sorted({int(r["year"]) for r in rows})
    if not years:
        raise SystemExit("No rows after filtering (check dataset/year).")

    def series(col: str) -> list[list[float]]:
        out = []
        for y in years:
            vals = [fnum(r.get(col)) for r in rows if int(r["year"]) == y]
            if 'burden' in col:
                out.append([v * 100.0 for v in vals if v is not None])
            else:
                out.append([v for v in vals if v is not None])
        return out

    metrics = [
        ("rent_pressure", "Rent pressure (median gross rent / affordable @30% income)"),
        ("owner_cost_pressure_with_mortgage", "Owner-cost pressure (with mortgage / affordable @30% income)"),
        ("rent_burden_30p", "Share of renters paying >=30% of income (B25070)"),
        ("rent_burden_50p", "Share of renters paying >=50% of income (B25070)"),
        ("mortgage_burden_30p", "Share of mortgaged owners paying >=30% of income (B25091)"),
        ("mortgage_burden_50p", "Share of mortgaged owners paying >=50% of income (B25091)"),
    ]

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    for col, title in metrics:
        data = series(col)
        plt.figure(figsize=(10, 6))
        plt.boxplot(data, tick_labels=[str(y) for y in years], showfliers=False)
        plt.title(f"MA counties ({args.dataset}): {title}")
        plt.xlabel("Year")
        plt.ylabel(col)
        plt.tight_layout()
        out_path = outdir / f"box_{col}.png"
        plt.savefig(out_path, dpi=180)
        plt.close()
        print("wrote", out_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
