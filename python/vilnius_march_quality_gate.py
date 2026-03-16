"""Validate Vilnius March anomaly artifacts before publishing results."""

from __future__ import annotations

import argparse
import json
import sys

import pandas as pd


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Vilnius March anomaly outputs")
    parser.add_argument(
        "--annual-input",
        type=str,
        default="python/output/vilnius_march/march_temperature_anomalies.csv",
    )
    parser.add_argument(
        "--summary-input",
        type=str,
        default="python/output/vilnius_march/summary.json",
    )
    parser.add_argument("--expected-years", type=int, default=30)
    parser.add_argument("--min-days", type=int, default=10)
    parser.add_argument("--max-abs-z", type=float, default=4.0)
    args = parser.parse_args()

    annual = pd.read_csv(args.annual_input)
    with open(args.summary_input, "r", encoding="utf-8") as handle:
        summary = json.load(handle)

    years_included = int(summary["window"]["years_included"])
    min_days = int(annual["days_observed"].min())
    max_abs_z = float(annual["zscore"].abs().max())

    print(
        "Vilnius March quality checks:",
        f"years={years_included}",
        f"min_days={min_days}",
        f"max_abs_z={max_abs_z:.3f}",
    )

    if years_included != args.expected_years:
        raise SystemExit(f"Expected {args.expected_years} March rows, found {years_included}")
    if min_days < args.min_days:
        raise SystemExit(f"At least one March slice is too sparse: min days = {min_days}")
    if max_abs_z > args.max_abs_z:
        raise SystemExit(f"March anomaly z-score {max_abs_z:.3f} exceeds threshold {args.max_abs_z:.3f}")

    print("Vilnius March quality gate passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())