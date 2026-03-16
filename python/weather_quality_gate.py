"""Fail when weather data coverage is too weak for a useful comparison."""

from __future__ import annotations

import argparse
import json
import sys

import pandas as pd


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate weather analysis outputs")
    parser.add_argument(
        "--summary-input",
        type=str,
        default="python/output/weather/ytd_summary.json",
    )
    parser.add_argument(
        "--country-monthly-input",
        type=str,
        default="python/output/weather/country_monthly_anomalies.csv",
    )
    parser.add_argument("--min-days", type=int, default=60)
    parser.add_argument("--min-month-days", type=int, default=5)
    parser.add_argument("--max-temp-abs-z", type=float, default=5.0)
    parser.add_argument("--max-precip-abs-z", type=float, default=5.0)
    parser.add_argument("--max-monthly-temp-abs-z", type=float, default=3.5)
    parser.add_argument("--max-monthly-precip-abs-z", type=float, default=3.5)
    args = parser.parse_args()

    with open(args.summary_input, "r", encoding="utf-8") as handle:
        summary = json.load(handle)
    monthly = pd.read_csv(args.country_monthly_input)

    days = int(summary["coverage"]["days_observed"])
    temp_z = float(summary["temperature"]["z_score_vs_baseline"])
    precip_z = float(summary["precipitation"]["z_score_vs_baseline"])

    weak_months = monthly[monthly["days"] < args.min_month_days]
    extreme_temp_months = monthly[monthly["temp_zscore"].abs() > args.max_monthly_temp_abs_z]
    extreme_precip_months = monthly[monthly["precip_zscore"].abs() > args.max_monthly_precip_abs_z]

    print(
        "Weather quality checks:",
        f"days={days}",
        f"temp_z={temp_z:.3f}",
        f"precip_z={precip_z:.3f}",
    )
    print(
        "Monthly checks:",
        f"min_days={args.min_month_days}",
        f"temp_threshold={args.max_monthly_temp_abs_z:.2f}",
        f"precip_threshold={args.max_monthly_precip_abs_z:.2f}",
    )

    if days < args.min_days:
        raise SystemExit(f"Only {days} days observed, expected at least {args.min_days}")
    if abs(temp_z) > args.max_temp_abs_z:
        raise SystemExit(f"Temperature z-score {temp_z:.3f} exceeds guardrail")
    if abs(precip_z) > args.max_precip_abs_z:
        raise SystemExit(f"Precipitation z-score {precip_z:.3f} exceeds guardrail")
    if not weak_months.empty:
        raise SystemExit(f"Found suspiciously sparse month rows: {weak_months[['month', 'days']].to_dict(orient='records')}")
    if not extreme_temp_months.empty:
        raise SystemExit(
            f"Monthly temperature anomalies exceeded threshold: {extreme_temp_months[['month', 'temp_zscore']].to_dict(orient='records')}"
        )
    if not extreme_precip_months.empty:
        raise SystemExit(
            f"Monthly precipitation anomalies exceeded threshold: {extreme_precip_months[['month', 'precip_zscore']].to_dict(orient='records')}"
        )

    print("Weather quality gate passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())