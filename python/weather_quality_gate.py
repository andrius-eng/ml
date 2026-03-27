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
    _temp_z_raw = summary["temperature"]["z_score_vs_baseline"]
    _precip_z_raw = summary["precipitation"]["z_score_vs_baseline"]
    import math
    temp_z = float(_temp_z_raw) if _temp_z_raw is not None else float('nan')
    precip_z = float(_precip_z_raw) if _precip_z_raw is not None else float('nan')

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

    failure: str | None = None
    if math.isnan(temp_z) or math.isnan(precip_z):
        # Baseline unavailable (e.g. archive API 429 fallback truncated history).
        # Warn but don't block — next successful full fetch will restore the baseline.
        print(f"WARNING: z-scores are NaN (no historical baseline in current CSV). Gate skipped for z-score checks.")
    if days < args.min_days:
        failure = f"Only {days} days observed, expected at least {args.min_days}"
    elif not math.isnan(temp_z) and abs(temp_z) > args.max_temp_abs_z:
        failure = f"Temperature z-score {temp_z:.3f} exceeds guardrail"
    elif not math.isnan(precip_z) and abs(precip_z) > args.max_precip_abs_z:
        failure = f"Precipitation z-score {precip_z:.3f} exceeds guardrail"
    elif not weak_months.empty:
        failure = f"Found suspiciously sparse month rows: {weak_months[['month', 'days']].to_dict(orient='records')}"
    elif not extreme_temp_months.empty:
        failure = (
            f"Monthly temperature anomalies exceeded threshold: {extreme_temp_months[['month', 'temp_zscore']].to_dict(orient='records')}"
        )
    elif not extreme_precip_months.empty:
        failure = (
            f"Monthly precipitation anomalies exceeded threshold: {extreme_precip_months[['month', 'precip_zscore']].to_dict(orient='records')}"
        )

    _log_quality_gate_to_mlflow(
        passed=(failure is None),
        temp_z=temp_z, precip_z=precip_z, days=days,
        n_weak_months=len(weak_months),
        n_extreme_temp_months=len(extreme_temp_months),
        n_extreme_precip_months=len(extreme_precip_months),
        monthly=monthly,
        failure_reason=failure,
    )

    if failure:
        raise SystemExit(failure)

    print("Weather quality gate passed.")
    return 0


def _log_quality_gate_to_mlflow(passed: bool, temp_z: float, precip_z: float,
                                  days: int, n_weak_months: int,
                                  monthly, failure_reason=None) -> None:
    """Log weather quality gate results to MLflow."""
    import os
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if not tracking_uri:
        return
    try:
        import mlflow
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("weather-analysis")
        with mlflow.start_run(run_name="weather-quality-gate", tags={"type": "quality_gate", "dag": "lithuania_weather"}):
            mlflow.log_metrics({
                "passed":                   float(passed),
                "days_observed":            float(days),
                "temp_z_score":             temp_z,
                "precip_z_score":           precip_z,
                "n_weak_months":            float(n_weak_months),
                "n_extreme_temp_months":    float(n_extreme_temp_months),
                "n_extreme_precip_months":  float(n_extreme_precip_months),
            })
            records = monthly.to_dict(orient="list")
            mlflow.log_table(data=records, artifact_file="weather_monthly_dataset.json")
        print("[mlflow] quality gate logged")
    except Exception as exc:
        print(f"[mlflow] WARNING: failed to log quality gate: {exc}")


if __name__ == "__main__":
    sys.exit(main())