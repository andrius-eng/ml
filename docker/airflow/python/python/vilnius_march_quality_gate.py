"""Validate Vilnius monthly anomaly artifacts before publishing results."""

from __future__ import annotations

import argparse
import calendar
import json
import sys

import pandas as pd


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Vilnius monthly anomaly outputs")
    parser.add_argument("--annual-input", type=str, default=None)
    parser.add_argument("--summary-input", type=str, default=None)
    parser.add_argument("--month", type=int, default=None, help="Calendar month number; if not set, inferred from summary JSON")
    parser.add_argument("--expected-years", type=int, default=30)
    parser.add_argument("--min-days", type=int, default=10)
    parser.add_argument("--max-abs-z", type=float, default=4.0)
    args = parser.parse_args()

    # Resolve summary path
    if args.summary_input:
        summary_input = args.summary_input
    else:
        month_for_default = args.month if args.month is not None else 3
        slug_for_default = calendar.month_name[month_for_default].lower()
        summary_input = f"python/output/vilnius_{slug_for_default}/summary.json"

    with open(summary_input, "r", encoding="utf-8") as handle:
        summary = json.load(handle)

    month = args.month if args.month is not None else summary.get("month", 3)
    month_name = summary.get("month_name", calendar.month_name[month])
    month_slug = month_name.lower()

    annual_input = args.annual_input or f"python/output/vilnius_{month_slug}/{month_slug}_temperature_anomalies.csv"

    annual = pd.read_csv(annual_input)
    years_included = int(summary["window"]["years_included"])
    min_days = int(annual["days_observed"].min())
    max_abs_z = float(annual["zscore"].abs().max())

    print(
        f"Vilnius {month_name} quality checks:",
        f"years={years_included}",
        f"min_days={min_days}",
        f"max_abs_z={max_abs_z:.3f}",
    )

    failure: str | None = None
    if years_included != args.expected_years:
        failure = f"Expected {args.expected_years} {month_name} rows, found {years_included}"
    elif min_days < args.min_days:
        failure = f"At least one {month_name} slice is too sparse: min days = {min_days}"
    elif max_abs_z > args.max_abs_z:
        failure = f"{month_name} anomaly z-score {max_abs_z:.3f} exceeds threshold {args.max_abs_z:.3f}"

    _log_quality_gate_to_mlflow(
        passed=(failure is None),
        month_name=month_name,
        years_included=years_included,
        min_days=min_days,
        max_abs_z=max_abs_z,
        annual=annual,
        failure_reason=failure,
    )

    if failure:
        raise SystemExit(failure)

    print(f"Vilnius {month_name} quality gate passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

def _log_quality_gate_to_mlflow(passed: bool, month_name: str, years_included: int,
                                  min_days: int, max_abs_z: float,
                                  annual, failure_reason=None) -> None:
    """Log Vilnius quality gate result to MLflow."""
    import os
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if not tracking_uri:
        return
    try:
        import mlflow
        month_slug = month_name.lower()
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("vilnius-temperature-analysis")
        with mlflow.start_run(
            run_name=f"vilnius-{month_slug}-quality-gate",
            tags={"type": "quality_gate", "dag": "vilnius_march_temperature", "month": month_slug},
        ):
            mlflow.log_metrics({
                "passed":          float(passed),
                "years_included":  float(years_included),
                "min_days":        float(min_days),
                "max_abs_z":       max_abs_z,
            })
            mlflow.log_table(data=annual.to_dict(orient="list"),
                             artifact_file=f"vilnius_{month_slug}_gate_dataset.json")
        print("[mlflow] vilnius quality gate logged")
    except Exception as exc:
        print(f"[mlflow] WARNING: failed to log: {exc}")
