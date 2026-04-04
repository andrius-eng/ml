"""Analyze Vilnius monthly temperature anomalies across the last N years."""

from __future__ import annotations

import argparse
import calendar
import json
from datetime import date
from pathlib import Path

import pandas as pd

from weather_common import ensure_parent


def render_report(summary: dict, annual: pd.DataFrame) -> str:
    month_name = summary.get("month_name", "March")
    month_num = summary.get("month", 3)
    hottest = annual.sort_values("anomaly_c", ascending=False).iloc[0]
    coldest = annual.sort_values("anomaly_c", ascending=True).iloc[0]
    latest = annual.sort_values("year").iloc[-1]

    lines = [
        f"# Vilnius {month_name} Temperature Anomalies",
        "",
        f"Window: {summary['window']['start_year']} to {summary['window']['end_year']}",
        f"{month_name} day cutoff used for every year: {month_num:02d}-{summary['window']['cutoff_day']:02d}",
        f"Years analyzed: {summary['window']['years_included']}",
        "",
        "## Baseline",
        "",
        f"- Baseline mean {month_name} temperature: {summary['baseline']['mean_temp_c']:.2f} C",
        f"- Baseline standard deviation: {summary['baseline']['std_temp_c']:.2f} C",
        "",
        "## Latest Year",
        "",
        f"- Year: {int(latest['year'])}",
        f"- Mean temperature: {latest['mean_temp_c']:.2f} C",
        f"- Anomaly: {latest['anomaly_c']:.2f} C",
        f"- Z-score: {latest['zscore']:.2f}",
        f"- {month_name} days used: {int(latest['days_observed'])}",
        "",
        "## Extremes",
        "",
        f"- Warmest {month_name} slice: {int(hottest['year'])} ({hottest['anomaly_c']:.2f} C anomaly)",
        f"- Coldest {month_name} slice: {int(coldest['year'])} ({coldest['anomaly_c']:.2f} C anomaly)",
    ]
    return "\n".join(lines)


def _log_to_mlflow(summary: dict, annual) -> None:
    """Log Vilnius monthly temperature analysis results to MLflow."""
    import os, math, sys
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if not tracking_uri:
        return
    try:
        import mlflow
        month_name = summary.get("month_name", "month").lower()
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("vilnius-temperature-analysis")
        years_included = int(summary["window"]["years_included"])
        run_tags = {
            "type": "temperature_analysis",
            "dag": "vilnius_march_temperature",
            "month": month_name,
        }
        if years_included < 5:
            run_tags["data_quality_warning"] = f"only {years_included} year(s) — archive API may be rate-limited"
        _parent_run_id = os.environ.get("MLFLOW_PARENT_RUN_ID", "")
        if _parent_run_id:
            run_tags["mlflow.parentRunId"] = _parent_run_id
        with mlflow.start_run(run_name=f"vilnius-{month_name}-analyze", tags=run_tags):
            latest_row = annual[annual["year"] == annual["year"].max()].iloc[0]
            def _safe(v):
                f = float(v)
                return 0.0 if math.isnan(f) or math.isinf(f) else f
            mlflow.log_metrics({
                "latest_mean_temp_c":   _safe(latest_row["mean_temp_c"]),
                "latest_anomaly_c":     _safe(latest_row["anomaly_c"]),
                "latest_zscore":        _safe(latest_row["zscore"]),
                "baseline_mean_temp_c": _safe(summary["baseline"]["mean_temp_c"]),
                "baseline_std_temp_c":  _safe(summary["baseline"]["std_temp_c"]),
                "years_included":       float(years_included),
            })
            mlflow.log_table(data=annual.to_dict(orient="list"),
                             artifact_file=f"vilnius_{month_name}_annual_dataset.json")
        print("[mlflow] vilnius temperature analysis logged")
        sys.stdout.flush()
    except Exception as exc:
        print(f"[mlflow] WARNING: failed to log: {exc}")
        sys.stdout.flush()

def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Vilnius monthly temperature anomalies")
    parser.add_argument("--month", type=int, default=3, help="Calendar month number (1-12, default: 3 for March)")
    parser.add_argument("--raw-input", type=str, default=None)
    parser.add_argument("--annual-output", type=str, default=None)
    parser.add_argument("--summary-output", type=str, default=None)
    parser.add_argument("--report-output", type=str, default=None)
    parser.add_argument("--execution-date", type=str, default=date.today().isoformat())
    parser.add_argument("--window-years", type=int, default=30)
    parser.add_argument("--require-flink", action="store_true", help="Fail instead of falling back to DirectRunner")
    args = parser.parse_args()

    month_slug = calendar.month_name[args.month].lower()
    raw_input = args.raw_input or f"python/output/vilnius_{month_slug}/raw_daily_weather.csv"
    annual_output = args.annual_output or f"python/output/vilnius_{month_slug}/{month_slug}_temperature_anomalies.csv"
    summary_output = args.summary_output or f"python/output/vilnius_{month_slug}/summary.json"
    report_output = args.report_output or f"python/output/vilnius_{month_slug}/report.md"

    execution_date = date.fromisoformat(args.execution_date)
    cutoff_day = execution_date.day if execution_date.month == args.month else calendar.monthrange(execution_date.year, args.month)[1]
    start_year = execution_date.year - args.window_years + 1

    # ── Use Beam pipeline for windowed aggregation ──────────────────────────
    # Uses CalendarMonthWindowFn + CombinePerKey for monthly means and baseline
    # stats. Mirrors the weather DAG: PortableRunner → Flink, fallback to Direct.
    import beam_analysis

    VILNIUS_COORDS = (54.6872, 25.2797)

    beam_output_dir = str(Path(raw_input).parent / "_beam_tmp")

    portable_args = [
        "--job_endpoint", "beam-job-server:8099",
        "--artifact_endpoint", "beam-job-server:8098",
        "--environment_type", "EXTERNAL",
        "--environment_config", "localhost:50000",
        "--parallelism", "1",
    ]

    try:
        print("[beam] Attempting PortableRunner → Flink...")
        beam_analysis.run(
            start_date=f"{start_year}-01-01",
            end_date=execution_date.isoformat(),
            output_dir=beam_output_dir,
            cities={"Vilnius": VILNIUS_COORDS},
            input_csv=raw_input,
            fetch_missing_cities=False,
            runner="PortableRunner",
            beam_args=portable_args,
        )
        print("[beam] PortableRunner succeeded")
    except Exception as exc:
        if args.require_flink:
            raise RuntimeError(f"PortableRunner failed and --require-flink is set: {exc}") from exc
        print(f"[beam] PortableRunner failed ({exc}); falling back to DirectRunner")
        beam_analysis.run(
            start_date=f"{start_year}-01-01",
            end_date=execution_date.isoformat(),
            output_dir=beam_output_dir,
            cities={"Vilnius": VILNIUS_COORDS},
            input_csv=raw_input,
            fetch_missing_cities=False,
            runner="DirectRunner",
        )

    beam_csv = Path(beam_output_dir) / "monthly_anomaly_matrix.csv"
    beam_df = pd.read_csv(beam_csv)

    # Filter to the target month, applying the cutoff-day constraint for the
    # current execution year (Beam already filtered via CalendarMonthWindowFn,
    # but the current month may be partial — drop the current year if the month
    # hasn't reached cutoff_day worth of data yet).
    annual = beam_df[
        (beam_df["city"] == "Vilnius")
        & (beam_df["month"] == args.month)
    ].copy()

    # Rename Beam columns → downstream schema
    annual = annual.rename(columns={
        "mean_temp": "mean_temp_c",
        "days":      "days_observed",
        "anomaly":   "anomaly_c",
        "z_score":   "zscore",
    })[["year", "mean_temp_c", "days_observed", "anomaly_c", "zscore"]].sort_values("year").reset_index(drop=True)

    # Derive summary stats from the Beam baseline columns (already computed)
    baseline_row = beam_df[
        (beam_df["city"] == "Vilnius") & (beam_df["month"] == args.month)
    ].iloc[0] if len(beam_df) > 0 else None
    baseline_mean = float(baseline_row["baseline_mean"]) if baseline_row is not None else float(annual["mean_temp_c"].mean())
    baseline_std  = float(baseline_row["baseline_std"])  if baseline_row is not None else 0.0

    summary = {
        "month": args.month,
        "month_name": calendar.month_name[args.month],
        "window": {
            "start_year": int(start_year),
            "end_year": int(execution_date.year),
            "years_included": int(len(annual)),
            "cutoff_day": int(cutoff_day),
            "execution_date": execution_date.isoformat(),
        },
        "baseline": {
            "mean_temp_c": baseline_mean,
            "std_temp_c": baseline_std,
        },
        "latest_year": int(annual["year"].max()),
    }

    report = render_report(summary, annual)

    ensure_parent(annual_output)
    ensure_parent(summary_output)
    ensure_parent(report_output)
    annual.to_csv(annual_output, index=False)
    Path(summary_output).write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    Path(report_output).write_text(report, encoding="utf-8")

    print(json.dumps(summary, indent=2, sort_keys=True))
    month_name = calendar.month_name[args.month]
    print(f"Saved {month_name} anomaly table to {annual_output}")
    print(f"Saved {month_name} anomaly report to {report_output}")
    _log_to_mlflow(summary, annual)


if __name__ == "__main__":
    main()

