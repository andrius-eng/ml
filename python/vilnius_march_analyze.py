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


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Vilnius monthly temperature anomalies")
    parser.add_argument("--month", type=int, default=3, help="Calendar month number (1-12, default: 3 for March)")
    parser.add_argument("--raw-input", type=str, default=None)
    parser.add_argument("--annual-output", type=str, default=None)
    parser.add_argument("--summary-output", type=str, default=None)
    parser.add_argument("--report-output", type=str, default=None)
    parser.add_argument("--execution-date", type=str, default=date.today().isoformat())
    parser.add_argument("--window-years", type=int, default=30)
    args = parser.parse_args()

    month_slug = calendar.month_name[args.month].lower()
    raw_input = args.raw_input or f"python/output/vilnius_{month_slug}/raw_daily_weather.csv"
    annual_output = args.annual_output or f"python/output/vilnius_{month_slug}/{month_slug}_temperature_anomalies.csv"
    summary_output = args.summary_output or f"python/output/vilnius_{month_slug}/summary.json"
    report_output = args.report_output or f"python/output/vilnius_{month_slug}/report.md"

    execution_date = date.fromisoformat(args.execution_date)
    cutoff_day = execution_date.day if execution_date.month == args.month else calendar.monthrange(execution_date.year, args.month)[1]
    start_year = execution_date.year - args.window_years + 1

    raw = pd.read_csv(raw_input)
    raw["time"] = pd.to_datetime(raw["time"])
    raw["year"] = raw["time"].dt.year
    raw["month"] = raw["time"].dt.month
    raw["day"] = raw["time"].dt.day

    month_data = raw[
        (raw["year"] >= start_year)
        & (raw["year"] <= execution_date.year)
        & (raw["month"] == args.month)
        & (raw["day"] <= cutoff_day)
    ].copy()

    annual = month_data.groupby("year", as_index=False).agg(
        mean_temp_c=("temperature_2m_mean", "mean"),
        days_observed=("time", "count"),
    )
    baseline_mean = float(annual["mean_temp_c"].mean())
    baseline_std = float(annual["mean_temp_c"].std(ddof=1))
    annual["anomaly_c"] = annual["mean_temp_c"] - baseline_mean
    annual["zscore"] = annual["anomaly_c"] / baseline_std if baseline_std else 0.0

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


if __name__ == "__main__":
    main()