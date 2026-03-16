"""Analyze Vilnius March temperature anomalies across the last 30 years."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import pandas as pd

from weather_common import ensure_parent


def render_report(summary: dict, annual: pd.DataFrame) -> str:
    hottest = annual.sort_values("anomaly_c", ascending=False).iloc[0]
    coldest = annual.sort_values("anomaly_c", ascending=True).iloc[0]
    latest = annual.sort_values("year").iloc[-1]

    lines = [
        "# Vilnius March Temperature Anomalies",
        "",
        f"Window: {summary['window']['start_year']} to {summary['window']['end_year']}",
        f"March day cutoff used for every year: 03-{summary['window']['cutoff_day']:02d}",
        f"Years analyzed: {summary['window']['years_included']}",
        "",
        "## Baseline",
        "",
        f"- Baseline mean March temperature: {summary['baseline']['mean_temp_c']:.2f} C",
        f"- Baseline standard deviation: {summary['baseline']['std_temp_c']:.2f} C",
        "",
        "## Latest Year",
        "",
        f"- Year: {int(latest['year'])}",
        f"- Mean temperature: {latest['mean_temp_c']:.2f} C",
        f"- Anomaly: {latest['anomaly_c']:.2f} C",
        f"- Z-score: {latest['zscore']:.2f}",
        f"- March days used: {int(latest['days_observed'])}",
        "",
        "## Extremes",
        "",
        f"- Warmest March slice: {int(hottest['year'])} ({hottest['anomaly_c']:.2f} C anomaly)",
        f"- Coldest March slice: {int(coldest['year'])} ({coldest['anomaly_c']:.2f} C anomaly)",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Vilnius March temperature anomalies")
    parser.add_argument(
        "--raw-input",
        type=str,
        default="python/output/vilnius_march/raw_daily_weather.csv",
    )
    parser.add_argument(
        "--annual-output",
        type=str,
        default="python/output/vilnius_march/march_temperature_anomalies.csv",
    )
    parser.add_argument(
        "--summary-output",
        type=str,
        default="python/output/vilnius_march/summary.json",
    )
    parser.add_argument(
        "--report-output",
        type=str,
        default="python/output/vilnius_march/report.md",
    )
    parser.add_argument("--execution-date", type=str, default=date.today().isoformat())
    parser.add_argument("--window-years", type=int, default=30)
    args = parser.parse_args()

    execution_date = date.fromisoformat(args.execution_date)
    cutoff_day = execution_date.day if execution_date.month == 3 else 31
    start_year = execution_date.year - args.window_years + 1

    raw = pd.read_csv(args.raw_input)
    raw["time"] = pd.to_datetime(raw["time"])
    raw["year"] = raw["time"].dt.year
    raw["month"] = raw["time"].dt.month
    raw["day"] = raw["time"].dt.day

    march = raw[
        (raw["year"] >= start_year)
        & (raw["year"] <= execution_date.year)
        & (raw["month"] == 3)
        & (raw["day"] <= cutoff_day)
    ].copy()

    annual = march.groupby("year", as_index=False).agg(
        mean_temp_c=("temperature_2m_mean", "mean"),
        days_observed=("time", "count"),
    )
    baseline_mean = float(annual["mean_temp_c"].mean())
    baseline_std = float(annual["mean_temp_c"].std(ddof=1))
    annual["anomaly_c"] = annual["mean_temp_c"] - baseline_mean
    annual["zscore"] = annual["anomaly_c"] / baseline_std if baseline_std else 0.0

    summary = {
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

    ensure_parent(args.annual_output)
    ensure_parent(args.summary_output)
    ensure_parent(args.report_output)
    annual.to_csv(args.annual_output, index=False)
    Path(args.summary_output).write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    Path(args.report_output).write_text(report, encoding="utf-8")

    print(json.dumps(summary, indent=2, sort_keys=True))
    print(f"Saved March anomaly table to {args.annual_output}")
    print(f"Saved March anomaly report to {args.report_output}")


if __name__ == "__main__":
    main()