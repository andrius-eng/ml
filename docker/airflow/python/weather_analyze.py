"""Analyze current-year Lithuania weather against historical expectations."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import pandas as pd

from weather_common import (
    apply_daily_climatology,
    build_annual_summary,
    build_city_annual_summary,
    build_city_daily,
    build_city_rankings,
    build_country_daily,
    build_daily_climatology,
    build_heat_stress_summary,
    build_monthly_anomalies,
    compute_city_weather_summary,
    compute_weather_summary,
    attach_current_anomaly_metrics,
    ensure_parent,
)


def render_markdown_report(summary: dict, city_summaries: list[dict]) -> str:
    hottest_recent = max(city_summaries, key=lambda item: item["temperature"]["latest_7d_anomaly"])
    driest_city = min(city_summaries, key=lambda item: item["precipitation"]["deviation_vs_1991_2020_mean"])
    lines = [
        "# Lithuania Weather Summary",
        "",
        f"Coverage: {summary['coverage']['period']}",
        f"Days observed: {summary['coverage']['days_observed']}",
        "",
        "## Country-level anomaly",
        "",
        f"- YTD mean temperature: {summary['current']['ytd_mean_temp']:.2f} C",
        f"- Temperature deviation vs 1991-2020 mean: {summary['temperature']['deviation_vs_1991_2020_mean']:.2f} C",
        f"- Temperature z-score: {summary['temperature']['z_score_vs_baseline']:.2f}",
        f"- Latest 7-day temperature anomaly: {summary['temperature']['latest_7d_anomaly']:.2f} C",
        f"- YTD precipitation: {summary['current']['ytd_total_precip']:.2f} mm",
        f"- Precipitation deviation vs 1991-2020 mean: {summary['precipitation']['deviation_vs_1991_2020_mean']:.2f} mm",
        f"- Precipitation z-score: {summary['precipitation']['z_score_vs_baseline']:.2f}",
        f"- Latest cumulative precipitation anomaly: {summary['precipitation']['latest_cumulative_anomaly']:.2f} mm",
        "",
        "## Highlights",
        "",
        f"- Strongest recent warm-up: {hottest_recent['city']} ({hottest_recent['temperature']['latest_7d_anomaly']:.2f} C over the latest 7 days)",
        f"- Largest precipitation deficit: {driest_city['city']} ({driest_city['precipitation']['deviation_vs_1991_2020_mean']:.2f} mm)",
        "",
        "## City-level anomaly",
        "",
    ]

    for city_summary in city_summaries:
        lines.extend(
            [
                f"### {city_summary['city']}",
                f"- Mean temperature: {city_summary['current']['ytd_mean_temp']:.2f} C",
                f"- Temperature anomaly: {city_summary['temperature']['deviation_vs_1991_2020_mean']:.2f} C",
                f"- Latest 7-day temperature anomaly: {city_summary['temperature']['latest_7d_anomaly']:.2f} C",
                f"- Precipitation: {city_summary['current']['ytd_total_precip']:.2f} mm",
                f"- Precipitation anomaly: {city_summary['precipitation']['deviation_vs_1991_2020_mean']:.2f} mm",
                f"- Latest cumulative precipitation anomaly: {city_summary['precipitation']['latest_cumulative_anomaly']:.2f} mm",
                "",
            ]
        )

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Lithuania weather anomalies")
    parser.add_argument(
        "--raw-input",
        type=str,
        default="python/output/weather/raw_daily_weather.csv",
        help="Raw city-level weather CSV",
    )
    parser.add_argument(
        "--country-daily-output",
        type=str,
        default="python/output/weather/country_daily_weather.csv",
        help="CSV output for Lithuania proxy country-level daily series",
    )
    parser.add_argument(
        "--annual-output",
        type=str,
        default="python/output/weather/annual_summary.csv",
        help="CSV output for annual YTD summaries",
    )
    parser.add_argument(
        "--summary-output",
        type=str,
        default="python/output/weather/ytd_summary.json",
        help="JSON output for current-year weather anomaly summary",
    )
    parser.add_argument(
        "--city-annual-output",
        type=str,
        default="python/output/weather/city_annual_summary.csv",
        help="CSV output for city-level annual YTD summaries",
    )
    parser.add_argument(
        "--city-summary-output",
        type=str,
        default="python/output/weather/city_ytd_summary.json",
        help="JSON output for city-level weather anomaly summaries",
    )
    parser.add_argument(
        "--report-output",
        type=str,
        default="python/output/weather/weather_summary.md",
        help="Markdown report summarizing country and city anomalies",
    )
    parser.add_argument(
        "--country-daily-anomalies-output",
        type=str,
        default="python/output/weather/country_daily_anomalies.csv",
        help="CSV output for current-year country daily climatology comparison",
    )
    parser.add_argument(
        "--city-daily-anomalies-output",
        type=str,
        default="python/output/weather/city_daily_anomalies.csv",
        help="CSV output for current-year city daily climatology comparison",
    )
    parser.add_argument(
        "--country-monthly-output",
        type=str,
        default="python/output/weather/country_monthly_anomalies.csv",
        help="CSV output for current-year country monthly anomalies",
    )
    parser.add_argument(
        "--city-monthly-output",
        type=str,
        default="python/output/weather/city_monthly_anomalies.csv",
        help="CSV output for current-year city monthly anomalies",
    )
    parser.add_argument(
        "--city-rankings-output",
        type=str,
        default="python/output/weather/city_rankings.json",
        help="JSON output ranking cities by deviation severity",
    )
    parser.add_argument("--current-year", type=int, default=None)
    parser.add_argument("--current-end", type=str, default=date.today().isoformat())
    parser.add_argument(
        "--heat-stress-output",
        type=str,
        default="python/output/weather/heat_stress.json",
        help="JSON output for frost/heat day counts vs 1991-2020 baseline",
    )
    args = parser.parse_args()

    raw = pd.read_csv(args.raw_input)
    current_end = date.fromisoformat(args.current_end)
    current_year = args.current_year or current_end.year
    country_daily = build_country_daily(raw, current_end)
    city_daily = build_city_daily(raw, current_end)
    country_climatology = build_daily_climatology(country_daily)
    city_climatology = build_daily_climatology(city_daily, group_cols=["city"])
    current_country_daily = apply_daily_climatology(
        country_daily[country_daily["year"] == current_year].copy(),
        country_climatology,
    )
    current_city_daily = apply_daily_climatology(
        city_daily[city_daily["year"] == current_year].copy(),
        city_climatology,
        group_cols=["city"],
    )
    annual = build_annual_summary(country_daily)
    city_annual = build_city_annual_summary(city_daily)
    summary = attach_current_anomaly_metrics(
        compute_weather_summary(annual, current_year=current_year),
        current_country_daily,
    )
    city_summaries = compute_city_weather_summary(city_annual, current_year=current_year)
    city_summaries = [
        attach_current_anomaly_metrics(summary_item, current_city_daily[current_city_daily["city"] == summary_item["city"]].copy())
        for summary_item in city_summaries
    ]
    country_monthly = build_monthly_anomalies(country_daily, current_year=current_year)
    city_monthly = build_monthly_anomalies(city_daily, group_cols=["city"], current_year=current_year)
    city_rankings = build_city_rankings(city_summaries)
    summary["coverage"]["period"] = f"01-01 to {current_end.strftime('%m-%d')}"
    for city_summary in city_summaries:
        city_summary["coverage"]["period"] = summary["coverage"]["period"]

    report = render_markdown_report(summary, city_summaries)

    ensure_parent(args.country_daily_output)
    ensure_parent(args.annual_output)
    ensure_parent(args.summary_output)
    ensure_parent(args.city_annual_output)
    ensure_parent(args.city_summary_output)
    ensure_parent(args.report_output)
    ensure_parent(args.country_daily_anomalies_output)
    ensure_parent(args.city_daily_anomalies_output)
    ensure_parent(args.country_monthly_output)
    ensure_parent(args.city_monthly_output)
    ensure_parent(args.city_rankings_output)
    country_daily.to_csv(args.country_daily_output, index=False)
    annual.to_csv(args.annual_output, index=False)
    city_annual.to_csv(args.city_annual_output, index=False)
    current_country_daily.to_csv(args.country_daily_anomalies_output, index=False)
    current_city_daily.to_csv(args.city_daily_anomalies_output, index=False)
    country_monthly.to_csv(args.country_monthly_output, index=False)
    city_monthly.to_csv(args.city_monthly_output, index=False)
    with open(args.summary_output, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
    with open(args.city_summary_output, "w", encoding="utf-8") as handle:
        json.dump(city_summaries, handle, indent=2, sort_keys=True)
    with open(args.city_rankings_output, "w", encoding="utf-8") as handle:
        json.dump(city_rankings, handle, indent=2, sort_keys=True)
    Path(args.report_output).write_text(report, encoding="utf-8")

    heat_stress = build_heat_stress_summary(raw, current_year, current_end)
    if heat_stress:
        ensure_parent(args.heat_stress_output)
        with open(args.heat_stress_output, "w", encoding="utf-8") as handle:
            json.dump(heat_stress, handle, indent=2, sort_keys=True)
        print(f"Saved heat stress summary to {args.heat_stress_output}")

    print(json.dumps(summary, indent=2, sort_keys=True))
    print(f"Saved city summaries to {args.city_summary_output}")
    print(f"Saved city rankings to {args.city_rankings_output}")
    print(f"Saved markdown report to {args.report_output}")

    _log_weather_to_mlflow(summary, city_summaries, raw_df=raw, csv_path=args.raw_input)


def _log_weather_to_mlflow(
    summary: dict,
    city_summaries: list[dict],
    raw_df=None,
    csv_path: str = "",
) -> None:
    """Log weather analysis results + dataset lineage to MLflow."""
    import os
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if not tracking_uri:
        return
    try:
        import mlflow
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("weather-analysis")

        temp = summary.get("temperature", {})
        precip = summary.get("precipitation", {})
        coverage = summary.get("coverage", {})

        temp_dev = float(temp.get("deviation_vs_1991_2020_mean", 0.0))
        temp_7d = float(temp.get("latest_7d_anomaly", 0.0))
        # +1 = warming (recent 7d warmer than season), -1 = cooling, 0 = neutral
        trend_direction = 1.0 if temp_7d > temp_dev + 0.5 else (-1.0 if temp_7d < temp_dev - 0.5 else 0.0)

        with mlflow.start_run(run_name="weather-dag", tags={"type": "weather_analysis", "dag": "lithuania_weather"}):
            _metrics = {
                "temp_deviation_vs_baseline":   temp_dev,
                "temp_z_score":                 float(temp.get("z_score_vs_baseline", 0.0)),
                "temp_7d_anomaly":              temp_7d,
                "trend_direction":              trend_direction,
                "precip_deviation_vs_baseline": float(precip.get("deviation_vs_1991_2020_mean", 0.0)),
                "precip_z_score":               float(precip.get("z_score_vs_baseline", 0.0)),
                "days_observed":                float(coverage.get("days_observed", 0)),
            }
            snowfall = summary.get("snowfall", {})
            if snowfall:
                _metrics["ytd_total_snowfall_cm"] = float(snowfall.get("ytd_total_cm", 0.0))
                _metrics["snowfall_deviation_vs_baseline_cm"] = float(snowfall.get("deviation_vs_baseline_cm", 0.0))
            sunshine = summary.get("sunshine", {})
            if sunshine:
                _metrics["ytd_total_sunshine_h"] = float(sunshine.get("ytd_total_hours", 0.0))
            current_s = summary.get("current", {})
            if "ytd_mean_wind_kmh" in current_s:
                _metrics["ytd_mean_wind_kmh"] = float(current_s["ytd_mean_wind_kmh"])
            if "ytd_total_et0_mm" in current_s:
                _metrics["ytd_total_et0_mm"] = float(current_s["ytd_total_et0_mm"])
            mlflow.log_metrics(_metrics)

            # --- Dataset lineage (DVC + MLflow Datasets) ---
            if raw_df is not None:
                import os as _os, yaml as _yaml
                import mlflow.data
                _dvc_file = csv_path + ".dvc" if csv_path else ""
                _dvc_md5 = ""
                if _dvc_file and _os.path.exists(_dvc_file):
                    try:
                        _meta = _yaml.safe_load(open(_dvc_file))
                        _dvc_md5 = (_meta.get("outs") or [{}])[0].get("md5", "")
                    except Exception:
                        pass
                _dataset = mlflow.data.from_pandas(
                    raw_df,
                    source=_os.path.abspath(csv_path) if csv_path else "unknown",
                    name="raw_daily_weather",
                    targets="temperature_2m_mean",
                )
                mlflow.log_input(_dataset, context="training")
                if _dvc_md5:
                    mlflow.set_tag("dvc.md5", _dvc_md5)
                    mlflow.set_tag("dvc.file", _os.path.basename(_dvc_file))
                mlflow.set_tag("dataset.rows", str(len(raw_df)))
                mlflow.set_tag("dataset.min_date", str(raw_df["time"].min()) if "time" in raw_df.columns else "")
                mlflow.set_tag("dataset.max_date", str(raw_df["time"].max()) if "time" in raw_df.columns else "")

            mlflow.log_table(
                data={
                    "city":              [c.get("city", "") for c in city_summaries],
                    "temp_anomaly_c":    [round(c.get("temperature", {}).get("deviation_vs_1991_2020_mean", 0.0), 2) for c in city_summaries],
                    "temp_z_score":      [round(c.get("temperature", {}).get("z_score_vs_baseline", 0.0), 2) for c in city_summaries],
                    "temp_7d_anomaly":   [round(c.get("temperature", {}).get("latest_7d_anomaly", 0.0), 2) for c in city_summaries],
                    "precip_anomaly_mm": [round(c.get("precipitation", {}).get("deviation_vs_1991_2020_mean", 0.0), 2) for c in city_summaries],
                    "precip_z_score":    [round(c.get("precipitation", {}).get("z_score_vs_baseline", 0.0), 2) for c in city_summaries],
                },
                artifact_file="weather_city_dataset.json",
            )
        print("[mlflow] weather analysis logged")
    except Exception as exc:
        print(f"[mlflow] WARNING: failed to log weather analysis: {exc}")


if __name__ == "__main__":
    main()