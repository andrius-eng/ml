"""Create plots for Lithuania weather anomalies and historical comparison."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib


matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot Lithuania weather anomaly charts")
    parser.add_argument(
        "--annual-input",
        type=str,
        default="python/output/weather/annual_summary.csv",
    )
    parser.add_argument(
        "--summary-input",
        type=str,
        default="python/output/weather/ytd_summary.json",
    )
    parser.add_argument(
        "--city-summary-input",
        type=str,
        default="python/output/weather/city_ytd_summary.json",
    )
    parser.add_argument(
        "--country-daily-input",
        type=str,
        default="python/output/weather/country_daily_anomalies.csv",
    )
    parser.add_argument(
        "--country-monthly-input",
        type=str,
        default="python/output/weather/country_monthly_anomalies.csv",
    )
    parser.add_argument(
        "--city-daily-input",
        type=str,
        default="python/output/weather/city_daily_anomalies.csv",
    )
    parser.add_argument(
        "--city-monthly-input",
        type=str,
        default="python/output/weather/city_monthly_anomalies.csv",
    )
    parser.add_argument(
        "--city-plots-dir",
        type=str,
        default="python/output/weather/cities",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="python/output/weather/weather_anomalies.png",
    )
    args = parser.parse_args()

    annual = pd.read_csv(args.annual_input)
    country_daily = pd.read_csv(args.country_daily_input)
    country_monthly = pd.read_csv(args.country_monthly_input)
    city_daily = pd.read_csv(args.city_daily_input)
    city_monthly = pd.read_csv(args.city_monthly_input)
    with open(args.summary_input, "r", encoding="utf-8") as handle:
        summary = json.load(handle)
    with open(args.city_summary_input, "r", encoding="utf-8") as handle:
        city_summaries = json.load(handle)

    current_year = summary["current_year"]
    fig, axes = plt.subplots(3, 2, figsize=(14, 12))

    axes[0, 0].plot(pd.to_datetime(country_daily["time"]), country_daily["temp_anomaly"], alpha=0.35, label="Daily anomaly")
    axes[0, 0].plot(pd.to_datetime(country_daily["time"]), country_daily["rolling_7d_temp_anomaly"], linewidth=2, label="7-day rolling")
    axes[0, 0].axhline(0, linestyle="--", color="black")
    axes[0, 0].set_title("Current-Year Temperature Anomaly")
    axes[0, 0].set_xlabel("Year")
    axes[0, 0].set_ylabel("Temperature anomaly (C)")
    axes[0, 0].legend()

    axes[0, 1].plot(pd.to_datetime(country_daily["time"]), country_daily["cumulative_precip_anomaly"], color="tab:green", linewidth=2)
    axes[0, 1].axhline(0, linestyle="--", color="black")
    axes[0, 1].set_title("Current-Year Cumulative Precipitation Anomaly")
    axes[0, 1].set_xlabel("Date")
    axes[0, 1].set_ylabel("Cumulative anomaly (mm)")

    axes[1, 0].bar(country_monthly["month"].astype(str), country_monthly["temp_anomaly"], color="tab:blue")
    axes[1, 0].axhline(0, color="black", linewidth=1)
    axes[1, 0].set_title("Monthly Temperature Anomaly")
    axes[1, 0].set_ylabel("Temperature anomaly (C)")

    axes[1, 1].bar(country_monthly["month"].astype(str), country_monthly["precip_anomaly"], color="tab:green")
    axes[1, 1].axhline(0, color="black", linewidth=1)
    axes[1, 1].set_title("Monthly Precipitation Anomaly")
    axes[1, 1].set_ylabel("Precipitation anomaly (mm)")

    city_names = [entry["city"] for entry in city_summaries]
    city_temp_anomalies = [entry["temperature"]["deviation_vs_1991_2020_mean"] for entry in city_summaries]
    city_precip_anomalies = [entry["precipitation"]["deviation_vs_1991_2020_mean"] for entry in city_summaries]

    temp_order = np.argsort(np.abs(city_temp_anomalies))[::-1]
    ordered_temp_names = [city_names[index] for index in temp_order]
    ordered_temp_anomalies = [city_temp_anomalies[index] for index in temp_order]
    ordered_precip_names = [city_names[index] for index in np.argsort(np.abs(city_precip_anomalies))[::-1]]
    ordered_precip_anomalies = [city_precip_anomalies[index] for index in np.argsort(np.abs(city_precip_anomalies))[::-1]]

    axes[2, 0].bar(ordered_temp_names, ordered_temp_anomalies, color="tab:blue")
    axes[2, 0].axhline(0, color="black", linewidth=1)
    axes[2, 0].set_title("City Temperature Anomaly Ranking")
    axes[2, 0].set_ylabel("Temperature anomaly (C)")

    axes[2, 1].bar(ordered_precip_names, ordered_precip_anomalies, color="tab:green")
    axes[2, 1].axhline(0, color="black", linewidth=1)
    axes[2, 1].set_title("City Precipitation Anomaly Ranking")
    axes[2, 1].set_ylabel("Precipitation anomaly (mm)")

    fig.tight_layout()
    fig.savefig(args.output, dpi=150)
    print(f"Saved weather anomaly plot to {args.output}")

    city_plot_dir = Path(args.city_plots_dir)
    city_plot_dir.mkdir(parents=True, exist_ok=True)
    city_daily["time"] = pd.to_datetime(city_daily["time"])

    for city in sorted(city_daily["city"].unique()):
        city_daily_frame = city_daily[city_daily["city"] == city].copy()
        city_monthly_frame = city_monthly[city_monthly["city"] == city].copy()

        city_fig, city_axes = plt.subplots(2, 2, figsize=(12, 8))

        city_axes[0, 0].plot(city_daily_frame["time"], city_daily_frame["temp_anomaly"], alpha=0.35, label="Daily anomaly")
        city_axes[0, 0].plot(city_daily_frame["time"], city_daily_frame["rolling_7d_temp_anomaly"], linewidth=2, label="7-day rolling")
        city_axes[0, 0].axhline(0, linestyle="--", color="black")
        city_axes[0, 0].set_title(f"{city} Temperature Anomaly")
        city_axes[0, 0].set_ylabel("Temperature anomaly (C)")
        city_axes[0, 0].legend()

        city_axes[0, 1].plot(city_daily_frame["time"], city_daily_frame["cumulative_precip_anomaly"], color="tab:green", linewidth=2)
        city_axes[0, 1].axhline(0, linestyle="--", color="black")
        city_axes[0, 1].set_title(f"{city} Cumulative Precipitation Anomaly")
        city_axes[0, 1].set_ylabel("Cumulative anomaly (mm)")

        city_axes[1, 0].bar(city_monthly_frame["month"].astype(str), city_monthly_frame["temp_anomaly"], color="tab:blue")
        city_axes[1, 0].axhline(0, color="black", linewidth=1)
        city_axes[1, 0].set_title(f"{city} Monthly Temperature Anomaly")
        city_axes[1, 0].set_ylabel("Temperature anomaly (C)")

        city_axes[1, 1].bar(city_monthly_frame["month"].astype(str), city_monthly_frame["precip_anomaly"], color="tab:green")
        city_axes[1, 1].axhline(0, color="black", linewidth=1)
        city_axes[1, 1].set_title(f"{city} Monthly Precipitation Anomaly")
        city_axes[1, 1].set_ylabel("Precipitation anomaly (mm)")

        city_fig.tight_layout()
        city_output = city_plot_dir / f"{city.lower()}.png"
        city_fig.savefig(city_output, dpi=150)
        plt.close(city_fig)
        print(f"Saved city weather plot to {city_output}")

    plt.close(fig)


if __name__ == "__main__":
    main()