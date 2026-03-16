"""Plot Vilnius March temperature anomalies across the analysis window."""

from __future__ import annotations

import argparse
import json

import matplotlib


matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot Vilnius March temperature anomalies")
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
    parser.add_argument(
        "--output",
        type=str,
        default="python/output/vilnius_march/march_temperature_anomalies.png",
    )
    args = parser.parse_args()

    annual = pd.read_csv(args.annual_input)
    with open(args.summary_input, "r", encoding="utf-8") as handle:
        summary = json.load(handle)

    colors = ["tab:red" if value >= 0 else "tab:blue" for value in annual["anomaly_c"]]
    fig, axes = plt.subplots(2, 1, figsize=(13, 9), sharex=True)

    axes[0].bar(annual["year"].astype(str), annual["anomaly_c"], color=colors)
    axes[0].axhline(0, color="black", linewidth=1)
    axes[0].set_title(
        f"Vilnius March Temperature Anomaly by Year (cutoff 03-{summary['window']['cutoff_day']:02d})"
    )
    axes[0].set_ylabel("Anomaly (C)")

    axes[1].plot(annual["year"], annual["mean_temp_c"], marker="o", linewidth=2, color="tab:orange")
    axes[1].axhline(summary["baseline"]["mean_temp_c"], color="black", linestyle="--", linewidth=1)
    axes[1].set_title("Vilnius March Mean Temperature")
    axes[1].set_ylabel("Mean temperature (C)")
    axes[1].set_xlabel("Year")

    tick_years = annual["year"].astype(int)
    axes[1].set_xticks(tick_years[::2])
    axes[1].tick_params(axis="x", rotation=45)

    fig.tight_layout()
    fig.savefig(args.output, dpi=150)
    plt.close(fig)
    print(f"Saved Vilnius March anomaly plot to {args.output}")


if __name__ == "__main__":
    main()