"""Plot Vilnius monthly temperature anomalies across the analysis window."""

from __future__ import annotations

import argparse
import calendar
import json

import matplotlib


matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot Vilnius monthly temperature anomalies")
    parser.add_argument("--annual-input", type=str, default=None)
    parser.add_argument("--summary-input", type=str, default=None)
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--month", type=int, default=None, help="Calendar month number; if not set, inferred from summary JSON")
    args = parser.parse_args()

    # Resolve summary path: explicit arg > month-derived default > march fallback
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
    output = args.output or f"python/output/vilnius_{month_slug}/{month_slug}_temperature_anomalies.png"

    annual = pd.read_csv(annual_input)
    colors = ["tab:red" if value >= 0 else "tab:blue" for value in annual["anomaly_c"]]
    fig, axes = plt.subplots(2, 1, figsize=(13, 9), sharex=True)

    axes[0].bar(annual["year"].astype(str), annual["anomaly_c"], color=colors)
    axes[0].axhline(0, color="black", linewidth=1)
    axes[0].set_title(
        f"Vilnius {month_name} Temperature Anomaly by Year (cutoff {month:02d}-{summary['window']['cutoff_day']:02d})"
    )
    axes[0].set_ylabel("Anomaly (C)")

    axes[1].plot(annual["year"], annual["mean_temp_c"], marker="o", linewidth=2, color="tab:orange")
    axes[1].axhline(summary["baseline"]["mean_temp_c"], color="black", linestyle="--", linewidth=1)
    axes[1].set_title(f"Vilnius {month_name} Mean Temperature")
    axes[1].set_ylabel("Mean temperature (C)")
    axes[1].set_xlabel("Year")

    tick_years = annual["year"].astype(int)
    axes[1].set_xticks(tick_years[::2])
    axes[1].tick_params(axis="x", rotation=45)

    fig.tight_layout()
    fig.savefig(output, dpi=150)
    plt.close(fig)
    print(f"Saved Vilnius {month_name} anomaly plot to {output}")


if __name__ == "__main__":
    main()