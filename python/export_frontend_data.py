"""Export pipeline outputs to a single JSON file consumed by the frontend dashboard."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import pandas as pd

from rag_pipeline import build_demo_payload


def load_json(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export pipeline outputs for frontend dashboard")
    parser.add_argument("--output-dir", type=str, default="python/output")
    parser.add_argument("--frontend-data", type=str, default="src/data/dashboard.json")
    args = parser.parse_args()

    out = Path(args.output_dir)
    march_summary = load_json(out / "vilnius_march" / "summary.json")
    march_annual = pd.read_csv(out / "vilnius_march" / "march_temperature_anomalies.csv")
    weather_summary = load_json(out / "weather" / "ytd_summary.json")
    city_rankings = load_json(out / "weather" / "city_rankings.json")
    # Prefer the real-data climate model evaluation; fall back to legacy synthetic run
    climate_eval = out / "climate" / "climate_evaluation.json"
    legacy_eval = out / "evaluation.json"
    ml_eval = load_json(climate_eval if climate_eval.exists() else legacy_eval)
    rag_demo_path = out / "rag" / "rag_demo.json"
    rag_demo = load_json(rag_demo_path) if rag_demo_path.exists() else build_demo_payload(out)

    annual_list = march_annual[["year", "mean_temp_c", "anomaly_c", "zscore"]].round(3).to_dict(orient="records")
    sorted_by_anomaly = march_annual.sort_values("anomaly_c")
    warmest = sorted_by_anomaly.iloc[-1]
    coldest = sorted_by_anomaly.iloc[0]
    latest_year_row = march_annual.sort_values("year").iloc[-1]

    dashboard = {
        "generated_at": date.today().isoformat(),
        "vilnius_march": {
            "window": march_summary["window"],
            "baseline": {
                "mean_temp_c": round(march_summary["baseline"]["mean_temp_c"], 3),
                "std_temp_c": round(march_summary["baseline"]["std_temp_c"], 3),
            },
            "latest_year": {
                "year": int(latest_year_row["year"]),
                "mean_temp_c": round(float(latest_year_row["mean_temp_c"]), 2),
                "anomaly_c": round(float(latest_year_row["anomaly_c"]), 2),
                "zscore": round(float(latest_year_row["zscore"]), 2),
            },
            "extremes": {
                "warmest": {
                    "year": int(warmest["year"]),
                    "anomaly_c": round(float(warmest["anomaly_c"]), 2),
                },
                "coldest": {
                    "year": int(coldest["year"]),
                    "anomaly_c": round(float(coldest["anomaly_c"]), 2),
                },
            },
            "annual": annual_list,
        },
        "lithuania_weather": {
            "year": weather_summary["current_year"],
            "period": weather_summary["coverage"]["period"],
            "cities": weather_summary["coverage"]["proxy_cities"],
            "temp_anomaly_c": round(weather_summary["temperature"]["deviation_vs_1991_2020_mean"], 2),
            "temp_zscore": round(weather_summary["temperature"]["z_score_vs_baseline"], 2),
            "precip_anomaly_mm": round(weather_summary["precipitation"]["deviation_vs_1991_2020_mean"], 1),
            "precip_zscore": round(weather_summary["precipitation"]["z_score_vs_baseline"], 2),
            "latest_7d_temp_anomaly": round(weather_summary["temperature"]["latest_7d_anomaly"], 2),
            "city_rankings": city_rankings,
        },
        "ml_model": {
            "r2": round(ml_eval["r2"], 4),
            "rmse": round(ml_eval["rmse"], 4),
            "mae": round(ml_eval["mae"], 4),
        },
        "rag_demo": rag_demo,
    }

    dest = Path(args.frontend_data)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "w") as f:
        json.dump(dashboard, f, indent=2)
    print(f"Dashboard data written to {dest}")


if __name__ == "__main__":
    main()
