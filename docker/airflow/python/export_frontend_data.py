"""Export pipeline outputs to a single JSON file consumed by the frontend dashboard."""

from __future__ import annotations

import argparse
import calendar
import json
import os
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from rag_pipeline import build_demo_payload


def _sanitize_json_values(value):
    """Convert NaN/Inf values to JSON-safe values recursively."""
    if isinstance(value, float):
        if np.isnan(value) or np.isinf(value):
            return None
        return value
    if isinstance(value, dict):
        return {k: _sanitize_json_values(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_values(v) for v in value]
    return value


def load_json(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def _sample_predictions(df: pd.DataFrame, max_points: int = 200) -> list[dict]:
    """Downsample predictions for frontend chart rendering."""
    if df.empty:
        return []
    if len(df) > max_points:
        idx = np.round(np.linspace(0, len(df) - 1, max_points)).astype(int)
        df = df.iloc[idx]
    return [
        {"actual": round(float(r.y_true), 2), "predicted": round(float(r.y_pred), 2)}
        for r in df.itertuples()
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Export pipeline outputs for frontend dashboard")
    default_output_dir = os.environ.get("ML_OUTPUT_DIR", "python/output")
    default_frontend_data = "src/data/dashboard.json"
    if default_output_dir.startswith("/"):
        project_root = Path(default_output_dir).parent.parent
        default_frontend_data = str(project_root / "src" / "data" / "dashboard.json")

    parser.add_argument("--output-dir", type=str, default=default_output_dir)
    parser.add_argument("--frontend-data", type=str, default=default_frontend_data)
    parser.add_argument("--month", type=int, default=3, help="Calendar month number for the Vilnius monthly anomaly pipeline (1-12)")
    args = parser.parse_args()

    out = Path(args.output_dir)
    month_slug = calendar.month_name[args.month].lower()
    month_name = calendar.month_name[args.month]
    month_csv_name = f"{month_slug}_temperature_anomalies.csv"

    month_summary = load_json(out / f"vilnius_{month_slug}" / "summary.json")
    month_annual = pd.read_csv(out / f"vilnius_{month_slug}" / month_csv_name)
    weather_summary = load_json(out / "weather" / "ytd_summary.json")
    city_rankings = load_json(out / "weather" / "city_rankings.json")
    # Prefer the real-data climate model evaluation; fall back to legacy synthetic run
    climate_eval = out / "climate" / "climate_evaluation.json"
    legacy_eval = out / "evaluation.json"
    ml_eval = load_json(climate_eval if climate_eval.exists() else legacy_eval)
    predictions_csv = out / "climate" / "climate_predictions.csv"
    predictions_df = pd.read_csv(predictions_csv) if predictions_csv.exists() else pd.DataFrame()
    rag_demo_path = out / "rag" / "rag_demo.json"
    rag_demo = load_json(rag_demo_path) if rag_demo_path.exists() else build_demo_payload(out)
    beam_summary_path = out / "beam" / "beam_summary.json"
    beam_summary = load_json(beam_summary_path) if beam_summary_path.exists() else None
    heat_stress_path = out / "weather" / "heat_stress.json"
    heat_stress = load_json(heat_stress_path) if heat_stress_path.exists() else None
    hdd_path = out / "weather" / "hdd.json"
    hdd = load_json(hdd_path) if hdd_path.exists() else None

    annual_list = month_annual[["year", "mean_temp_c", "anomaly_c", "zscore"]].round(3).to_dict(orient="records")
    sorted_by_anomaly = month_annual.sort_values("anomaly_c")
    warmest = sorted_by_anomaly.iloc[-1]
    coldest = sorted_by_anomaly.iloc[0]
    latest_year_row = month_annual.sort_values("year").iloc[-1]

    dashboard = {
        "generated_at": date.today().isoformat(),
        "vilnius_month_anomaly": {
            "month_name": month_name,
            "window": month_summary["window"],
            "baseline": {
                "mean_temp_c": round(month_summary["baseline"]["mean_temp_c"], 3),
                "std_temp_c": round(month_summary["baseline"]["std_temp_c"], 3),
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
            "predictions": _sample_predictions(predictions_df),
        },
        "rag_demo": rag_demo,
        "beam_regional": beam_summary,
        "heat_stress": heat_stress,
        "heating_degree_days": hdd,
    }

    dest = Path(args.frontend_data)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dashboard = _sanitize_json_values(dashboard)
    with open(dest, "w") as f:
        json.dump(dashboard, f, indent=2, allow_nan=False)
    print(f"Dashboard data written to {dest}")


if __name__ == "__main__":
    main()
