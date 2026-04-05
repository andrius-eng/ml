"""Export pipeline outputs to a single JSON file consumed by the frontend dashboard."""

from __future__ import annotations

import argparse
import calendar
import json
import os
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd


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


def load_optional_json(path: Path) -> dict | list | None:
    if not path.exists():
        return None
    return load_json(path)


def _prepare_vilnius_annual(annual: pd.DataFrame, *, recompute_stats: bool = False) -> pd.DataFrame:
    prepared = annual.copy()
    prepared["year"] = pd.to_numeric(prepared["year"], errors="coerce")
    prepared["mean_temp_c"] = pd.to_numeric(prepared["mean_temp_c"], errors="coerce")
    if "days_observed" in prepared.columns:
        prepared["days_observed"] = pd.to_numeric(prepared["days_observed"], errors="coerce")

    prepared = prepared.dropna(subset=["year", "mean_temp_c"]).sort_values("year").reset_index(drop=True)
    if prepared.empty:
        raise ValueError("Vilnius monthly anomaly dataset is empty")

    baseline_mean = float(prepared["mean_temp_c"].mean())
    baseline_std = float(prepared["mean_temp_c"].std(ddof=1)) if len(prepared) > 1 else 0.0

    if recompute_stats or "anomaly_c" not in prepared.columns:
        prepared["anomaly_c"] = prepared["mean_temp_c"] - baseline_mean
    else:
        prepared["anomaly_c"] = pd.to_numeric(prepared["anomaly_c"], errors="coerce")
        prepared["anomaly_c"] = prepared["anomaly_c"].fillna(prepared["mean_temp_c"] - baseline_mean)

    if recompute_stats or "zscore" not in prepared.columns:
        prepared["zscore"] = prepared["anomaly_c"] / baseline_std if baseline_std else 0.0
    else:
        prepared["zscore"] = pd.to_numeric(prepared["zscore"], errors="coerce")
        prepared["zscore"] = prepared["zscore"].fillna(
            prepared["anomaly_c"] / baseline_std if baseline_std else 0.0
        )

    prepared["year"] = prepared["year"].astype(int)
    return prepared


def _build_vilnius_summary_from_annual(
    annual: pd.DataFrame,
    *,
    month: int,
    cutoff_day: int | None = None,
    execution_date: date | None = None,
) -> dict:
    latest_row = annual.sort_values("year").iloc[-1]
    latest_year = int(latest_row["year"])

    if cutoff_day is None:
        if "days_observed" in annual.columns and pd.notna(latest_row.get("days_observed")):
            cutoff_day = int(latest_row["days_observed"])
        else:
            cutoff_day = calendar.monthrange(latest_year, month)[1]

    max_day = calendar.monthrange(latest_year, month)[1]
    cutoff_day = max(1, min(int(cutoff_day), max_day))
    execution_date = execution_date or date(latest_year, month, cutoff_day)

    return {
        "month": month,
        "month_name": calendar.month_name[month],
        "window": {
            "start_year": int(annual["year"].min()),
            "end_year": latest_year,
            "years_included": int(len(annual)),
            "cutoff_day": cutoff_day,
            "execution_date": execution_date.isoformat(),
        },
        "baseline": {
            "mean_temp_c": float(annual["mean_temp_c"].mean()),
            "std_temp_c": float(annual["mean_temp_c"].std(ddof=1)) if len(annual) > 1 else 0.0,
        },
        "latest_year": latest_year,
    }


def _derive_city_month_from_raw(
    raw_weather_path: Path,
    city: str,
    month: int,
    *,
    window_years: int = 87,
) -> tuple[dict, pd.DataFrame]:
    raw = pd.read_csv(raw_weather_path)
    if "time" not in raw.columns:
        raise ValueError(f"{raw_weather_path} is missing the 'time' column")

    temp_column = next(
        (column for column in ("temperature_2m_mean", "mean_temp_c") if column in raw.columns),
        None,
    )
    if temp_column is None:
        raise ValueError(f"{raw_weather_path} is missing a mean-temperature column")

    prepared = raw.copy()
    prepared["time"] = pd.to_datetime(prepared["time"], errors="coerce")
    prepared[temp_column] = pd.to_numeric(prepared[temp_column], errors="coerce")
    if "city" in prepared.columns:
        prepared = prepared[prepared["city"].astype(str).str.casefold() == city.casefold()]

    prepared = prepared.dropna(subset=["time", temp_column])
    prepared = prepared[prepared["time"].dt.month == month].copy()
    if prepared.empty:
        raise FileNotFoundError(
            f"No {city} month={month} rows were found in {raw_weather_path}"
        )

    latest_observation = prepared["time"].max()
    cutoff_day = int(latest_observation.day)
    prepared = prepared[prepared["time"].dt.day <= cutoff_day].copy()
    prepared["year"] = prepared["time"].dt.year

    annual = (
        prepared.groupby("year", as_index=False)
        .agg(mean_temp_c=(temp_column, "mean"), days_observed=("time", "count"))
        .sort_values("year")
        .reset_index(drop=True)
    )
    if annual.empty:
        raise FileNotFoundError(
            f"Unable to derive {city} month={month} annual aggregates from {raw_weather_path}"
        )

    latest_year = int(annual["year"].max())
    start_year = max(int(annual["year"].min()), latest_year - window_years + 1)
    annual = annual[annual["year"] >= start_year].reset_index(drop=True)
    annual = _prepare_vilnius_annual(annual, recompute_stats=True)

    return _build_vilnius_summary_from_annual(
        annual,
        month=month,
        cutoff_day=cutoff_day,
        execution_date=latest_observation.date(),
    ), annual


def _derive_vilnius_month_from_raw(
    raw_weather_path: Path,
    *,
    month: int,
    window_years: int = 87,
) -> tuple[dict, pd.DataFrame]:
    return _derive_city_month_from_raw(raw_weather_path, "Vilnius", month, window_years=window_years)


def _build_city_months_from_raw(raw_path: Path, window_years: int = 87) -> dict:
    """Derive per-city per-month anomaly data for all Lithuanian cities."""
    from weather_common import LITHUANIA_PROXY_CITIES

    if not raw_path.exists():
        return {}

    city_months: dict = {}
    for city_name in LITHUANIA_PROXY_CITIES:
        city_slug = city_name.lower()
        month_data: dict = {}
        for month_num in range(1, 13):
            month_slug = calendar.month_name[month_num].lower()
            try:
                m_summary, m_annual = _derive_city_month_from_raw(
                    raw_path, city_name, month_num, window_years=window_years
                )
                if len(m_annual) < 2:
                    continue
                m_latest = m_annual.sort_values("year").iloc[-1]
                month_data[month_slug] = {
                    "city": city_name,
                    "month_name": calendar.month_name[month_num],
                    "window": m_summary["window"],
                    "baseline": {
                        "mean_temp_c": round(m_summary["baseline"]["mean_temp_c"], 3),
                        "std_temp_c": round(m_summary["baseline"]["std_temp_c"], 3),
                    },
                    "latest_year": {
                        "year": int(m_latest["year"]),
                        "mean_temp_c": round(float(m_latest["mean_temp_c"]), 2),
                        "anomaly_c": round(float(m_latest["anomaly_c"]), 2),
                        "zscore": round(float(m_latest["zscore"]), 2),
                    },
                    "annual": m_annual[["year", "mean_temp_c", "anomaly_c", "zscore"]].round(3).to_dict(orient="records"),
                }
            except Exception as _e:
                pass  # Partial data for this city/month is acceptable
        if month_data:
            city_months[city_slug] = month_data
    return city_months


def _load_vilnius_month_payload(output_dir: Path, month: int) -> tuple[dict, pd.DataFrame]:
    month_slug = calendar.month_name[month].lower()
    month_dir = output_dir / f"vilnius_{month_slug}"
    month_summary = load_optional_json(month_dir / "summary.json")
    month_csv = month_dir / f"{month_slug}_temperature_anomalies.csv"
    raw_weather_csv = output_dir / "weather" / "raw_daily_weather.csv"

    if month_csv.exists():
        annual = _prepare_vilnius_annual(pd.read_csv(month_csv))
        if isinstance(month_summary, dict):
            return month_summary, annual
        return _build_vilnius_summary_from_annual(annual, month=month), annual

    if raw_weather_csv.exists():
        return _derive_city_month_from_raw(raw_weather_csv, "Vilnius", month)

    raise FileNotFoundError(
        f"Missing Vilnius monthly outputs under {month_dir} and no fallback raw weather CSV was found at {raw_weather_csv}"
    )


def _query_model_history() -> list[dict]:
    """Return training run history from MLflow, ordered oldest-first."""
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    try:
        import mlflow as _mlflow
        _mlflow.set_tracking_uri(tracking_uri)
        _client = _mlflow.tracking.MlflowClient()
        _exp = _client.get_experiment_by_name("climate-temperature-model")
        if _exp is None:
            return []
        _runs = _client.search_runs(
            [_exp.experiment_id],
            filter_string='tags.`mlflow.runName` = "train-climate-model"',
            order_by=["start_time ASC"],
            max_results=200,
        )
        history = []
        for _r in _runs:
            _m = _r.data.metrics
            if "test_r2" not in _m or "test_rmse" not in _m:
                continue
            entry = {
                "run_id": _r.info.run_id[:8],
                "date": datetime.fromtimestamp(_r.info.start_time / 1000).date().isoformat(),
                "r2": round(float(_m["test_r2"]), 4),
                "rmse": round(float(_m["test_rmse"]), 4),
                "mae": round(float(_m.get("test_mae", 0)), 4),
            }
            if "test_residual_mean" in _m:
                entry["residual_mean"] = round(float(_m["test_residual_mean"]), 4)
            if "test_residual_std" in _m:
                entry["residual_std"] = round(float(_m["test_residual_std"]), 4)
            history.append(entry)
        return history
    except Exception as _e:
        print(f"[export] WARNING: could not load model history from {tracking_uri}: {_e}")
        return []


def _query_weather_mlflow_extras() -> dict:
    """Query MLflow for weather metrics not captured in ytd_summary.json."""
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    result: dict = {}
    try:
        import mlflow as _mlflow
        _mlflow.set_tracking_uri(tracking_uri)
        _client = _mlflow.tracking.MlflowClient()
        _exp = _client.get_experiment_by_name("weather-analysis")
        if _exp is None:
            return result
        dag_runs = _client.search_runs(
            [_exp.experiment_id],
            filter_string='tags.`mlflow.runName` = "weather-dag"',
            order_by=["start_time DESC"],
            max_results=1,
        )
        if dag_runs:
            _m = dag_runs[0].data.metrics
            result["sunshine_h"] = round(float(_m.get("ytd_total_sunshine_h", 0)), 1)
            result["snowfall_cm"] = round(float(_m.get("ytd_total_snowfall_cm", 0)), 1)
            result["snowfall_deviation_cm"] = round(float(_m.get("snowfall_deviation_vs_baseline_cm", 0)), 1)
            result["wind_kmh"] = round(float(_m.get("ytd_mean_wind_kmh", 0)), 1)
            result["et0_mm"] = round(float(_m.get("ytd_total_et0_mm", 0)), 1)
            result["trend_direction"] = int(_m.get("trend_direction", 0))
        qg_runs = _client.search_runs(
            [_exp.experiment_id],
            filter_string='tags.`mlflow.runName` = "weather-quality-gate"',
            order_by=["start_time DESC"],
            max_results=1,
        )
        if qg_runs:
            _m = qg_runs[0].data.metrics
            result["quality_gate"] = {
                "passed": bool(_m.get("passed", 0)),
                "n_extreme_temp_months": int(_m.get("n_extreme_temp_months", 0)),
                "n_extreme_precip_months": int(_m.get("n_extreme_precip_months", 0)),
                "n_weak_months": int(_m.get("n_weak_months", 0)),
            }
    except Exception as _e:
        print(f"[export] WARNING: could not load weather MLflow extras from {tracking_uri}: {_e}")
    return result


def _query_ml_model_extras() -> dict:
    """Query MLflow for latest model residual metrics and training params."""
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    result: dict = {}
    try:
        import mlflow as _mlflow
        _mlflow.set_tracking_uri(tracking_uri)
        _client = _mlflow.tracking.MlflowClient()
        _exp = _client.get_experiment_by_name("climate-temperature-model")
        if _exp is None:
            return result
        runs = _client.search_runs(
            [_exp.experiment_id],
            filter_string='tags.`mlflow.runName` = "train-climate-model"',
            order_by=["start_time DESC"],
            max_results=1,
        )
        if not runs:
            return result
        _r = runs[0]
        _m = _r.data.metrics
        _p = _r.data.params
        if "test_residual_mean" in _m:
            result["residual_mean"] = round(float(_m["test_residual_mean"]), 4)
        if "test_residual_std" in _m:
            result["residual_std"] = round(float(_m["test_residual_std"]), 4)
        if _p:
            result["params"] = {
                "epochs": int(_p.get("epochs", 0)),
                "batch_size": int(_p.get("batch_size", 0)),
                "train_rows": int(_p.get("train_rows", 0)),
                "feature_count": int(_p.get("feature_count", 0)),
                "features": _p.get("features", ""),
                "lr": float(_p.get("lr", 0)),
            }
    except Exception as _e:
        print(f"[export] WARNING: could not load ML model extras from {tracking_uri}: {_e}")
    return result


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

    month_summary, month_annual = _load_vilnius_month_payload(out, args.month)
    weather_summary = load_json(out / "weather" / "ytd_summary.json")
    city_rankings = load_json(out / "weather" / "city_rankings.json")
    # Prefer the real-data climate model evaluation; fall back to legacy synthetic run
    climate_eval = out / "climate" / "climate_evaluation.json"
    legacy_eval = out / "evaluation.json"
    ml_eval = load_json(climate_eval if climate_eval.exists() else legacy_eval)
    predictions_csv = out / "climate" / "climate_predictions.csv"
    predictions_df = pd.read_csv(predictions_csv) if predictions_csv.exists() else pd.DataFrame()
    rag_demo_path = out / "rag" / "rag_demo.json"
    if rag_demo_path.exists():
        rag_demo = load_json(rag_demo_path)
    else:
        from rag_pipeline import build_demo_payload  # lazy: avoids slow torch import
        rag_demo = build_demo_payload(out)
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
            "city": "Vilnius",
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
            "history": _query_model_history(),
            **_query_ml_model_extras(),
        },
        "weather_mlflow": _query_weather_mlflow_extras(),
        "rag_demo": rag_demo,
        "beam_regional": beam_summary,
        "heat_stress": heat_stress,
        "heating_degree_days": hdd,
    }

    # Collect all available Vilnius monthly anomaly datasets for the month picker (backward compat)
    vilnius_months: dict = {}
    month_name_to_num = {name.lower(): i for i, name in enumerate(calendar.month_name) if name}
    for month_dir in sorted(out.glob("vilnius_*/")):
        slug = month_dir.name.replace("vilnius_", "")
        m_num = month_name_to_num.get(slug)
        if m_num is None:
            continue
        try:
            m_summary, m_annual = _load_vilnius_month_payload(out, m_num)
            m_sorted = m_annual.sort_values("year")
            m_latest = m_sorted.iloc[-1]
            vilnius_months[slug] = {
                "city": "Vilnius",
                "month_name": calendar.month_name[m_num],
                "window": m_summary["window"],
                "baseline": {
                    "mean_temp_c": round(m_summary["baseline"]["mean_temp_c"], 3),
                    "std_temp_c": round(m_summary["baseline"]["std_temp_c"], 3),
                },
                "latest_year": {
                    "year": int(m_latest["year"]),
                    "mean_temp_c": round(float(m_latest["mean_temp_c"]), 2),
                    "anomaly_c": round(float(m_latest["anomaly_c"]), 2),
                    "zscore": round(float(m_latest["zscore"]), 2),
                },
                "annual": m_annual[["year", "mean_temp_c", "anomaly_c", "zscore"]].round(3).to_dict(orient="records"),
            }
        except Exception as _e:
            print(f"WARNING: could not load vilnius_{slug}: {_e}")
    if vilnius_months:
        dashboard["vilnius_months"] = vilnius_months

    # Build per-city per-month anomaly data for all Lithuanian cities
    raw_weather_csv = out / "weather" / "raw_daily_weather.csv"
    city_months = _build_city_months_from_raw(raw_weather_csv)
    if city_months:
        dashboard["city_months"] = city_months

    dest = Path(args.frontend_data)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dashboard = _sanitize_json_values(dashboard)
    with open(dest, "w") as f:
        json.dump(dashboard, f, indent=2, allow_nan=False)
    print(f"Dashboard data written to {dest}")


if __name__ == "__main__":
    main()
