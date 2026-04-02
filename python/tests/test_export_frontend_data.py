"""
Smoke tests for the export_frontend_data pipeline bridge.

Verifies that the JSON schema produced by export_frontend_data.py matches
what the frontend expects, using a temporary output fixture.

Run with: cd ml/python && ../.venv/bin/pytest tests/ -v
"""
import json
import sys
import tempfile
from pathlib import Path

import pytest
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from rag_pipeline import DEFAULT_QUESTIONS

# ── fixtures ──────────────────────────────────────────────────────────────

MARCH_CSV = """\
year,mean_temp_c,days_observed,anomaly_c,zscore
2022,1.5,16,1.431,0.449
2023,-0.3,16,-0.369,-0.116
2024,2.1,16,2.031,0.637
2025,3.2,10,3.131,0.982
2026,4.3,16,4.231,1.327
"""

MARCH_SUMMARY = {
    "month": 3,
    "month_name": "March",
    "window": {
        "start_year": 2022,
        "end_year": 2026,
        "years_included": 5,
        "cutoff_day": 16,
        "execution_date": "2026-03-16",
    },
    "baseline": {"mean_temp_c": 0.069, "std_temp_c": 3.189},
    "latest_year": 2026,
}

YTD_SUMMARY = {
    "current_year": 2026,
    "coverage": {"period": "01-01 to 03-16", "proxy_cities": ["Vilnius"]},
    "current": {"ytd_mean_temp": -5.4, "ytd_total_precip": 95.9},
    "temperature": {
        "deviation_vs_1991_2020_mean": -3.6,
        "z_score_vs_baseline": -1.53,
        "latest_7d_anomaly": 6.3,
    },
    "precipitation": {
        "deviation_vs_1991_2020_mean": -18.4,
        "z_score_vs_baseline": -0.54,
        "latest_7d_anomaly": 6.4,
    },
}

CITY_RANKINGS = {
    "combined": [{"city": "Vilnius", "combined_score": 1.35, "precipitation_z": -1.35, "temperature_z": -1.35}],
    "temperature": [{"city": "Vilnius", "anomaly": -3.29, "z_score": -1.35}],
    "precipitation": [{"city": "Vilnius", "anomaly": -41.75, "z_score": -1.35}],
}

ML_EVAL = {"r2": 0.9889, "rmse": 0.2035, "mae": 0.1604}

RAW_WEATHER_CSV = """\
time,temperature_2m_mean,city
2022-03-01,1.0,Vilnius
2022-03-02,3.0,Vilnius
2023-03-01,2.0,Vilnius
2023-03-02,4.0,Vilnius
2024-03-01,5.0,Vilnius
2024-03-02,7.0,Vilnius
"""


@pytest.fixture
def pipeline_output_dir(tmp_path):
    """Creates a minimal replica of python/output/ in a temp directory."""
    march_dir = tmp_path / "vilnius_march"
    march_dir.mkdir()
    (march_dir / "march_temperature_anomalies.csv").write_text(MARCH_CSV)
    (march_dir / "summary.json").write_text(json.dumps(MARCH_SUMMARY))

    weather_dir = tmp_path / "weather"
    weather_dir.mkdir()
    (weather_dir / "ytd_summary.json").write_text(json.dumps(YTD_SUMMARY))
    (weather_dir / "city_ytd_summary.json").write_text(json.dumps([]))
    (weather_dir / "city_rankings.json").write_text(json.dumps(CITY_RANKINGS))
    (weather_dir / "weather_summary.md").write_text("Lithuania remains colder than normal on a year-to-date basis.")

    (march_dir / "report.md").write_text("Vilnius March remains warmer than the 30-year baseline.")

    (tmp_path / "evaluation.json").write_text(json.dumps(ML_EVAL))
    return tmp_path


@pytest.fixture
def pipeline_output_dir_without_vilnius_summary(tmp_path):
    weather_dir = tmp_path / "weather"
    weather_dir.mkdir()
    (weather_dir / "ytd_summary.json").write_text(json.dumps(YTD_SUMMARY))
    (weather_dir / "city_ytd_summary.json").write_text(json.dumps([]))
    (weather_dir / "city_rankings.json").write_text(json.dumps(CITY_RANKINGS))
    (weather_dir / "raw_daily_weather.csv").write_text(RAW_WEATHER_CSV)

    (tmp_path / "evaluation.json").write_text(json.dumps(ML_EVAL))
    return tmp_path


# ── tests ─────────────────────────────────────────────────────────────────

class TestExportFrontendData:

    def _run_export(self, output_dir, dest):
        """Import and call main() with patched sys.argv."""
        import importlib
        import export_frontend_data as mod

        original_argv = sys.argv
        sys.argv = [
            "export_frontend_data.py",
            "--output-dir", str(output_dir),
            "--frontend-data", str(dest),
        ]
        try:
            mod.main()
        finally:
            sys.argv = original_argv

    def test_output_file_created(self, pipeline_output_dir, tmp_path):
        dest = tmp_path / "dashboard.json"
        self._run_export(pipeline_output_dir, dest)
        assert dest.exists()

    def test_top_level_keys_present(self, pipeline_output_dir, tmp_path):
        dest = tmp_path / "dashboard.json"
        self._run_export(pipeline_output_dir, dest)
        data = json.loads(dest.read_text())
        assert "generated_at" in data
        assert "vilnius_month_anomaly" in data
        assert "lithuania_weather" in data
        assert "ml_model" in data
        assert "rag_demo" in data

    def test_vilnius_march_annual_count(self, pipeline_output_dir, tmp_path):
        dest = tmp_path / "dashboard.json"
        self._run_export(pipeline_output_dir, dest)
        data = json.loads(dest.read_text())
        assert len(data["vilnius_month_anomaly"]["annual"]) == 5

    def test_ml_metrics_are_floats(self, pipeline_output_dir, tmp_path):
        dest = tmp_path / "dashboard.json"
        self._run_export(pipeline_output_dir, dest)
        data = json.loads(dest.read_text())
        ml = data["ml_model"]
        assert isinstance(ml["r2"], float)
        assert isinstance(ml["rmse"], float)
        assert isinstance(ml["mae"], float)

    def test_latest_year_matches_summary(self, pipeline_output_dir, tmp_path):
        dest = tmp_path / "dashboard.json"
        self._run_export(pipeline_output_dir, dest)
        data = json.loads(dest.read_text())
        assert data["vilnius_month_anomaly"]["latest_year"]["year"] == 2026

    def test_extremes_present(self, pipeline_output_dir, tmp_path):
        dest = tmp_path / "dashboard.json"
        self._run_export(pipeline_output_dir, dest)
        data = json.loads(dest.read_text())
        extremes = data["vilnius_month_anomaly"]["extremes"]
        assert "warmest" in extremes and "coldest" in extremes
        assert extremes["warmest"]["year"] == 2026
        assert extremes["coldest"]["year"] == 2023

    def test_rag_demo_contains_questions(self, pipeline_output_dir, tmp_path):
        dest = tmp_path / "dashboard.json"
        self._run_export(pipeline_output_dir, dest)
        data = json.loads(dest.read_text())
        assert data["rag_demo"]["corpus_size"] >= 4
        assert len(data["rag_demo"]["questions"]) == len(DEFAULT_QUESTIONS)

    def test_falls_back_to_raw_weather_when_vilnius_summary_is_missing(self, pipeline_output_dir_without_vilnius_summary, tmp_path):
        dest = tmp_path / "dashboard.json"
        self._run_export(pipeline_output_dir_without_vilnius_summary, dest)
        data = json.loads(dest.read_text())

        vilnius = data["vilnius_month_anomaly"]
        assert vilnius["month_name"] == "March"
        assert vilnius["latest_year"]["year"] == 2024
        assert len(vilnius["annual"]) == 3
        assert vilnius["window"]["cutoff_day"] == 2
