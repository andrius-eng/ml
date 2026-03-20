"""Airflow DAG for Lithuania year-to-date weather anomaly analysis."""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


DAG_DIR = Path(__file__).resolve().parent
DEFAULT_PROJECT_ROOT = DAG_DIR.parents[1] if len(DAG_DIR.parents) >= 2 else Path("/opt/airflow/project")
PROJECT_ROOT = Path(os.environ.get("ML_PROJECT_ROOT", str(DEFAULT_PROJECT_ROOT))).resolve()
PYTHON_BIN = os.environ.get("TRAIN_PYTHON_BIN", "python")
if PYTHON_BIN != "python" and not Path(PYTHON_BIN).exists():
    PYTHON_BIN = "python"

WEATHER_FETCH_SCRIPT = PROJECT_ROOT / "python" / "weather_fetch.py"
WEATHER_ANALYZE_SCRIPT = PROJECT_ROOT / "python" / "weather_analyze.py"
WEATHER_PLOT_SCRIPT = PROJECT_ROOT / "python" / "weather_plot.py"
WEATHER_QUALITY_GATE_SCRIPT = PROJECT_ROOT / "python" / "weather_quality_gate.py"
BEAM_ANALYSIS_SCRIPT = PROJECT_ROOT / "python" / "beam_analysis.py"
RAG_PIPELINE_SCRIPT = PROJECT_ROOT / "python" / "rag_pipeline.py"

WEATHER_OUTPUT_DIR = PROJECT_ROOT / "python" / "output" / "weather"
RAW_WEATHER_PATH = WEATHER_OUTPUT_DIR / "raw_daily_weather.csv"
COUNTRY_DAILY_PATH = WEATHER_OUTPUT_DIR / "country_daily_weather.csv"
ANNUAL_SUMMARY_PATH = WEATHER_OUTPUT_DIR / "annual_summary.csv"
CITY_ANNUAL_SUMMARY_PATH = WEATHER_OUTPUT_DIR / "city_annual_summary.csv"
WEATHER_SUMMARY_PATH = WEATHER_OUTPUT_DIR / "ytd_summary.json"
CITY_WEATHER_SUMMARY_PATH = WEATHER_OUTPUT_DIR / "city_ytd_summary.json"
COUNTRY_DAILY_ANOMALY_PATH = WEATHER_OUTPUT_DIR / "country_daily_anomalies.csv"
CITY_DAILY_ANOMALY_PATH = WEATHER_OUTPUT_DIR / "city_daily_anomalies.csv"
COUNTRY_MONTHLY_PATH = WEATHER_OUTPUT_DIR / "country_monthly_anomalies.csv"
CITY_MONTHLY_PATH = WEATHER_OUTPUT_DIR / "city_monthly_anomalies.csv"
CITY_RANKINGS_PATH = WEATHER_OUTPUT_DIR / "city_rankings.json"
WEATHER_PLOT_PATH = WEATHER_OUTPUT_DIR / "weather_anomalies.png"
WEATHER_REPORT_PATH = WEATHER_OUTPUT_DIR / "weather_summary.md"
CITY_PLOTS_DIR = WEATHER_OUTPUT_DIR / "cities"
BEAM_OUTPUT_DIR = PROJECT_ROOT / "python" / "output" / "beam"
RAG_DEMO_PATH = PROJECT_ROOT / "python" / "output" / "rag" / "rag_demo.json"
ANALYSIS_END = "{{ ds }}"


def project_python_command(*args: str) -> str:
    quoted_args = " ".join(f'"{arg}"' for arg in args)
    return f'"{PYTHON_BIN}" {quoted_args}'


with DAG(
    dag_id="lithuania_weather_analysis",
    default_args=DEFAULT_ARGS,
    description="Compare Lithuania 2026 year-to-date weather with historical expectations",
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "analytics", "lithuania"],
) as dag:
    fetch_weather = BashOperator(
        task_id="fetch_weather_data",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{WEATHER_FETCH_SCRIPT}"\n'
            f'{project_python_command(str(WEATHER_FETCH_SCRIPT), "--start-date", "1991-01-01", "--end-date", ANALYSIS_END, "--output", str(RAW_WEATHER_PATH))}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
    )

    analyze_weather = BashOperator(
        task_id="analyze_weather",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{WEATHER_ANALYZE_SCRIPT}"\n'
            f'{project_python_command(str(WEATHER_ANALYZE_SCRIPT), "--raw-input", str(RAW_WEATHER_PATH), "--country-daily-output", str(COUNTRY_DAILY_PATH), "--annual-output", str(ANNUAL_SUMMARY_PATH), "--city-annual-output", str(CITY_ANNUAL_SUMMARY_PATH), "--summary-output", str(WEATHER_SUMMARY_PATH), "--city-summary-output", str(CITY_WEATHER_SUMMARY_PATH), "--report-output", str(WEATHER_REPORT_PATH), "--country-daily-anomalies-output", str(COUNTRY_DAILY_ANOMALY_PATH), "--city-daily-anomalies-output", str(CITY_DAILY_ANOMALY_PATH), "--country-monthly-output", str(COUNTRY_MONTHLY_PATH), "--city-monthly-output", str(CITY_MONTHLY_PATH), "--city-rankings-output", str(CITY_RANKINGS_PATH), "--current-end", ANALYSIS_END)}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
    )

    plot_weather = BashOperator(
        task_id="plot_weather_anomalies",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{WEATHER_PLOT_SCRIPT}"\n'
            f'{project_python_command(str(WEATHER_PLOT_SCRIPT), "--annual-input", str(ANNUAL_SUMMARY_PATH), "--summary-input", str(WEATHER_SUMMARY_PATH), "--city-summary-input", str(CITY_WEATHER_SUMMARY_PATH), "--country-daily-input", str(COUNTRY_DAILY_ANOMALY_PATH), "--country-monthly-input", str(COUNTRY_MONTHLY_PATH), "--city-daily-input", str(CITY_DAILY_ANOMALY_PATH), "--city-monthly-input", str(CITY_MONTHLY_PATH), "--city-plots-dir", str(CITY_PLOTS_DIR), "--output", str(WEATHER_PLOT_PATH))}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
    )

    quality_gate = BashOperator(
        task_id="validate_weather_summary",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{WEATHER_QUALITY_GATE_SCRIPT}"\n'
            f'{project_python_command(str(WEATHER_QUALITY_GATE_SCRIPT), "--summary-input", str(WEATHER_SUMMARY_PATH), "--country-monthly-input", str(COUNTRY_MONTHLY_PATH), "--min-days", "60", "--min-month-days", "5", "--max-monthly-temp-abs-z", "3.5", "--max-monthly-precip-abs-z", "3.5")}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
    )

    refresh_rag_context = BashOperator(
        task_id="refresh_rag_context",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{RAG_PIPELINE_SCRIPT}"\n'
            f'{project_python_command(str(RAG_PIPELINE_SCRIPT), "--output-dir", str(PROJECT_ROOT / "python" / "output"), "--demo-output", str(RAG_DEMO_PATH))}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
    )

    beam_regional_analysis = BashOperator(
        task_id="beam_regional_analysis",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{BEAM_ANALYSIS_SCRIPT}"\n'
            f'{project_python_command(str(BEAM_ANALYSIS_SCRIPT), "--input", str(RAW_WEATHER_PATH), "--output-dir", str(BEAM_OUTPUT_DIR), "--end-date", ANALYSIS_END)}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
    )

    fetch_weather >> analyze_weather >> [plot_weather, quality_gate, beam_regional_analysis]
    [plot_weather, quality_gate, beam_regional_analysis] >> refresh_rag_context