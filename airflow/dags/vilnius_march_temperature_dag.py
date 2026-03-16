"""Airflow DAG for Vilnius March temperature anomalies over the last 30 years."""

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

FETCH_SCRIPT = PROJECT_ROOT / "python" / "vilnius_march_fetch.py"
ANALYZE_SCRIPT = PROJECT_ROOT / "python" / "vilnius_march_analyze.py"
PLOT_SCRIPT = PROJECT_ROOT / "python" / "vilnius_march_plot.py"
QUALITY_GATE_SCRIPT = PROJECT_ROOT / "python" / "vilnius_march_quality_gate.py"

OUTPUT_DIR = PROJECT_ROOT / "python" / "output" / "vilnius_march"
RAW_PATH = OUTPUT_DIR / "raw_daily_weather.csv"
ANNUAL_PATH = OUTPUT_DIR / "march_temperature_anomalies.csv"
SUMMARY_PATH = OUTPUT_DIR / "summary.json"
REPORT_PATH = OUTPUT_DIR / "report.md"
PLOT_PATH = OUTPUT_DIR / "march_temperature_anomalies.png"
EXECUTION_DATE = "{{ ds }}"


def project_python_command(*args: str) -> str:
    quoted_args = " ".join(f'"{arg}"' for arg in args)
    return f'"{PYTHON_BIN}" {quoted_args}'


with DAG(
    dag_id="vilnius_march_temperature_anomalies",
    default_args=DEFAULT_ARGS,
    description="Compare Vilnius March temperature slices across the last 30 years",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "vilnius", "temperature", "march"],
) as dag:
    fetch_vilnius_march = BashOperator(
        task_id="fetch_vilnius_march_weather",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{FETCH_SCRIPT}"\n'
            f'{project_python_command(str(FETCH_SCRIPT), "--execution-date", EXECUTION_DATE, "--window-years", "30", "--output", str(RAW_PATH))}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
    )

    analyze_vilnius_march = BashOperator(
        task_id="analyze_vilnius_march_anomalies",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{ANALYZE_SCRIPT}"\n'
            f'{project_python_command(str(ANALYZE_SCRIPT), "--raw-input", str(RAW_PATH), "--annual-output", str(ANNUAL_PATH), "--summary-output", str(SUMMARY_PATH), "--report-output", str(REPORT_PATH), "--execution-date", EXECUTION_DATE, "--window-years", "30")}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
    )

    plot_vilnius_march = BashOperator(
        task_id="plot_vilnius_march_anomalies",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{PLOT_SCRIPT}"\n'
            f'{project_python_command(str(PLOT_SCRIPT), "--annual-input", str(ANNUAL_PATH), "--summary-input", str(SUMMARY_PATH), "--output", str(PLOT_PATH))}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
    )

    quality_gate = BashOperator(
        task_id="validate_vilnius_march_output",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{QUALITY_GATE_SCRIPT}"\n'
            f'{project_python_command(str(QUALITY_GATE_SCRIPT), "--annual-input", str(ANNUAL_PATH), "--summary-input", str(SUMMARY_PATH), "--expected-years", "30", "--min-days", "10", "--max-abs-z", "4.0")}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
    )

    fetch_vilnius_march >> analyze_vilnius_march >> [plot_vilnius_march, quality_gate]