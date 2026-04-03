"""Airflow DAG for Vilnius monthly temperature anomalies over the last 30 years.

Set VILNIUS_ANALYSIS_MONTH env var (1-12) to analyze a different month.
Default is 3 (March).
"""

from __future__ import annotations

import calendar
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
import mlflow

MLFLOW_TRACKING_URI = os.environ.get('MLFLOW_TRACKING_URI', 'http://mlflow:5000')

def _set_mlflow_experiment(experiment_name: str):
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(experiment_name)


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

MONTH = int(os.environ.get("VILNIUS_ANALYSIS_MONTH", "3"))
MONTH_SLUG = calendar.month_name[MONTH].lower()

ANALYZE_SCRIPT = PROJECT_ROOT / "python" / "vilnius_march_analyze.py"
PLOT_SCRIPT = PROJECT_ROOT / "python" / "vilnius_march_plot.py"
QUALITY_GATE_SCRIPT = PROJECT_ROOT / "python" / "vilnius_march_quality_gate.py"
RAG_PIPELINE_SCRIPT = PROJECT_ROOT / "python" / "rag_pipeline.py"

OUTPUT_DIR = PROJECT_ROOT / "python" / "output" / f"vilnius_{MONTH_SLUG}"
# Reuse the canonical weather ingest artifact to avoid duplicate API fetches.
RAW_PATH = PROJECT_ROOT / "python" / "output" / "weather" / "raw_daily_weather.csv"
ANNUAL_PATH = OUTPUT_DIR / f"{MONTH_SLUG}_temperature_anomalies.csv"
SUMMARY_PATH = OUTPUT_DIR / "summary.json"
REPORT_PATH = OUTPUT_DIR / "report.md"
PLOT_PATH = OUTPUT_DIR / f"{MONTH_SLUG}_temperature_anomalies.png"
RAG_DEMO_PATH = PROJECT_ROOT / "python" / "output" / "rag" / "rag_demo.json"
EXECUTION_DATE = "{{ ds }}"


def project_python_command(*args: str) -> str:
    quoted_args = " ".join(f'"{arg}"' for arg in args)
    return f'"{PYTHON_BIN}" {quoted_args}'


with DAG(
    dag_id=f"vilnius_{MONTH_SLUG}_anomaly",
    default_args=DEFAULT_ARGS,
    description=f"Compute 30-year {calendar.month_name[MONTH]} temperature anomalies for Vilnius; feeds RAG context, LLM SFT, and frontend dashboard",
    schedule="0 7 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "vilnius", "temperature", "anomaly", MONTH_SLUG],
) as dag:
    ensure_weather_artifact = BashOperator(
        task_id="ensure_weather_artifact",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{RAW_PATH}" || '
            f'{{ echo "ERROR: {RAW_PATH} not found. Run DAG lithuania_weather_anomaly first."; exit 1; }}\n'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
        append_env=True,
    )

    analyze_vilnius_month = BashOperator(
        task_id=f"analyze_vilnius_{MONTH_SLUG}_anomalies",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{ANALYZE_SCRIPT}"\n'
            f'{project_python_command(str(ANALYZE_SCRIPT), "--month", str(MONTH), "--raw-input", str(RAW_PATH), "--annual-output", str(ANNUAL_PATH), "--summary-output", str(SUMMARY_PATH), "--report-output", str(REPORT_PATH), "--execution-date", EXECUTION_DATE, "--window-years", "30")}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
        append_env=True,
    )

    plot_vilnius_month = BashOperator(
        task_id=f"plot_vilnius_{MONTH_SLUG}_anomalies",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{PLOT_SCRIPT}"\n'
            f'{project_python_command(str(PLOT_SCRIPT), "--annual-input", str(ANNUAL_PATH), "--summary-input", str(SUMMARY_PATH), "--output", str(PLOT_PATH))}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
        append_env=True,
    )

    quality_gate = BashOperator(
        task_id=f"validate_vilnius_{MONTH_SLUG}_output",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{QUALITY_GATE_SCRIPT}"\n'
            f'{project_python_command(str(QUALITY_GATE_SCRIPT), "--annual-input", str(ANNUAL_PATH), "--summary-input", str(SUMMARY_PATH), "--expected-years", "30", "--min-days", "10", "--max-abs-z", "4.0")}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
        append_env=True,
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
        append_env=True,
    )

    ensure_weather_artifact >> analyze_vilnius_month >> [plot_vilnius_month, quality_gate]
    [plot_vilnius_month, quality_gate] >> refresh_rag_context