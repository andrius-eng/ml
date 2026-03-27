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

FETCH_SCRIPT = PROJECT_ROOT / "python" / "vilnius_march_fetch.py"
ANALYZE_SCRIPT = PROJECT_ROOT / "python" / "vilnius_march_analyze.py"
PLOT_SCRIPT = PROJECT_ROOT / "python" / "vilnius_march_plot.py"
QUALITY_GATE_SCRIPT = PROJECT_ROOT / "python" / "vilnius_march_quality_gate.py"
RAG_PIPELINE_SCRIPT = PROJECT_ROOT / "python" / "rag_pipeline.py"

OUTPUT_DIR = PROJECT_ROOT / "python" / "output" / f"vilnius_{MONTH_SLUG}"
RAW_PATH = OUTPUT_DIR / "raw_daily_weather.csv"
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
    dag_id=f"vilnius_{MONTH_SLUG}_temperature_anomalies",
    default_args=DEFAULT_ARGS,
    description=f"Compare Vilnius {calendar.month_name[MONTH]} temperature slices across the last 30 years",
    schedule="0 7 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "vilnius", "temperature", MONTH_SLUG],
) as dag:
    fetch_vilnius_month = BashOperator(
        task_id=f"fetch_vilnius_{MONTH_SLUG}_weather",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{FETCH_SCRIPT}"\n'
            f'{project_python_command(str(FETCH_SCRIPT), "--month", str(MONTH), "--execution-date", EXECUTION_DATE, "--window-years", "30", "--output", str(RAW_PATH))}'
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

    fetch_vilnius_month >> analyze_vilnius_month >> [plot_vilnius_month, quality_gate]
    [plot_vilnius_month, quality_gate] >> refresh_rag_context