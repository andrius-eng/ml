"""Airflow DAG for running the example training script with explicit paths."""

from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, timedelta

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
TRAIN_SCRIPT = PROJECT_ROOT / "python" / "train.py"
DATA_SCRIPT = PROJECT_ROOT / "python" / "data.py"
EVALUATE_SCRIPT = PROJECT_ROOT / "python" / "evaluate.py"
PLOT_SCRIPT = PROJECT_ROOT / "python" / "plot.py"
DIAGNOSTICS_SCRIPT = PROJECT_ROOT / "python" / "diagnostics.py"
QUALITY_GATE_SCRIPT = PROJECT_ROOT / "python" / "quality_gate.py"
TRACKING_DIR = PROJECT_ROOT / "mlruns"
OUTPUT_DIR = PROJECT_ROOT / "python" / "output"
DATASET_PATH = OUTPUT_DIR / "data.csv"
MODEL_PATH = OUTPUT_DIR / "model.pth"
TRAINING_METRICS_PATH = OUTPUT_DIR / "metrics.csv"
EVALUATION_PATH = OUTPUT_DIR / "evaluation.json"
PREDICTIONS_PATH = OUTPUT_DIR / "predictions.csv"
TRAINING_PLOT_PATH = OUTPUT_DIR / "training_mse.png"
DIAGNOSTICS_PLOT_PATH = OUTPUT_DIR / "diagnostics.png"
PYTHON_BIN = os.environ.get("TRAIN_PYTHON_BIN", "python")
if PYTHON_BIN != "python" and not Path(PYTHON_BIN).exists():
    PYTHON_BIN = "python"


def project_python_command(*args: str) -> str:
    quoted_args = ' '.join(f'"{arg}"' for arg in args)
    return f'"{PYTHON_BIN}" {quoted_args}'


with DAG(
    dag_id="mlflow_torch_training",
    default_args=DEFAULT_ARGS,
    description="Train a tiny Torch model and log with MLflow",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ml", "mlflow", "torch"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{DATA_SCRIPT}"\n'
            f'{project_python_command(str(DATA_SCRIPT), "--output", str(DATASET_PATH), "--samples", "500")}'
        ),
        env={
            "ML_PROJECT_ROOT": str(PROJECT_ROOT),
            "TRAIN_PYTHON_BIN": PYTHON_BIN,
        },
    )

    run_training = BashOperator(
        task_id="run_training",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{TRAIN_SCRIPT}"\n'
            f'{project_python_command(str(TRAIN_SCRIPT), "--epochs", "80", "--lr", "0.01", "--tracking-uri", str(TRACKING_DIR), "--data", str(DATASET_PATH), "--model-path", str(MODEL_PATH), "--metrics-path", str(TRAINING_METRICS_PATH))}'
        ),
        env={
            "ML_PROJECT_ROOT": str(PROJECT_ROOT),
            "TRAIN_PYTHON_BIN": PYTHON_BIN,
        },
    )

    evaluate_model = BashOperator(
        task_id="evaluate_model",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{EVALUATE_SCRIPT}"\n'
            f'{project_python_command(str(EVALUATE_SCRIPT), "--model", str(MODEL_PATH), "--data", str(DATASET_PATH), "--summary-json", str(EVALUATION_PATH), "--predictions-csv", str(PREDICTIONS_PATH))}'
        ),
        env={
            "ML_PROJECT_ROOT": str(PROJECT_ROOT),
            "TRAIN_PYTHON_BIN": PYTHON_BIN,
        },
    )

    plot_training_metrics = BashOperator(
        task_id="plot_training_metrics",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{PLOT_SCRIPT}"\n'
            f'{project_python_command(str(PLOT_SCRIPT), "--metrics", str(TRAINING_METRICS_PATH), "--output", str(TRAINING_PLOT_PATH))}'
        ),
        env={
            "ML_PROJECT_ROOT": str(PROJECT_ROOT),
            "TRAIN_PYTHON_BIN": PYTHON_BIN,
        },
    )

    plot_diagnostics = BashOperator(
        task_id="plot_diagnostics",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{DIAGNOSTICS_SCRIPT}"\n'
            f'{project_python_command(str(DIAGNOSTICS_SCRIPT), "--predictions", str(PREDICTIONS_PATH), "--output", str(DIAGNOSTICS_PLOT_PATH))}'
        ),
        env={
            "ML_PROJECT_ROOT": str(PROJECT_ROOT),
            "TRAIN_PYTHON_BIN": PYTHON_BIN,
        },
    )

    quality_gate = BashOperator(
        task_id="quality_gate",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{QUALITY_GATE_SCRIPT}"\n'
            f'{project_python_command(str(QUALITY_GATE_SCRIPT), "--summary-json", str(EVALUATION_PATH), "--max-mse", "0.08", "--min-r2", "0.97")}'
        ),
        env={
            "ML_PROJECT_ROOT": str(PROJECT_ROOT),
            "TRAIN_PYTHON_BIN": PYTHON_BIN,
        },
    )

    generate_data >> run_training >> evaluate_model
    run_training >> plot_training_metrics
    evaluate_model >> plot_diagnostics >> quality_gate
