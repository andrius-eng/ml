"""Airflow DAG – Climate temperature model trained on real ERA5 daily weather.

Trains a PyTorch residual MLP (BatchNorm + skip connection + Dropout) to predict
daily mean temperature for Lithuania using real ERA5 data (1991–present) fetched
by the lithuania_weather_analysis DAG.
--------
prepare_data        climate_data.py     feature engineering + train/test split
    ↓
train_model         climate_train.py    MLP training, MLflow logging
    ├─→ plot_training   plot.py         training MSE curve
    └─→ evaluate_model  climate_evaluate.py  held-out test metrics
            ├─→ plot_diagnostics  diagnostics.py  parity & residual plots
            └─→ quality_gate      quality_gate.py  R² / MSE thresholds
"""

from __future__ import annotations

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

# Scripts
CLIMATE_DATA_SCRIPT = PROJECT_ROOT / "python" / "climate_data.py"
CLIMATE_TRAIN_SCRIPT = PROJECT_ROOT / "python" / "climate_train.py"
CLIMATE_EVALUATE_SCRIPT = PROJECT_ROOT / "python" / "climate_evaluate.py"
PLOT_SCRIPT = PROJECT_ROOT / "python" / "plot.py"
DIAGNOSTICS_SCRIPT = PROJECT_ROOT / "python" / "diagnostics.py"
QUALITY_GATE_SCRIPT = PROJECT_ROOT / "python" / "quality_gate.py"
RAG_PIPELINE_SCRIPT = PROJECT_ROOT / "python" / "rag_pipeline.py"

# Input: raw city-level data produced by the lithuania_weather_analysis DAG
# climate_data.py aggregates cities → country-level internally
WEATHER_DAILY_PATH = PROJECT_ROOT / "python" / "output" / "weather" / "raw_daily_weather.csv"

# Outputs: all under python/output/climate/
TRACKING_DIR = PROJECT_ROOT / "mlruns"
OUTPUT_DIR = PROJECT_ROOT / "python" / "output" / "climate"
TRAIN_SET_PATH = OUTPUT_DIR / "climate_train.csv"
TEST_SET_PATH = OUTPUT_DIR / "climate_test.csv"
MODEL_PATH = OUTPUT_DIR / "climate_model.pth"
METRICS_PATH = OUTPUT_DIR / "climate_metrics.csv"
EVALUATION_PATH = OUTPUT_DIR / "climate_evaluation.json"
PREDICTIONS_PATH = OUTPUT_DIR / "climate_predictions.csv"
TRAINING_PLOT_PATH = OUTPUT_DIR / "climate_training_mse.png"
DIAGNOSTICS_PLOT_PATH = OUTPUT_DIR / "climate_diagnostics.png"
RAG_DEMO_PATH = PROJECT_ROOT / "python" / "output" / "rag" / "rag_demo.json"


def project_python_command(*args: str) -> str:
    quoted_args = " ".join(f'"{arg}"' for arg in args)
    return f'"{PYTHON_BIN}" {quoted_args}'


with DAG(
    dag_id="climate_temperature_model",
    default_args=DEFAULT_ARGS,
    description=(
        "Train a PyTorch residual MLP on real ERA5 Lithuania daily weather data (1991–2022) "
        "and evaluate on held-out 2023+ years, logging metrics to MLflow."
    ),
    schedule="0 5 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "mlflow", "climate", "era5", "torch"],
) as dag:

    prepare_data = BashOperator(
        task_id="prepare_climate_data",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{WEATHER_DAILY_PATH}" || '
            f'{{ echo "ERROR: Run the lithuania_weather_analysis DAG first to produce country_daily_weather.csv"; exit 1; }}\n'
            f'test -f "{CLIMATE_DATA_SCRIPT}"\n'
            f'{project_python_command(str(CLIMATE_DATA_SCRIPT), "--input", str(WEATHER_DAILY_PATH), "--train-output", str(TRAIN_SET_PATH), "--test-output", str(TEST_SET_PATH), "--test-from-year", "2023")}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
    )

    train_model = BashOperator(
        task_id="train_climate_model",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{CLIMATE_TRAIN_SCRIPT}"\n'
            f'_TRACKING="${{MLFLOW_TRACKING_URI:-{TRACKING_DIR}}}"\n'
            f'{project_python_command(str(CLIMATE_TRAIN_SCRIPT), "--train-data", str(TRAIN_SET_PATH), "--epochs", "200", "--lr", "0.001", "--batch-size", "128", "--tracking-uri", "$_TRACKING", "--model-path", str(MODEL_PATH), "--metrics-path", str(METRICS_PATH))}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
        append_env=True,
    )

    plot_training = BashOperator(
        task_id="plot_training_metrics",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{PLOT_SCRIPT}"\n'
            f'{project_python_command(str(PLOT_SCRIPT), "--metrics", str(METRICS_PATH), "--output", str(TRAINING_PLOT_PATH))}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
        append_env=True,
    )

    evaluate_model = BashOperator(
        task_id="evaluate_climate_model",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{CLIMATE_EVALUATE_SCRIPT}"\n'
            f'{project_python_command(str(CLIMATE_EVALUATE_SCRIPT), "--model", str(MODEL_PATH), "--test-data", str(TEST_SET_PATH), "--summary-json", str(EVALUATION_PATH), "--predictions-csv", str(PREDICTIONS_PATH))}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
        append_env=True,
    )

    plot_diagnostics = BashOperator(
        task_id="plot_diagnostics",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{DIAGNOSTICS_SCRIPT}"\n'
            f'{project_python_command(str(DIAGNOSTICS_SCRIPT), "--predictions", str(PREDICTIONS_PATH), "--output", str(DIAGNOSTICS_PLOT_PATH))}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
        append_env=True,
    )

    quality_gate = BashOperator(
        task_id="quality_gate",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{QUALITY_GATE_SCRIPT}"\n'
            # R² > 0.65: model must explain 65% of variance; MSE < 50 °C²: daily noise expected
            f'{project_python_command(str(QUALITY_GATE_SCRIPT), "--summary-json", str(EVALUATION_PATH), "--max-mse", "50.0", "--min-r2", "0.65")}'
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
    )

    prepare_data >> train_model
    train_model >> [plot_training, evaluate_model]
    evaluate_model >> [plot_diagnostics, quality_gate]
    [plot_training, plot_diagnostics, quality_gate] >> refresh_rag_context

