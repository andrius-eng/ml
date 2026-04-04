"""Airflow DAG – Climate temperature model trained on real ERA5 daily weather.

Trains a PyTorch residual MLP (BatchNorm + skip connection + Dropout) to predict
daily mean temperature for Lithuania using real ERA5 data (1991–present) fetched
by the lithuania_weather_anomaly DAG.
--------
create_mlflow_run   creates the parent DAG-level MLflow run (run_id stored in XCom)
prepare_data        climate_data.py     feature engineering + train/test split
    ↓
train_model         climate_train.py    MLP training, MLflow logging (writes mlflow_run_id.txt)
    ├─→ plot_training   plot.py         training MSE curve    (resumes run from mlflow_run_id.txt)
    └─→ evaluate_model  climate_evaluate.py  held-out test metrics  (same)
            ├─→ plot_diagnostics  diagnostics.py  parity & residual plots  (same)
            └─→ quality_gate      quality_gate.py  R² / MSE thresholds + @champion promotion
                    └─→ refresh_rag_context  rag_pipeline.py  update RAG context
"""

from __future__ import annotations

import os
import sys
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

MLFLOW_TRACKING_URI = os.environ.get('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
MLFLOW_EXPERIMENT = 'climate-temperature-model'

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

WEATHER_DAILY_PATH = PROJECT_ROOT / "python" / "output" / "weather" / "raw_daily_weather.csv"

OUTPUT_DIR = PROJECT_ROOT / "python" / "output" / "climate"
TRAIN_SET_PATH = OUTPUT_DIR / "climate_train.csv"
TEST_SET_PATH = OUTPUT_DIR / "climate_test.csv"
MODEL_PATH = OUTPUT_DIR / "climate_model.pth"
METRICS_PATH = OUTPUT_DIR / "climate_metrics.csv"
EVALUATION_PATH = OUTPUT_DIR / "climate_evaluation.json"
PREDICTIONS_PATH = OUTPUT_DIR / "climate_predictions.csv"
TRAINING_PLOT_PATH = OUTPUT_DIR / "climate_training_mse.png"
DIAGNOSTICS_PLOT_PATH = OUTPUT_DIR / "climate_diagnostics.png"
MLFLOW_RUN_ID_PATH = OUTPUT_DIR / "mlflow_run_id.txt"
RAG_DEMO_PATH = PROJECT_ROOT / "python" / "output" / "rag" / "rag_demo.json"


# ── MLflow helpers ────────────────────────────────────────────────────────────

def _pull_parent_run_id(context) -> str:
    return context['task_instance'].xcom_pull(task_ids='create_mlflow_run', key='mlflow_parent_run_id') or ''


def _pull_train_run_id(context) -> str:
    return context['task_instance'].xcom_pull(task_ids='train_climate_model', key='mlflow_train_run_id') or ''


def _mlflow_create_dag_run(**context):
    import mlflow, socket
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)
    with mlflow.start_run(
        run_name=f"climate-pipeline-{context['ds']}",
        tags={
            'dag_id': 'era5_temperature_forecast_retrain',
            'dag_run_id': context.get('run_id', ''),
            'execution_date': context['ds'],
            'hostname': socket.gethostname(),
            'type': 'dag_run',
        },
    ) as run:
        mlflow.log_param('execution_date', context['ds'])
    context['task_instance'].xcom_push(key='mlflow_parent_run_id', value=run.info.run_id)


def _run_script(script_path, args, logger, timeout=3600, extra_env=None):
    import threading
    env = {**os.environ, **(extra_env or {})}
    cmd = [sys.executable, '-u', str(script_path)] + [str(a) for a in args]
    logger.info(f"Running: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1, env=env)

    def _drain(stream, log_fn):
        for line in stream:
            log_fn(line.rstrip())
        stream.close()

    t_o = threading.Thread(target=_drain, args=(proc.stdout, logger.info), daemon=True)
    t_e = threading.Thread(target=_drain, args=(proc.stderr, logger.warning), daemon=True)
    t_o.start(); t_e.start()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill(); t_o.join(5); t_e.join(5); raise
    t_o.join(); t_e.join()
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd)


def _log_artifact(path) -> None:
    import mlflow
    p = Path(str(path))
    if p.exists() and p.is_file():
        mlflow.log_artifact(str(p))
        try:
            mlflow.log_metric(p.stem.replace('-', '_') + '_size_kb', p.stat().st_size / 1024)
        except Exception:
            pass


# ── Task callables ────────────────────────────────────────────────────────────

def prepare_climate_data(**context):
    import logging, mlflow, socket, time
    parent_run_id = _pull_parent_run_id(context)
    logger = logging.getLogger(__name__)

    if not WEATHER_DAILY_PATH.exists():
        raise FileNotFoundError(
            f"Run the lithuania_weather_anomaly DAG first to produce {WEATHER_DAILY_PATH}"
        )

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)
    start = time.time()
    with mlflow.start_run(
        run_name='prepare-climate-data',
        tags={
            'mlflow.parentRunId': parent_run_id,
            'dag_id': 'era5_temperature_forecast_retrain',
            'task_id': 'prepare_climate_data',
            'hostname': socket.gethostname(),
        },
    ):
        mlflow.log_param('test_from_year', '2023')
        try:
            _run_script(
                CLIMATE_DATA_SCRIPT,
                [
                    "--input", str(WEATHER_DAILY_PATH),
                    "--train-output", str(TRAIN_SET_PATH),
                    "--test-output", str(TEST_SET_PATH),
                    "--test-from-year", "2023",
                ],
                logger,
                extra_env={'MLFLOW_TRACKING_URI': MLFLOW_TRACKING_URI},
            )
            mlflow.log_metric('duration_s', time.time() - start)
            mlflow.log_metric('success', 1.0)
            for p in (TRAIN_SET_PATH, TEST_SET_PATH):
                _log_artifact(p)
                if p.exists():
                    import pandas as pd
                    rows = len(pd.read_csv(p))
                    mlflow.log_metric(p.stem + '_rows', rows)
        except Exception as exc:
            mlflow.log_metric('success', 0.0)
            mlflow.set_tag('error', str(exc)[:250])
            raise


def train_climate_model(**context):
    import logging, mlflow, socket, time
    parent_run_id = _pull_parent_run_id(context)
    logger = logging.getLogger(__name__)

    _run_script(
        CLIMATE_TRAIN_SCRIPT,
        [
            "--train-data", str(TRAIN_SET_PATH),
            "--epochs", "50",
            "--lr", "0.01",
            "--batch-size", "256",
            "--tracking-uri", MLFLOW_TRACKING_URI,
            "--model-path", str(MODEL_PATH),
            "--metrics-path", str(METRICS_PATH),
        ],
        logger,
        timeout=7200,
        extra_env={'MLFLOW_TRACKING_URI': MLFLOW_TRACKING_URI},
    )

    # climate_train.py wrote the run_id — read it and store in both XCom and link to parent
    train_run_id = ''
    if MLFLOW_RUN_ID_PATH.exists():
        train_run_id = MLFLOW_RUN_ID_PATH.read_text().strip()
        context['task_instance'].xcom_push(key='mlflow_train_run_id', value=train_run_id)

    # Tag the train run with parent link so it nests in the UI
    if train_run_id and parent_run_id:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        client = mlflow.tracking.MlflowClient()
        try:
            client.set_tag(train_run_id, 'mlflow.parentRunId', parent_run_id)
        except Exception as exc:
            logger.warning(f"Could not link train run to parent: {exc}")


def plot_training_metrics(**context):
    import logging
    logger = logging.getLogger(__name__)
    # plot.py resumes the train run via mlflow_run_id.txt automatically
    _run_script(
        PLOT_SCRIPT,
        ["--metrics", str(METRICS_PATH), "--output", str(TRAINING_PLOT_PATH)],
        logger,
        extra_env={'MLFLOW_TRACKING_URI': MLFLOW_TRACKING_URI},
    )


def evaluate_climate_model(**context):
    import logging
    logger = logging.getLogger(__name__)
    # climate_evaluate.py resumes the train run via mlflow_run_id.txt automatically
    _run_script(
        CLIMATE_EVALUATE_SCRIPT,
        [
            "--model", str(MODEL_PATH),
            "--test-data", str(TEST_SET_PATH),
            "--summary-json", str(EVALUATION_PATH),
            "--predictions-csv", str(PREDICTIONS_PATH),
        ],
        logger,
        extra_env={'MLFLOW_TRACKING_URI': MLFLOW_TRACKING_URI},
    )


def plot_diagnostics(**context):
    import logging
    logger = logging.getLogger(__name__)
    # diagnostics.py resumes the train run via mlflow_run_id.txt automatically
    _run_script(
        DIAGNOSTICS_SCRIPT,
        ["--predictions", str(PREDICTIONS_PATH), "--output", str(DIAGNOSTICS_PLOT_PATH)],
        logger,
        extra_env={'MLFLOW_TRACKING_URI': MLFLOW_TRACKING_URI},
    )


def run_quality_gate(**context):
    import logging
    logger = logging.getLogger(__name__)
    # quality_gate.py resumes the train run + promotes @champion automatically
    _run_script(
        QUALITY_GATE_SCRIPT,
        [
            "--summary-json", str(EVALUATION_PATH),
            "--max-mse", "50.0",
            "--min-r2", "0.65",
        ],
        logger,
        extra_env={'MLFLOW_TRACKING_URI': MLFLOW_TRACKING_URI},
    )


def refresh_rag_context(**context):
    import logging, mlflow, socket, time
    parent_run_id = _pull_parent_run_id(context)
    logger = logging.getLogger(__name__)

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)
    start = time.time()
    with mlflow.start_run(
        run_name='refresh-rag-context',
        tags={
            'mlflow.parentRunId': parent_run_id,
            'dag_id': 'era5_temperature_forecast_retrain',
            'task_id': 'refresh_rag_context',
            'hostname': socket.gethostname(),
        },
    ):
        try:
            _run_script(
                RAG_PIPELINE_SCRIPT,
                [
                    "--output-dir", str(PROJECT_ROOT / "python" / "output"),
                    "--demo-output", str(RAG_DEMO_PATH),
                ],
                logger,
                extra_env={'MLFLOW_TRACKING_URI': MLFLOW_TRACKING_URI},
            )
            mlflow.log_metric('duration_s', time.time() - start)
            mlflow.log_metric('success', 1.0)
            _log_artifact(RAG_DEMO_PATH)
        except Exception as exc:
            mlflow.log_metric('success', 0.0)
            mlflow.set_tag('error', str(exc)[:250])
            raise


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="era5_temperature_forecast_retrain",
    default_args=DEFAULT_ARGS,
    description=(
        "Daily retrain of PyTorch residual MLP on ERA5 Lithuania temperatures (1991–present); "
        "logs to MLflow and promotes @champion on passing quality gate."
    ),
    schedule="0 5 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "mlflow", "climate", "era5", "torch", "retrain"],
) as dag:
    create_mlflow_dag_run = PythonOperator(
        task_id="create_mlflow_run",
        python_callable=_mlflow_create_dag_run,
    )

    prepare_data = PythonOperator(
        task_id="prepare_climate_data",
        python_callable=prepare_climate_data,
    )

    train_model = PythonOperator(
        task_id="train_climate_model",
        python_callable=train_climate_model,
    )

    plot_training = PythonOperator(
        task_id="plot_training_metrics",
        python_callable=plot_training_metrics,
    )

    evaluate_model = PythonOperator(
        task_id="evaluate_climate_model",
        python_callable=evaluate_climate_model,
    )

    plot_diag = PythonOperator(
        task_id="plot_diagnostics",
        python_callable=plot_diagnostics,
    )

    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=run_quality_gate,
    )

    rag = PythonOperator(
        task_id="refresh_rag_context",
        python_callable=refresh_rag_context,
    )

    create_mlflow_dag_run >> prepare_data >> train_model
    train_model >> [plot_training, evaluate_model]
    evaluate_model >> [plot_diag, quality_gate]
    [plot_training, plot_diag, quality_gate] >> rag

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

MLFLOW_TRACKING_URI = os.environ.get('MLFLOW_TRACKING_URI', 'http://mlflow:5000')

def _set_mlflow_experiment(experiment_name: str):
    import mlflow  # lazy import to avoid slow DAG parse
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

# Input: raw city-level data produced by the lithuania_weather_anomaly DAG
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
    dag_id="era5_temperature_forecast_retrain",
    default_args=DEFAULT_ARGS,
    description=(
        "Daily retrain of PyTorch residual MLP on ERA5 Lithuania temperatures (1991–present); "
        "logs to MLflow and promotes @champion on passing quality gate."
    ),
    schedule="0 5 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "mlflow", "climate", "era5", "torch", "retrain"],
) as dag:

    prepare_data = BashOperator(
        task_id="prepare_climate_data",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{WEATHER_DAILY_PATH}" || '
            f'{{ echo "ERROR: Run the lithuania_weather_anomaly DAG first to produce country_daily_weather.csv"; exit 1; }}\n'
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
            f'{project_python_command(str(CLIMATE_TRAIN_SCRIPT), "--train-data", str(TRAIN_SET_PATH), "--epochs", "50", "--lr", "0.01", "--batch-size", "256", "--tracking-uri", "$_TRACKING", "--model-path", str(MODEL_PATH), "--metrics-path", str(METRICS_PATH))}'
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

