"""Airflow DAG for Vilnius monthly temperature anomalies over the last 85 years.

Set VILNIUS_ANALYSIS_MONTH env var (1-12) to analyze a different month.
Default is 3 (March).
"""

from __future__ import annotations

import calendar
import os
import sys
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

MLFLOW_TRACKING_URI = os.environ.get('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
MLFLOW_EXPERIMENT = 'vilnius-temperature-analysis'

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
RAW_PATH = PROJECT_ROOT / "python" / "output" / "weather" / "raw_daily_weather.csv"
ANNUAL_PATH = OUTPUT_DIR / f"{MONTH_SLUG}_temperature_anomalies.csv"
SUMMARY_PATH = OUTPUT_DIR / "summary.json"
REPORT_PATH = OUTPUT_DIR / "report.md"
PLOT_PATH = OUTPUT_DIR / f"{MONTH_SLUG}_temperature_anomalies.png"
RAG_DEMO_PATH = PROJECT_ROOT / "python" / "output" / "rag" / "rag_demo.json"


# ── MLflow helpers ────────────────────────────────────────────────────────────

def _pull_parent_run_id(context) -> str:
    return context['task_instance'].xcom_pull(task_ids='create_mlflow_run', key='mlflow_parent_run_id') or ''


def _mlflow_create_dag_run(**context):
    import mlflow, socket
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)
    with mlflow.start_run(
        run_name=f"vilnius-{MONTH_SLUG}-pipeline-{context['ds']}",
        tags={
            'dag_id': f'vilnius_{MONTH_SLUG}_anomaly',
            'dag_run_id': context.get('run_id', ''),
            'execution_date': context['ds'],
            'hostname': socket.gethostname(),
            'month': MONTH_SLUG,
            'type': 'dag_run',
        },
    ) as run:
        mlflow.log_param('execution_date', context['ds'])
        mlflow.log_param('month', MONTH_SLUG)
        mlflow.log_param('month_num', MONTH)
    context['task_instance'].xcom_push(key='mlflow_parent_run_id', value=run.info.run_id)


def _mlflow_child_run(run_name, parent_run_id, output_paths, fn, extra_tags=None, **context):
    import mlflow, socket, time
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)
    tags = {
        'mlflow.parentRunId': parent_run_id,
        'dag_id': f'vilnius_{MONTH_SLUG}_anomaly',
        'execution_date': context.get('ds', ''),
        'hostname': socket.gethostname(),
        'task_id': run_name,
        'month': MONTH_SLUG,
        **(extra_tags or {}),
    }
    start = time.time()
    with mlflow.start_run(run_name=run_name, tags=tags):
        mlflow.log_param('task_id', run_name)
        try:
            result = fn(**context)
            mlflow.log_metric('duration_s', time.time() - start)
            mlflow.log_metric('success', 1.0)
            for p in output_paths:
                _log_artifact(p)
            return result
        except Exception as exc:
            mlflow.log_metric('success', 0.0)
            mlflow.log_metric('duration_s', time.time() - start)
            mlflow.set_tag('error', str(exc)[:250])
            raise


def _log_artifact(path) -> None:
    import mlflow
    p = Path(str(path))
    if p.exists():
        mlflow.log_artifact(str(p))
        try:
            if p.is_file():
                mlflow.log_metric(p.stem.replace('-', '_') + '_size_kb', p.stat().st_size / 1024)
        except Exception:
            pass


def _run_script(script_path, args, logger, timeout=600, extra_env=None):
    import threading
    env = {**os.environ, **(extra_env or {})}
    cmd = [sys.executable, '-u', str(script_path)] + [str(a) for a in args]
    logger.info(f"Running: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1, env=env)

    def _drain(stream, log_fn):
        for line in stream:
            log_fn(line.rstrip())
        stream.close()

    import threading
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


# ── Task callables ────────────────────────────────────────────────────────────

def ensure_weather_artifact(**context):
    if not RAW_PATH.exists():
        raise FileNotFoundError(
            f"{RAW_PATH} not found. Run DAG lithuania_weather_anomaly first."
        )


def analyze_vilnius_month(**context):
    import logging
    parent_run_id = _pull_parent_run_id(context)

    def _do(**ctx):
        _run_script(
            ANALYZE_SCRIPT,
            [
                "--month", str(MONTH),
                "--raw-input", str(RAW_PATH),
                "--annual-output", str(ANNUAL_PATH),
                "--summary-output", str(SUMMARY_PATH),
                "--report-output", str(REPORT_PATH),
                "--window-years", "85",
                "--require-flink",
            ],
            logging.getLogger(__name__),
            timeout=900,
            extra_env={
                'ML_PROJECT_ROOT': str(PROJECT_ROOT),
                'MLFLOW_TRACKING_URI': MLFLOW_TRACKING_URI,
                'MLFLOW_PARENT_RUN_ID': parent_run_id,
            },
        )

    _mlflow_child_run(
        f'analyze-vilnius-{MONTH_SLUG}', parent_run_id,
        (ANNUAL_PATH, SUMMARY_PATH, REPORT_PATH),
        _do, **context,
    )


def plot_vilnius_month(**context):
    import logging
    parent_run_id = _pull_parent_run_id(context)

    def _do(**ctx):
        _run_script(
            PLOT_SCRIPT,
            [
                "--annual-input", str(ANNUAL_PATH),
                "--summary-input", str(SUMMARY_PATH),
                "--output", str(PLOT_PATH),
            ],
            logging.getLogger(__name__),
            extra_env={
                'ML_PROJECT_ROOT': str(PROJECT_ROOT),
                'MLFLOW_TRACKING_URI': MLFLOW_TRACKING_URI,
                'MLFLOW_PARENT_RUN_ID': parent_run_id,
            },
        )

    _mlflow_child_run(
        f'plot-vilnius-{MONTH_SLUG}', parent_run_id,
        (PLOT_PATH,),
        _do, **context,
    )


def validate_vilnius_month(**context):
    import logging
    parent_run_id = _pull_parent_run_id(context)

    def _do(**ctx):
        _run_script(
            QUALITY_GATE_SCRIPT,
            [
                "--annual-input", str(ANNUAL_PATH),
                "--summary-input", str(SUMMARY_PATH),
                "--expected-years", "85",
                "--min-days", "10",
                "--max-abs-z", "4.0",
            ],
            logging.getLogger(__name__),
            extra_env={
                'ML_PROJECT_ROOT': str(PROJECT_ROOT),
                'MLFLOW_TRACKING_URI': MLFLOW_TRACKING_URI,
                'MLFLOW_PARENT_RUN_ID': parent_run_id,
            },
        )

    _mlflow_child_run(f'validate-vilnius-{MONTH_SLUG}', parent_run_id, (), _do, **context)


def refresh_rag_context(**context):
    import logging
    parent_run_id = _pull_parent_run_id(context)

    def _do(**ctx):
        _run_script(
            RAG_PIPELINE_SCRIPT,
            [
                "--output-dir", str(PROJECT_ROOT / "python" / "output"),
                "--demo-output", str(RAG_DEMO_PATH),
            ],
            logging.getLogger(__name__),
            extra_env={
                'ML_PROJECT_ROOT': str(PROJECT_ROOT),
                'MLFLOW_TRACKING_URI': MLFLOW_TRACKING_URI,
                'MLFLOW_PARENT_RUN_ID': parent_run_id,
            },
        )

    _mlflow_child_run('refresh-rag-context', parent_run_id, (RAG_DEMO_PATH,), _do, **context)


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id=f"vilnius_{MONTH_SLUG}_anomaly",
    default_args=DEFAULT_ARGS,
    description=f"Compute 85-year {calendar.month_name[MONTH]} temperature anomalies for Vilnius (ERA5 back to 1940, fixed 1991–2025 baseline); feeds RAG context, LLM SFT, and frontend dashboard",
    schedule="0 7 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "vilnius", "temperature", "anomaly", MONTH_SLUG],
) as dag:
    create_mlflow_run = PythonOperator(
        task_id="create_mlflow_run",
        python_callable=_mlflow_create_dag_run,
    )

    ensure_artifact = PythonOperator(
        task_id="ensure_weather_artifact",
        python_callable=ensure_weather_artifact,
    )

    analyze = PythonOperator(
        task_id=f"analyze_vilnius_{MONTH_SLUG}_anomalies",
        python_callable=analyze_vilnius_month,
    )

    plot = PythonOperator(
        task_id=f"plot_vilnius_{MONTH_SLUG}_anomalies",
        python_callable=plot_vilnius_month,
    )

    quality_gate = PythonOperator(
        task_id=f"validate_vilnius_{MONTH_SLUG}_output",
        python_callable=validate_vilnius_month,
    )

    rag = PythonOperator(
        task_id="refresh_rag_context",
        python_callable=refresh_rag_context,
    )

    create_mlflow_run >> ensure_artifact >> analyze >> [plot, quality_gate]
    [plot, quality_gate] >> rag