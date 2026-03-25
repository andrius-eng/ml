"""Airflow DAG for Lithuania year-to-date weather anomaly analysis."""

from __future__ import annotations

import os
import sys
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule
import logging


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def check_flink_ready(**context):
    """Check if Flink is ready with at least one taskmanager registered."""
    import requests
    
    try:
        response = requests.get("http://flink-jobmanager:8081/v1/overview", timeout=5)
        response.raise_for_status()
        data = response.json()
        
        taskmanagers = data.get("taskmanagers", 0)
        slots = data.get("slots-total", 0)
        
        if taskmanagers >= 1:
            context["task_instance"].xcom_push(
                key="flink_status",
                value=f"Flink ready: {taskmanagers} taskmanager(s), {slots} slot(s)"
            )
            logging.getLogger(__name__).info(f"✓ Flink ready with {taskmanagers} taskmanager(s) and {slots} slot(s)")
            return True
        return False
    except Exception as e:
        logging.getLogger(__name__).warning(f"Flink health check failed: {e}")
        return False


def run_script(script_path: Path, args: list, logger):
    """Run a Python script with given arguments and capture output."""
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")
    
    cmd = [sys.executable, str(script_path)] + args
    logger.info(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=300)
        if result.stdout:
            logger.info(result.stdout)
        return result
    except subprocess.TimeoutExpired as e:
        logger.error(f"Script timeout after 300 seconds: {e}")
        raise
    except subprocess.CalledProcessError as e:
        logger.error(f"Script failed with exit code {e.returncode}")
        if e.stdout:
            logger.error(f"stdout: {e.stdout}")
        if e.stderr:
            logger.error(f"stderr: {e.stderr}")
        raise


def resolve_analysis_end(context: dict, analysis_end: str | None = None):
    """Resolve analysis end date from context or fallback."""
    if analysis_end and analysis_end != '{{ ds }}':
        return analysis_end

    date_str = context.get('ds')
    if date_str:
        return date_str

    return datetime.now().strftime('%Y-%m-%d')

def fetch_weather_data(**context):
    analysis_end = resolve_analysis_end(context, None)
    """Fetch weather data with caching (reuse if less than 60 minutes old)."""
    logger = logging.getLogger(__name__)
    
    # Get execution date from Airflow context
    execution_date = context.get("ds", datetime.now().strftime("%Y-%m-%d"))

    # Check cache
    if RAW_WEATHER_PATH.exists():
        age_seconds = (datetime.now() - datetime.fromtimestamp(RAW_WEATHER_PATH.stat().st_mtime)).total_seconds()       
        if age_seconds < 3600:  # Less than 60 minutes
            logger.info(f"✓ Using cached raw weather data (age: {age_seconds/60:.0f} minutes)")
            return

    logger.info("Fetching fresh weather data...")
    run_script(
        WEATHER_FETCH_SCRIPT,
        ["--start-date", "1991-01-01", "--end-date", execution_date, "--output", str(RAW_WEATHER_PATH)],    
        logger,
    )

def analyze_weather_data(analysis_end=None, **context):
    analysis_end = resolve_analysis_end(context, analysis_end if 'analysis_end' in globals() else None)
    """Analyze weather patterns and generate summaries."""
    logger = logging.getLogger(__name__)
    run_script(
        WEATHER_ANALYZE_SCRIPT,
        [
            "--raw-input", str(RAW_WEATHER_PATH),
            "--country-daily-output", str(COUNTRY_DAILY_PATH),
            "--annual-output", str(ANNUAL_SUMMARY_PATH),
            "--city-annual-output", str(CITY_ANNUAL_SUMMARY_PATH),
            "--summary-output", str(WEATHER_SUMMARY_PATH),
            "--city-summary-output", str(CITY_WEATHER_SUMMARY_PATH),
            "--report-output", str(WEATHER_REPORT_PATH),
            "--country-daily-anomalies-output", str(COUNTRY_DAILY_ANOMALY_PATH),
            "--city-daily-anomalies-output", str(CITY_DAILY_ANOMALY_PATH),
            "--country-monthly-output", str(COUNTRY_MONTHLY_PATH),
            "--city-monthly-output", str(CITY_MONTHLY_PATH),
            "--city-rankings-output", str(CITY_RANKINGS_PATH),
            "--current-end", analysis_end,
        ],
        logger,
    )


def plot_weather_data(analysis_end=None, **context):
    analysis_end = resolve_analysis_end(context, analysis_end if 'analysis_end' in globals() else None)
    """Generate weather visualization plots."""
    logger = logging.getLogger(__name__)
    run_script(
        WEATHER_PLOT_SCRIPT,
        [
            "--annual-input", str(ANNUAL_SUMMARY_PATH),
            "--summary-input", str(WEATHER_SUMMARY_PATH),
            "--city-summary-input", str(CITY_WEATHER_SUMMARY_PATH),
            "--country-daily-input", str(COUNTRY_DAILY_ANOMALY_PATH),
            "--country-monthly-input", str(COUNTRY_MONTHLY_PATH),
            "--city-daily-input", str(CITY_DAILY_ANOMALY_PATH),
            "--city-monthly-input", str(CITY_MONTHLY_PATH),
            "--city-plots-dir", str(CITY_PLOTS_DIR),
            "--output", str(WEATHER_PLOT_PATH),
        ],
        logger,
    )


def validate_weather_summary(analysis_end=None, **context):
    analysis_end = resolve_analysis_end(context, analysis_end if 'analysis_end' in globals() else None)
    """Validate weather summary meets quality gates."""
    logger = logging.getLogger(__name__)
    run_script(
        WEATHER_QUALITY_GATE_SCRIPT,
        [
            "--summary-input", str(WEATHER_SUMMARY_PATH),
            "--country-monthly-input", str(COUNTRY_MONTHLY_PATH),
            "--min-days", "60",
            "--min-month-days", "5",
            "--max-monthly-temp-abs-z", "3.5",
            "--max-monthly-precip-abs-z", "3.5",
        ],
        logger,
    )


def refresh_rag_context_data(analysis_end=None, **context):
    analysis_end = resolve_analysis_end(context, analysis_end if 'analysis_end' in globals() else None)
    """Refresh RAG pipeline context with latest analysis."""
    logger = logging.getLogger(__name__)
    run_script(
        RAG_PIPELINE_SCRIPT,
        [
            "--output-dir", str(PROJECT_ROOT / "python" / "output"),
            "--demo-output", str(RAG_DEMO_PATH),
        ],
        logger,
    )


def run_beam_analysis_with_fallback(analysis_end=None, **context):
    analysis_end = resolve_analysis_end(context, analysis_end if 'analysis_end' in globals() else None)
    """Run Beam pipeline with FlinkRunner, fallback to DirectRunner if needed."""
    logger = logging.getLogger(__name__)
    
    # Try FlinkRunner first with proper beam worker pool configuration
    try:
        logger.info("Attempting Beam pipeline with FlinkRunner...")
        cmd = [
            sys.executable, str(BEAM_ANALYSIS_SCRIPT),
            "--input", str(RAW_WEATHER_PATH),
            "--output-dir", str(BEAM_OUTPUT_DIR),
            "--end-date", analysis_end,
            "--runner", "FlinkRunner",
            "--flink_master", "flink-jobmanager:8081",
            "--parallelism", "2",
            "--job_endpoint", "beam-job-server:8099",
            "--artifact_endpoint", "beam-job-server:8098",
            "--environment_type", "EXTERNAL",
            "--environment_config", "beam-worker-pool:50000",
        ]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=2700)
        if result.stdout:
            logger.info(result.stdout)
        logger.info("✅ Beam pipeline completed successfully with FlinkRunner")
        return
    except subprocess.TimeoutExpired:
        logger.warning("FlinkRunner timeout - falling back to DirectRunner...")
    except subprocess.CalledProcessError as e:
        logger.warning(f"FlinkRunner failed (exit {e.returncode})")
        if e.stderr:
            logger.warning(f"stderr: {e.stderr}")
    
    # Fallback to DirectRunner
    logger.info("Running Beam pipeline with DirectRunner...")
    try:
        cmd = [
            sys.executable, str(BEAM_ANALYSIS_SCRIPT),
            "--input", str(RAW_WEATHER_PATH),
            "--output-dir", str(BEAM_OUTPUT_DIR),
            "--end-date", analysis_end,
            "--runner", "DirectRunner",
        ]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=2700)
        if result.stdout:
            logger.info(result.stdout)
        logger.warning("⚠️ Beam pipeline completed with DirectRunner (fallback from FlinkRunner)")
    except subprocess.CalledProcessError as e:
        logger.error(f"DirectRunner also failed (exit {e.returncode})")
        if e.stderr:
            logger.error(f"stderr: {e.stderr}")
        raise RuntimeError("Both FlinkRunner and DirectRunner failed for Beam pipeline")
    except subprocess.TimeoutExpired:
        raise RuntimeError("DirectRunner timeout for Beam pipeline")


DAG_DIR = Path(__file__).resolve().parent
# Discover project root: check /opt/airflow first, then fallback to /opt/airflow/project
_check_root = DAG_DIR.parent  # /opt/airflow
if (_check_root / "python").exists():
    DEFAULT_PROJECT_ROOT = _check_root
else:
    DEFAULT_PROJECT_ROOT = Path("/opt/airflow/project")
PROJECT_ROOT = Path(os.environ.get("ML_PROJECT_ROOT", str(DEFAULT_PROJECT_ROOT))).resolve()

# Python script paths
WEATHER_FETCH_SCRIPT = PROJECT_ROOT / "python" / "weather_fetch.py"
WEATHER_ANALYZE_SCRIPT = PROJECT_ROOT / "python" / "weather_analyze.py"
WEATHER_PLOT_SCRIPT = PROJECT_ROOT / "python" / "weather_plot.py"
WEATHER_QUALITY_GATE_SCRIPT = PROJECT_ROOT / "python" / "weather_quality_gate.py"
BEAM_ANALYSIS_SCRIPT = PROJECT_ROOT / "python" / "beam_analysis.py"
RAG_PIPELINE_SCRIPT = PROJECT_ROOT / "python" / "rag_pipeline.py"

# Output paths
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


with DAG(
    dag_id="lithuania_weather_analysis",
    default_args=DEFAULT_ARGS,
    description="Compare Lithuania 2026 year-to-date weather with historical expectations",
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "analytics", "lithuania"],
) as dag:
    fetch_weather = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data,
    )

    analyze_weather = PythonOperator(
        task_id="analyze_weather",
        python_callable=analyze_weather_data,
    )

    plot_weather = PythonOperator(
        task_id="plot_weather_anomalies",
        python_callable=plot_weather_data,
    )

    quality_gate = PythonOperator(
        task_id="validate_weather_summary",
        python_callable=validate_weather_summary,
    )

    refresh_rag_context = PythonOperator(
        task_id="refresh_rag_context",
        python_callable=refresh_rag_context_data,
    )

    wait_for_flink = PythonSensor(
        task_id="wait_for_flink_jobmanager",
        python_callable=check_flink_ready,
        poke_interval=5,
        timeout=600,
        mode="poke",
    )

    beam_regional_analysis = PythonOperator(
        task_id="beam_regional_analysis",
        python_callable=run_beam_analysis_with_fallback,
        execution_timeout=timedelta(minutes=50),
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    fetch_weather >> analyze_weather >> [plot_weather, quality_gate, wait_for_flink]
    wait_for_flink >> beam_regional_analysis
    [plot_weather, quality_gate, beam_regional_analysis] >> refresh_rag_context