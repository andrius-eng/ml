"""Airflow DAG for Lithuania year-to-date weather anomaly analysis."""

from __future__ import annotations

import os
import sys
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule
import logging

MLFLOW_TRACKING_URI = os.environ.get('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
MLFLOW_EXPERIMENT = 'lithuania_weather_analysis'
FLINK_OVERVIEW_URL = os.environ.get('FLINK_OVERVIEW_URL', 'http://flink-jobmanager:8081/v1/overview')
FLINK_READY_REQUEST_TIMEOUT = int(os.environ.get('FLINK_READY_REQUEST_TIMEOUT_SECONDS', '5'))
FLINK_READY_POKE_INTERVAL = int(os.environ.get('FLINK_READY_POKE_INTERVAL_SECONDS', '10'))
FLINK_READY_TIMEOUT = int(os.environ.get('FLINK_READY_TIMEOUT_SECONDS', '1800'))


# ── MLflow helpers ────────────────────────────────────────────────────────────

def _mlflow_create_dag_run(**context):
    """Create the parent MLflow run for this DAG execution and push run_id to XCom."""
    import mlflow
    import socket
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)
    with mlflow.start_run(
        run_name=f"weather-pipeline-{context['ds']}",
        tags={
            'dag_id': 'lithuania_weather_anomaly',
            'dag_run_id': context.get('run_id', ''),
            'execution_date': context['ds'],
            'hostname': socket.gethostname(),
            'type': 'dag_run',
        },
    ) as run:
        mlflow.log_param('execution_date', context['ds'])
        mlflow.log_param('analysis_year', str(context['ds'])[:4])
    context['task_instance'].xcom_push(key='mlflow_parent_run_id', value=run.info.run_id)


def _mlflow_child_run(run_name: str, parent_run_id: str, output_paths: tuple, fn, extra_tags: dict | None = None, **context):
    """Execute fn inside a child MLflow run linked to parent_run_id, then log output artifacts."""
    import mlflow
    import socket
    import time
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)
    tags = {
        'mlflow.parentRunId': parent_run_id,
        'dag_id': 'lithuania_weather_anomaly',
        'execution_date': context.get('ds', ''),
        'hostname': socket.gethostname(),
        'task_id': run_name,
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
    """Log a single file or glob a directory for PNG/JSON/CSV artifacts."""
    import mlflow
    p = Path(str(path))
    if p.is_dir():
        for f in sorted(p.rglob('*'))[:30]:
            if f.is_file() and f.suffix in ('.png', '.json', '.csv', '.md'):
                mlflow.log_artifact(str(f), artifact_path=p.name)
    elif p.exists():
        import mlflow
        mlflow.log_artifact(str(p))
        try:
            mlflow.log_metric(p.stem.replace('-', '_') + '_size_kb', p.stat().st_size / 1024)
        except Exception:
            pass


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _pull_parent_run_id(context) -> str:
    return context['task_instance'].xcom_pull(task_ids='create_mlflow_run', key='mlflow_parent_run_id') or ''


def check_flink_ready(**context):
    """Check if Flink is ready with at least one taskmanager registered."""
    import requests
    
    try:
        response = requests.get(FLINK_OVERVIEW_URL, timeout=FLINK_READY_REQUEST_TIMEOUT)
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


def _push_beam_execution_metadata(context: dict, *, runner: str, started_on_flink: bool):
    task_instance = context.get("task_instance")
    if task_instance is None:
        return

    task_instance.xcom_push(key="beam_runner", value=runner)
    task_instance.xcom_push(key="beam_started_on_flink", value=started_on_flink)


def verify_beam_started_on_flink(**context):
    task_instance = context["task_instance"]
    beam_runner = task_instance.xcom_pull(task_ids="beam_regional_analysis", key="beam_runner")
    beam_started_on_flink = task_instance.xcom_pull(
        task_ids="beam_regional_analysis",
        key="beam_started_on_flink",
    )

    if beam_runner == "PortableRunner" and beam_started_on_flink:
        logging.getLogger(__name__).info("✓ Beam regional analysis executed on Flink")
        return

    raise AirflowFailException(
        "beam_regional_analysis did not start on Flink; PortableRunner fell back to DirectRunner"
    )


def run_script(script_path: Path, args: list, logger, timeout: int = 300, extra_env: dict | None = None):
    """Run a Python script, streaming stdout/stderr to the task log in real time."""
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    cmd = [sys.executable, "-u", str(script_path)] + args
    logger.info(f"Running: {' '.join(cmd)}")

    import select

    env = {**os.environ, **(extra_env or {})}

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env=env,
    )

    stderr_lines = []
    deadline = __import__("time").time() + timeout

    while True:
        remaining = deadline - __import__("time").time()
        if remaining <= 0:
            proc.kill()
            proc.wait()
            raise subprocess.TimeoutExpired(cmd, timeout)

        ready, _, _ = select.select([proc.stdout, proc.stderr], [], [], min(remaining, 5.0))
        for stream in ready:
            line = stream.readline()
            if not line:
                continue
            line = line.rstrip()
            if stream is proc.stdout:
                logger.info(line)
            else:
                stderr_lines.append(line)
                logger.warning(line)

        if proc.poll() is not None:
            # Drain remaining output
            for line in proc.stdout:
                logger.info(line.rstrip())
            for line in proc.stderr:
                stderr_lines.append(line.rstrip())
                logger.warning(line.rstrip())
            break

    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd)


def resolve_analysis_end(context: dict, analysis_end: str | None = None):
    """Resolve analysis end date from context or fallback."""
    if analysis_end and analysis_end != '{{ ds }}':
        return analysis_end

    date_str = context.get('ds')
    if date_str:
        return date_str

    return datetime.now().strftime('%Y-%m-%d')

def fetch_weather_data(**context):
    """Fetch weather data with caching (reuse if less than 60 minutes old)."""
    import mlflow
    parent_run_id = _pull_parent_run_id(context)

    def _do(**ctx):
        logger = logging.getLogger(__name__)
        execution_date = ctx.get("ds", datetime.now().strftime("%Y-%m-%d"))
        min_years_required = 80
        force_full_fetch = False

        if RAW_WEATHER_PATH.exists():
            age_seconds = (datetime.now() - datetime.fromtimestamp(RAW_WEATHER_PATH.stat().st_mtime)).total_seconds()
            years_present = 0
            try:
                import csv
                years: set[int] = set()
                with RAW_WEATHER_PATH.open("r", encoding="utf-8", newline="") as fh:
                    reader = csv.DictReader(fh)
                    for row in reader:
                        t = row.get("time", "")
                        if len(t) >= 4 and t[:4].isdigit():
                            years.add(int(t[:4]))
                years_present = len(years)
            except Exception as exc:
                logger.warning(f"Could not inspect cached weather coverage: {exc}")

            if years_present < min_years_required:
                force_full_fetch = True
                mlflow.set_tag('cache_action', 'force_full_fetch')
                mlflow.log_param('cache_years_found', years_present)
                logger.warning(
                    "Cached weather data only spans %s years (< %s); forcing full historical backfill",
                    years_present, min_years_required,
                )
            elif age_seconds < 3600:
                mlflow.set_tag('cache_action', 'hit')
                mlflow.log_metric('cache_age_minutes', round(age_seconds / 60, 1))
                mlflow.log_metric('cache_years', years_present)
                logger.info("✓ Using cached raw weather data (age: %.0f minutes, %s years)", age_seconds / 60, years_present)
                return

        mlflow.set_tag('cache_action', 'miss_or_refresh')
        logger.info("Fetching fresh weather data...")
        fetch_args = [
            "--start-date", "1940-01-01",
            "--end-date", execution_date,
            "--output", str(RAW_WEATHER_PATH),
            "--min-years-required", str(min_years_required),
        ]
        if force_full_fetch:
            fetch_args.extend(["--force-full-fetch", "--cache-minutes", "0"])

        run_script(
            WEATHER_FETCH_SCRIPT,
            fetch_args,
            logger,
            timeout=1800,
            extra_env={'MLFLOW_TRACKING_URI': MLFLOW_TRACKING_URI, 'MLFLOW_PARENT_RUN_ID': parent_run_id},
        )

        if RAW_WEATHER_PATH.exists():
            import csv
            with RAW_WEATHER_PATH.open('r') as f:
                row_count = sum(1 for _ in csv.reader(f)) - 1
            mlflow.log_metric('raw_row_count', row_count)

    _mlflow_child_run('fetch-weather-data', parent_run_id, (RAW_WEATHER_PATH,), _do, **context)


def analyze_weather_data(analysis_end=None, **context):
    """Analyze weather patterns and generate summaries."""
    parent_run_id = _pull_parent_run_id(context)
    resolved_end = resolve_analysis_end(context, analysis_end)

    def _do(**ctx):
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
                "--heat-stress-output", str(HEAT_STRESS_PATH),
                "--current-end", resolved_end,
            ],
            logger,
            extra_env={'MLFLOW_TRACKING_URI': MLFLOW_TRACKING_URI, 'MLFLOW_PARENT_RUN_ID': parent_run_id},
        )

    _mlflow_child_run(
        'analyze-weather-data', parent_run_id,
        (ANNUAL_SUMMARY_PATH, WEATHER_SUMMARY_PATH, WEATHER_REPORT_PATH, CITY_RANKINGS_PATH, HEAT_STRESS_PATH),
        _do,
        extra_tags={'analysis_end': resolved_end},
        **context,
    )


def plot_weather_data(analysis_end=None, **context):
    """Generate weather visualization plots."""
    parent_run_id = _pull_parent_run_id(context)
    resolved_end = resolve_analysis_end(context, analysis_end)

    def _do(**ctx):
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
            extra_env={'MLFLOW_TRACKING_URI': MLFLOW_TRACKING_URI, 'MLFLOW_PARENT_RUN_ID': parent_run_id},
        )

    _mlflow_child_run(
        'plot-weather-data', parent_run_id,
        (WEATHER_PLOT_PATH, CITY_PLOTS_DIR),
        _do,
        **context,
    )


def validate_weather_summary(analysis_end=None, **context):
    """Validate weather summary meets quality gates."""
    parent_run_id = _pull_parent_run_id(context)

    def _do(**ctx):
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
            extra_env={'MLFLOW_TRACKING_URI': MLFLOW_TRACKING_URI, 'MLFLOW_PARENT_RUN_ID': parent_run_id},
        )

    _mlflow_child_run('validate-weather-summary', parent_run_id, (), _do, **context)


def refresh_rag_context_data(analysis_end=None, **context):
    """Refresh RAG pipeline context with latest analysis."""
    parent_run_id = _pull_parent_run_id(context)

    def _do(**ctx):
        logger = logging.getLogger(__name__)
        run_script(
            RAG_PIPELINE_SCRIPT,
            [
                "--output-dir", str(PROJECT_ROOT / "python" / "output"),
                "--demo-output", str(RAG_DEMO_PATH),
            ],
            logger,
            extra_env={'MLFLOW_TRACKING_URI': MLFLOW_TRACKING_URI, 'MLFLOW_PARENT_RUN_ID': parent_run_id},
        )

    _mlflow_child_run('refresh-rag-context', parent_run_id, (RAG_DEMO_PATH,), _do, **context)


def _stream_subprocess(cmd: list, logger, timeout: int, label: str) -> int:
    """Run a subprocess and stream stdout/stderr line-by-line to the Airflow logger.

    Returns exit code. Raises subprocess.TimeoutExpired if the process exceeds
    ``timeout`` seconds. Unlike subprocess.run(capture_output=True) this makes
    every print() inside the child process visible immediately in Airflow logs
    rather than only after the process exits.
    """
    import select
    import threading

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    lines_out: list[str] = []
    lines_err: list[str] = []

    def _drain(stream, collector, log_fn):
        for line in stream:
            line = line.rstrip()
            collector.append(line)
            log_fn(f"[{label}] {line}")
        stream.close()

    t_out = threading.Thread(target=_drain, args=(proc.stdout, lines_out, logger.info), daemon=True)
    t_err = threading.Thread(target=_drain, args=(proc.stderr, lines_err, logger.warning), daemon=True)
    t_out.start()
    t_err.start()

    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        t_out.join(timeout=5)
        t_err.join(timeout=5)
        raise

    t_out.join()
    t_err.join()
    return proc.returncode


def run_beam_analysis_with_fallback(analysis_end=None, **context):
    """Run Beam pipeline via PortableRunner -> beam-job-server -> Flink cluster.

    Falls back to DirectRunner if the Flink/Beam stack is unavailable.
    """
    parent_run_id = _pull_parent_run_id(context)
    analysis_end = resolve_analysis_end(context, analysis_end)

    def _do(**ctx):
        logger = logging.getLogger(__name__)
        import mlflow

        # Attempt PortableRunner → Flink
        try:
            logger.info("Attempting Beam pipeline with PortableRunner -> Flink...")
            mlflow.set_tag('beam_runner_attempted', 'PortableRunner')
            cmd = [
                sys.executable, str(BEAM_ANALYSIS_SCRIPT),
                "--input", str(RAW_WEATHER_PATH),
                "--output-dir", str(BEAM_OUTPUT_DIR),
                "--end-date", analysis_end,
                "--no-fetch-missing-cities",
                "--runner", "PortableRunner",
                "--job_endpoint", "beam-job-server:8099",
                "--artifact_endpoint", "beam-job-server:8098",
                "--environment_type", "EXTERNAL",
                "--environment_config", "localhost:50000",
                "--parallelism", "1",
            ]
            rc = _stream_subprocess(cmd, logger, timeout=2700, label="PortableRunner")
            if rc != 0:
                raise subprocess.CalledProcessError(rc, cmd)
            _push_beam_execution_metadata(ctx, runner="PortableRunner", started_on_flink=True)
            mlflow.log_param('beam_runner_used', 'PortableRunner')
            mlflow.set_tag('beam_flink', 'true')
            logger.info("✅ Beam pipeline completed successfully on Flink via PortableRunner")
            return
        except subprocess.TimeoutExpired:
            logger.warning("PortableRunner/Flink timed out after 45 min — falling back to DirectRunner")
            mlflow.set_tag('beam_flink_fallback_reason', 'timeout')
        except subprocess.CalledProcessError as e:
            logger.warning(f"PortableRunner/Flink failed (exit {e.returncode}) — falling back to DirectRunner")
            mlflow.set_tag('beam_flink_fallback_reason', f'exit_{e.returncode}')

        # Fallback to DirectRunner
        mlflow.log_param('beam_runner_used', 'DirectRunner')
        mlflow.set_tag('beam_flink', 'false')
        logger.warning("⚠️ Falling back to DirectRunner - results will not use Flink")
        try:
            cmd = [
                sys.executable, str(BEAM_ANALYSIS_SCRIPT),
                "--input", str(RAW_WEATHER_PATH),
                "--output-dir", str(BEAM_OUTPUT_DIR),
                "--end-date", analysis_end,
                "--no-fetch-missing-cities",
                "--runner", "DirectRunner",
            ]
            rc = _stream_subprocess(cmd, logger, timeout=2700, label="DirectRunner")
            if rc != 0:
                raise subprocess.CalledProcessError(rc, cmd)
            _push_beam_execution_metadata(ctx, runner="DirectRunner", started_on_flink=False)
            logger.warning("⚠️ Beam pipeline completed with DirectRunner (fallback - Flink unavailable)")
        except subprocess.CalledProcessError as e:
            logger.error(f"DirectRunner also failed (exit {e.returncode})")
            raise RuntimeError("Both PortableRunner/Flink and DirectRunner failed for Beam pipeline")
        except subprocess.TimeoutExpired:
            raise RuntimeError("DirectRunner timed out after 45 min")

    _mlflow_child_run(
        'beam-regional-analysis', parent_run_id,
        (BEAM_OUTPUT_DIR,),
        _do,
        extra_tags={'analysis_end': analysis_end},
        **context,
    )


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
EUROSTAT_FETCH_SCRIPT = PROJECT_ROOT / "python" / "eurostat_fetch.py"

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
HEAT_STRESS_PATH = WEATHER_OUTPUT_DIR / "heat_stress.json"
HDD_PATH = WEATHER_OUTPUT_DIR / "hdd.json"

BEAM_OUTPUT_DIR = PROJECT_ROOT / "python" / "output" / "beam"
RAG_DEMO_PATH = PROJECT_ROOT / "python" / "output" / "rag" / "rag_demo.json"


with DAG(
    dag_id="lithuania_weather_anomaly",
    default_args=DEFAULT_ARGS,
    description="Fetch ERA5 + Eurostat HDD, run Beam/Flink regional analysis, publish anomaly data for dashboard and RAG",
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "analytics", "lithuania", "era5", "beam", "flink"],
) as dag:
    create_mlflow_run = PythonOperator(
        task_id="create_mlflow_run",
        python_callable=_mlflow_create_dag_run,
    )

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
        poke_interval=FLINK_READY_POKE_INTERVAL,
        timeout=FLINK_READY_TIMEOUT,
        mode="reschedule",
    )

    beam_regional_analysis = PythonOperator(
        task_id="beam_regional_analysis",
        python_callable=run_beam_analysis_with_fallback,
        execution_timeout=timedelta(minutes=50),
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    verify_beam_on_flink = PythonOperator(
        task_id="verify_beam_started_on_flink",
        python_callable=verify_beam_started_on_flink,
    )

    def _fetch_eurostat_hdd_task(**ctx):
        parent_run_id = _pull_parent_run_id(ctx)
        def _do(**_ctx):
            run_script(
                EUROSTAT_FETCH_SCRIPT,
                ["--output", str(HDD_PATH)],
                logging.getLogger(__name__),
                extra_env={'MLFLOW_TRACKING_URI': MLFLOW_TRACKING_URI, 'MLFLOW_PARENT_RUN_ID': parent_run_id},
            )
        _mlflow_child_run('fetch-eurostat-hdd', parent_run_id, (HDD_PATH,), _do, **ctx)

    fetch_eurostat_hdd = PythonOperator(
        task_id="fetch_eurostat_hdd",
        python_callable=_fetch_eurostat_hdd_task,
    )

    create_mlflow_run >> fetch_weather >> analyze_weather >> [plot_weather, quality_gate, wait_for_flink]
    create_mlflow_run >> fetch_eurostat_hdd >> beam_regional_analysis >> verify_beam_on_flink >> refresh_rag_context
    wait_for_flink >> beam_regional_analysis
    [plot_weather, quality_gate, verify_beam_on_flink] >> refresh_rag_context