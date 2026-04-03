"""A minimal FastAPI app to serve predictions from the trained model."""

from __future__ import annotations

import math
import time
from datetime import date, timedelta
from pathlib import Path

import torch
import re
import urllib.request as _urllib_req
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from model import ClimateModel

import json
import os
import sys

# Make sibling modules importable when running from outside python/
sys.path.insert(0, str(Path(__file__).resolve().parent))

from climate_model_contract import (
    build_input_tensor,
    build_input_tensor_for_date,
    attach_feature_spec,
    load_climate_feature_spec,
    load_local_climate_model,
)
from rag_pipeline import answer_question

try:
    from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
    from starlette.responses import Response as StarletteResponse
    _prometheus_available = True
except ImportError:
    _prometheus_available = False

try:
    import mlflow
    import mlflow.pytorch
    from mlflow import MlflowClient
    _mlflow_available = True
except Exception:
    mlflow = None
    MlflowClient = None
    _mlflow_available = False

MLFLOW_TRACKING_URI = os.environ.get('MLFLOW_TRACKING_URI', '')
MLFLOW_EXPERIMENT_NAME = os.environ.get('MLFLOW_EXPERIMENT_NAME', 'climate-temperature-model')
MODEL_REGISTRY_NAME = 'ClimateTemperatureModel'
MODEL_ALIAS = 'champion'

if _mlflow_available and MLFLOW_TRACKING_URI:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    mlflow.enable_system_metrics_logging()

ML_OUTPUT_DIR = Path(os.environ.get("ML_OUTPUT_DIR", "python/output"))

# ── Prometheus metrics ────────────────────────────────────────────────────────
if _prometheus_available:
    REQUEST_COUNT = Counter(
        "ml_server_requests_total",
        "Total HTTP requests",
        ["method", "endpoint", "status"],
    )
    REQUEST_LATENCY = Histogram(
        "ml_server_request_duration_seconds",
        "HTTP request latency",
        ["endpoint"],
    )
    FORECAST_COUNT = Counter(
        "ml_server_forecasts_total",
        "Total /forecast calls",
    )
    PREDICTION_TEMPERATURE = Histogram(
        "ml_prediction_temperature_celsius",
        "Distribution of predicted temperatures (°C)",
        buckets=[-20, -15, -10, -5, 0, 5, 10, 15, 20, 25, 30, 35],
    )
    MODEL_VERSION_GAUGE = Gauge(
        "ml_model_version_loaded",
        "Currently loaded registered model version (0 = .pth fallback)",
    )
    MODEL_VERSION_GAUGE.set(0)


class PredictionRequest(BaseModel):
    sin_doy: float
    cos_doy: float
    year_norm: float
    precip_log1p: float | None = None
    snow_log1p: float | None = None
    sunshine_frac_day: float | None = None
    wind_norm: float | None = None
    et0_norm: float | None = None


class PredictionResponse(BaseModel):
    sin_doy: float
    cos_doy: float
    year_norm: float
    temperature_c: float


class ForecastDay(BaseModel):
    date: str
    temperature_c: float


class ForecastResponse(BaseModel):
    start_date: str
    end_date: str
    days: list[ForecastDay]


MODEL_PATH = ML_OUTPUT_DIR / 'climate' / 'climate_model.pth'
_model_cache: ClimateModel | None = None
_model_version: int = 0
_feature_spec_cache = None


def get_feature_spec():
    global _feature_spec_cache
    if _feature_spec_cache is None:
        _feature_spec_cache = load_climate_feature_spec(MODEL_PATH.parent)
    return _feature_spec_cache


def get_model() -> ClimateModel | None:
    global _model_cache, _model_version
    if _model_cache is not None:
        return _model_cache

    feature_spec = get_feature_spec()

    # Try loading @champion from MLflow registry first
    if _mlflow_available and MLFLOW_TRACKING_URI:
        try:
            mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
            # Resolve alias → model version → run_id, then load via runs:/ URI.
            # MLflow 3 places downloaded artifacts one directory deeper than
            # load_model expects when using models:/ URIs, causing a missing
            # MLmodel error. Using runs:/{run_id}/{artifact_path} avoids this.
            alias_mv = MlflowClient().get_model_version_by_alias(
                MODEL_REGISTRY_NAME, MODEL_ALIAS
            )
            _model_version = int(alias_mv.version)
            run_id = alias_mv.run_id
            # source is e.g. "runs:/<run_id>/model" — extract artifact_path
            artifact_path = alias_mv.source.split('/', 2)[-1] if alias_mv.source.startswith('runs:/') else 'model'
            loaded = mlflow.pytorch.load_model(f'runs:/{run_id}/{artifact_path}')
            loaded.eval()
            _model_cache = attach_feature_spec(loaded, feature_spec)
            if _prometheus_available:
                MODEL_VERSION_GAUGE.set(_model_version)
            print(
                f'Loaded {MODEL_REGISTRY_NAME} v{_model_version} '
                f'from MLflow registry @{MODEL_ALIAS} (run {run_id[:8]})'
            )
            return _model_cache
        except Exception as _e:
            print(f'WARNING: registry load failed ({_e}); falling back to .pth')

    # Fallback: load from .pth file on disk
    if MODEL_PATH.exists():
        try:
            _model_cache = load_local_climate_model(MODEL_PATH, feature_spec)
            _model_version = 0
            if _prometheus_available:
                MODEL_VERSION_GAUGE.set(0)
            print('Loaded model from .pth fallback')
        except Exception as exc:
            _model_cache = None
            _model_version = 0
            print(f'WARNING: local model load failed ({exc})')
    return _model_cache


app = FastAPI(title='Lithuania Climate ML API')

app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:5173', 'http://127.0.0.1:5173', 'http://localhost', 'http://127.0.0.1'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

class RagQueryResponse(BaseModel):
    question: str
    answer: str
    interpretation: str = ''
    sources: list[dict]


FORECAST_QUERY_KEYWORDS = ('tomorrow', 'next week', 'forecast', 'predict', 'will it be', 'will the temp')


def _forecast_question_target(question: str) -> date | None:
    normalized = question.strip().lower()
    if not any(keyword in normalized for keyword in FORECAST_QUERY_KEYWORDS):
        return None

    if 'next week' in normalized:
        return date.today() + timedelta(days=7)

    date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', question)
    if date_match:
        try:
            return date.fromisoformat(date_match.group(0))
        except ValueError:
            pass

    return date.today() + timedelta(days=1)


def _recent_temperature_fallback(window_days: int = 7) -> tuple[float, int] | None:
    obs_path = ML_OUTPUT_DIR / 'weather' / 'country_daily_anomalies.csv'
    if not obs_path.exists():
        return None

    try:
        import pandas as pd
        df = pd.read_csv(obs_path, parse_dates=['time'])
    except Exception:
        return None

    df = df.dropna(subset=['temperature_2m_mean']).sort_values('time')
    if df.empty:
        return None

    recent = df.tail(window_days)
    return float(recent['temperature_2m_mean'].mean()), len(recent)


def _estimate_temperature_for_date(target_date: date) -> dict | None:
    model = get_model()
    if model is not None:
        x = build_input_tensor_for_date(target_date, get_feature_spec())
        try:
            with torch.no_grad():
                temperature_c = float(model(x).item())
            return {
                'temperature_c': temperature_c,
                'mode': 'model',
                'source': f'models:/{MODEL_REGISTRY_NAME}@{MODEL_ALIAS}' if _model_version != 0 else 'climate/climate_model.pth',
            }
        except Exception as exc:
            print(f'WARNING: model inference failed for {target_date.isoformat()} ({exc})')

    fallback = _recent_temperature_fallback()
    if fallback is None:
        return None

    temperature_c, observed_days = fallback
    return {
        'temperature_c': temperature_c,
        'mode': 'recent-observations',
        'source': 'weather/country_daily_anomalies.csv',
        'observed_days': observed_days,
    }


def _answer_direct_forecast_query(question: str) -> dict | None:
    target_date = _forecast_question_target(question)
    if target_date is None:
        return None

    estimate = _estimate_temperature_for_date(target_date)
    if estimate is None:
        return None

    rounded_temp = round(float(estimate['temperature_c']), 1)
    label = 'tomorrow' if target_date == date.today() + timedelta(days=1) else target_date.isoformat()

    if estimate['mode'] == 'model':
        answer = (
            f"For {label} ({target_date.isoformat()}) in Lithuania, the climate model estimate is {rounded_temp}°C. "
            f"This is a climatological temperature estimate from the trained model, not a live short-range weather forecast."
        )
        interpretation = f'Direct climate model estimate for {target_date.isoformat()}: {rounded_temp}°C.'
        source_title = 'Climate temperature model forecast'
    else:
        observed_days = int(estimate.get('observed_days', 0))
        answer = (
            f"For {label} ({target_date.isoformat()}) in Lithuania, the current fallback estimate is {rounded_temp}°C, "
            f"based on the average of the most recent {observed_days} observed days because the trained model artifact is not currently loadable. "
            f"Treat this as a short-term heuristic rather than a true model forecast."
        )
        interpretation = (
            f'Recent-observation fallback for {target_date.isoformat()}: {rounded_temp}°C '
            f'from the latest {observed_days} daily observations.'
        )
        source_title = 'Recent daily temperature observations'

    return {
        'question': question,
        'answer': answer,
        'interpretation': interpretation,
        'sources': [
            {
                'title': source_title,
                'source': str(estimate['source']),
                'score': 1.0,
            }
        ],
    }


def _heuristic_judge(answer: str, sources: list) -> float:
    """Fast rule-based quality score 0.0-5.0. No network calls, never blocks."""
    import re as _re
    if 'No relevant pipeline artifacts' in answer:
        return 0.0
    score = 2.5
    # Sources tier
    n_sources = len(sources) if sources else 0
    if n_sources >= 3:
        score += 1.0
    elif n_sources >= 1:
        score += 0.5
    # Length tier
    length = len(answer)
    if length > 500:
        score += 1.0
    elif length > 250:
        score += 0.5
    elif length < 100:
        score -= 1.0
    # Numeric evidence
    numbers = _re.findall(r'\b\d+(?:[.,]\d+)?(?:\s*%|\xb0C|mm|hPa)?\b', answer)
    if len(numbers) >= 3:
        score += 0.5
    elif len(numbers) >= 1:
        score += 0.25
    # Epistemic hedging
    hedges = ['likely', 'approximately', 'around', 'estimated', 'about', 'suggests']
    if any(h in answer.lower() for h in hedges):
        score += 0.25
    # Refusal
    refusals = ["i don't know", 'i do not know', 'cannot answer', 'no information']
    if any(r in answer.lower() for r in refusals):
        score -= 1.0
    return round(max(0.0, min(5.0, score)), 2)


def _run_rag_evaluation(question: str, answer: str, sources: list, trace_id: str | None = None) -> None:
    """Background task: score answer, log feedback on trace (Quality tab) and dataset to run (Datasets tab)."""
    if not (_mlflow_available and MLFLOW_TRACKING_URI):
        return
    try:
        import pandas as pd
        judge_score = _heuristic_judge(answer, sources)
        answered = "No relevant pipeline artifacts" not in answer
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

        # Quality tab: log feedback on the standalone trace
        if trace_id:
            try:
                mlflow.log_feedback(
                    trace_id=trace_id,
                    name="judge_score",
                    value=judge_score,
                    rationale="Heuristic: artifact presence, numeric content, answer length, uncertainty phrases",
                )
            except Exception as fb_exc:
                print(f"[eval] WARNING: log_feedback failed: {fb_exc}")

        # Datasets tab: log_input with a Dataset object inside a run
        df = pd.DataFrame({
            "question": [question],
            "answer": [answer],
            "judge_score": [judge_score],
            "sources": [", ".join(s.get("source", "") for s in sources)],
            "answered": [answered],
        })
        dataset = mlflow.data.from_pandas(df, name="rag_eval_dataset", source="rag_queries")
        with mlflow.start_run(run_name="rag-eval", tags={"type": "rag_evaluation"}):
            mlflow.log_input(dataset, context="eval")
            mlflow.log_metrics({
                "answered": float(answered),
                "source_count": float(len(sources)),
                "answer_length": float(len(answer)),
                "judge_score": judge_score,
            })
        print(f"[eval] logged judge_score={judge_score} answered={answered} trace_id={trace_id}")
    except Exception as exc:
        print(f"WARNING: MLflow eval logging failed: {exc}")


@app.get('/rag/query', response_model=RagQueryResponse)
def rag_query(q: str = '', background_tasks: BackgroundTasks = None):
    q = q.strip()
    if not q:
        raise HTTPException(status_code=400, detail='Query parameter "q" is required')
    result = _answer_direct_forecast_query(q) or answer_question(q, ML_OUTPUT_DIR, top_k=5)
    answer_text = result.get('answer', '')
    sources = result.get('sources', [])
    answered = 'No relevant pipeline artifacts' not in answer_text
    trace_id = None
    if _mlflow_available and MLFLOW_TRACKING_URI:
        with mlflow.start_span(name='rag_query') as span:
            span.set_inputs({'question': q})
            span.set_outputs({'answer': answer_text[:500]})
            span.set_attribute('answered', answered)
            span.set_attribute('source_count', len(sources))
            span.set_attribute('answer_length', len(answer_text))
        trace_id = mlflow.get_last_active_trace_id()
        if background_tasks is not None:
            background_tasks.add_task(_run_rag_evaluation, q, answer_text, sources, trace_id)
    return result


@app.get('/')
def health():
    return {'status': 'ok', 'model_loaded': get_model() is not None}


@app.get('/dashboard')
def dashboard():
    """Serve the latest dashboard.json produced by the export pipeline."""
    dashboard_path = ML_OUTPUT_DIR.parent / 'src' / 'data' / 'dashboard.json'
    if not dashboard_path.exists():
        # Fall back to output dir
        dashboard_path = ML_OUTPUT_DIR / 'dashboard.json'
    if not dashboard_path.exists():
        raise HTTPException(status_code=404, detail='Dashboard data not yet generated')
    with open(dashboard_path, encoding='utf-8') as f:
        return json.load(f)


@app.post('/predict', response_model=PredictionResponse)
def predict(req: PredictionRequest):
    model = get_model()
    if model is None:
        raise HTTPException(status_code=503, detail='Model not loaded')

    x = build_input_tensor(get_feature_spec(), req.model_dump(exclude_none=True))
    with torch.no_grad():
        temperature_c = float(model(x).item())

    if _prometheus_available:
        PREDICTION_TEMPERATURE.observe(temperature_c)

    result = {
        'sin_doy': req.sin_doy,
        'cos_doy': req.cos_doy,
        'year_norm': req.year_norm,
        'temperature_c': temperature_c,
    }
    if _mlflow_available and MLFLOW_TRACKING_URI:
        with mlflow.start_span(name='predict') as span:
            span.set_inputs({'sin_doy': req.sin_doy, 'cos_doy': req.cos_doy, 'year_norm': req.year_norm})
            span.set_outputs({'temperature_c': temperature_c})
    return result


def _date_to_features(d: date) -> tuple[float, float, float]:
    """Convert a calendar date to (sin_doy, cos_doy, year_norm) model inputs."""
    doy = d.timetuple().tm_yday
    sin_doy = math.sin(2 * math.pi * doy / 365)
    cos_doy = math.cos(2 * math.pi * doy / 365)
    year_norm = (d.year - 1991) / 30.0
    return sin_doy, cos_doy, year_norm


@app.get('/forecast', response_model=ForecastResponse)
def forecast(
    start: str = Query(default=None, description="Start date YYYY-MM-DD (default: today)"),
    days: int = Query(default=7, ge=1, le=365, description="Number of days to forecast"),
):
    """Return model temperature predictions for a date range.

    Example: GET /forecast?start=2026-07-01&days=14
    """
    try:
        start_date = date.fromisoformat(start) if start else date.today()
    except ValueError:
        raise HTTPException(status_code=422, detail='start must be YYYY-MM-DD')

    end_date = start_date + timedelta(days=days - 1)

    if _prometheus_available:
        FORECAST_COUNT.inc()

    results: list[ForecastDay] = []
    for i in range(days):
        d = start_date + timedelta(days=i)
        estimate = _estimate_temperature_for_date(d)
        if estimate is None:
            raise HTTPException(status_code=503, detail='Model not loaded')
        temp = float(estimate['temperature_c'])
        if _prometheus_available:
            PREDICTION_TEMPERATURE.observe(temp)
        results.append(ForecastDay(date=d.isoformat(), temperature_c=round(temp, 2)))

    return ForecastResponse(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        days=results,
    )


@app.get('/forecast/tomorrow', response_model=ForecastResponse)
def forecast_tomorrow():
    """Return the model's temperature prediction for tomorrow.

    Convenience shortcut for GET /forecast?start=<tomorrow>&days=1
    """
    tomorrow = date.today() + timedelta(days=1)
    estimate = _estimate_temperature_for_date(tomorrow)
    if estimate is None:
        raise HTTPException(status_code=503, detail='Model not loaded')
    temp = float(estimate['temperature_c'])
    if _prometheus_available:
        PREDICTION_TEMPERATURE.observe(temp)
        FORECAST_COUNT.inc()
    day = ForecastDay(date=tomorrow.isoformat(), temperature_c=round(temp, 2))
    return ForecastResponse(
        start_date=tomorrow.isoformat(),
        end_date=tomorrow.isoformat(),
        days=[day],
    )


@app.get('/metrics')
def metrics():
    """Prometheus metrics endpoint."""
    if not _prometheus_available:
        raise HTTPException(status_code=501, detail='prometheus_client not installed')
    return StarletteResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)
