"""A minimal FastAPI app to serve predictions from the trained model."""

from __future__ import annotations

import math
import time
from datetime import date, timedelta
from pathlib import Path

import torch
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from model import ClimateModel

import json
import os
import sys

# Make sibling modules importable when running from outside python/
sys.path.insert(0, str(Path(__file__).resolve().parent))

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
MODEL_REGISTRY_NAME = 'ClimateTemperatureModel'
MODEL_ALIAS = 'champion'

if _mlflow_available and MLFLOW_TRACKING_URI:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
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


def get_model() -> ClimateModel | None:
    global _model_cache, _model_version
    if _model_cache is not None:
        return _model_cache

    # Try loading @champion from MLflow registry first
    if _mlflow_available and MLFLOW_TRACKING_URI:
        try:
            mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
            loaded = mlflow.pytorch.load_model(
                f'models:/{MODEL_REGISTRY_NAME}@{MODEL_ALIAS}'
            )
            loaded.eval()
            _model_cache = loaded
            try:
                alias_mv = MlflowClient().get_model_version_by_alias(
                    MODEL_REGISTRY_NAME, MODEL_ALIAS
                )
                _model_version = int(alias_mv.version)
            except Exception:
                _model_version = -1
            if _prometheus_available:
                MODEL_VERSION_GAUGE.set(_model_version)
            print(
                f'Loaded {MODEL_REGISTRY_NAME} v{_model_version} '
                f'from MLflow registry @{MODEL_ALIAS}'
            )
            return _model_cache
        except Exception as _e:
            print(f'WARNING: registry load failed ({_e}); falling back to .pth')

    # Fallback: load from .pth file on disk
    if MODEL_PATH.exists():
        _model_cache = ClimateModel()
        _model_cache.load_state_dict(torch.load(str(MODEL_PATH), weights_only=True))
        _model_cache.eval()
        _model_version = 0
        if _prometheus_available:
            MODEL_VERSION_GAUGE.set(0)
        print('Loaded model from .pth fallback')
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


@app.get('/rag/query', response_model=RagQueryResponse)
def rag_query(q: str = ''):
    q = q.strip()
    if not q:
        raise HTTPException(status_code=400, detail='Query parameter "q" is required')
    if _mlflow_available and MLFLOW_TRACKING_URI:
        with mlflow.start_span(name='rag_query') as span:
            span.set_inputs({'question': q})
            result = answer_question(q, ML_OUTPUT_DIR)
            span.set_outputs({'answer': result.get('answer', '')[:500]})
            return result
    return answer_question(q, ML_OUTPUT_DIR)


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

    x = torch.tensor([[req.sin_doy, req.cos_doy, req.year_norm]], dtype=torch.float32)
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
    model = get_model()
    if model is None:
        raise HTTPException(status_code=503, detail='Model not loaded')

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
        sin_doy, cos_doy, year_norm = _date_to_features(d)
        x = torch.tensor([[sin_doy, cos_doy, year_norm]], dtype=torch.float32)
        with torch.no_grad():
            temp = float(model(x).item())
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
    model = get_model()
    if model is None:
        raise HTTPException(status_code=503, detail='Model not loaded')
    sin_doy, cos_doy, year_norm = _date_to_features(tomorrow)
    x = torch.tensor([[sin_doy, cos_doy, year_norm]], dtype=torch.float32)
    with torch.no_grad():
        temp = float(model(x).item())
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
