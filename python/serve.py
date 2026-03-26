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
    from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
    from starlette.responses import Response as StarletteResponse
    _prometheus_available = True
except ImportError:
    _prometheus_available = False

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


def get_model() -> ClimateModel | None:
    global _model_cache
    if _model_cache is None and MODEL_PATH.exists():
        _model_cache = ClimateModel()
        _model_cache.load_state_dict(torch.load(str(MODEL_PATH), weights_only=True))
        _model_cache.eval()
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
        temperature_c = model(x).item()

    return {'sin_doy': req.sin_doy, 'cos_doy': req.cos_doy, 'year_norm': req.year_norm, 'temperature_c': float(temperature_c)}


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
            temp = model(x).item()
        results.append(ForecastDay(date=d.isoformat(), temperature_c=round(float(temp), 2)))

    return ForecastResponse(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        days=results,
    )


@app.get('/metrics')
def metrics():
    """Prometheus metrics endpoint."""
    if not _prometheus_available:
        raise HTTPException(status_code=501, detail='prometheus_client not installed')
    return StarletteResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)
