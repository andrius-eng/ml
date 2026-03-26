"""A minimal FastAPI app to serve predictions from the trained model."""

from __future__ import annotations

from pathlib import Path

import torch
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from model import ClimateModel

import json
import os
import sys

# Make sibling modules importable when running from outside python/
sys.path.insert(0, str(Path(__file__).resolve().parent))

from rag_pipeline import answer_question

ML_OUTPUT_DIR = Path(os.environ.get("ML_OUTPUT_DIR", "python/output"))


class PredictionRequest(BaseModel):
    sin_doy: float
    cos_doy: float
    year_norm: float


class PredictionResponse(BaseModel):
    sin_doy: float
    cos_doy: float
    year_norm: float
    temperature_c: float


MODEL_PATH = ML_OUTPUT_DIR / 'climate' / 'climate_model.pth'
_model_cache: ClimateModel | None = None


def get_model() -> ClimateModel | None:
    global _model_cache
    if _model_cache is None and MODEL_PATH.exists():
        _model_cache = ClimateModel()
        _model_cache.load_state_dict(torch.load(str(MODEL_PATH), weights_only=True))
        _model_cache.eval()
    return _model_cache


app = FastAPI(title='Torch + MLflow Demo')

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
