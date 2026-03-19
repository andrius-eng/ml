"""A minimal FastAPI app to serve predictions from the trained model."""

from __future__ import annotations

from pathlib import Path

import torch
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from model import LinearModel

import os
import sys

# Make sibling modules importable when running from outside python/
sys.path.insert(0, str(Path(__file__).resolve().parent))

from rag_pipeline import answer_question

ML_OUTPUT_DIR = Path(os.environ.get("ML_OUTPUT_DIR", "python/output"))


class PredictionRequest(BaseModel):
    x: float


class PredictionResponse(BaseModel):
    x: float
    y: float


MODEL_PATH = Path('python/output/model.pth')
_model_cache: LinearModel | None = None


def get_model() -> LinearModel | None:
    global _model_cache
    if _model_cache is None and MODEL_PATH.exists():
        _model_cache = LinearModel()
        _model_cache.load_state_dict(torch.load(str(MODEL_PATH), weights_only=True))
        _model_cache.eval()
    return _model_cache


app = FastAPI(title='Torch + MLflow Demo')


class RagQueryResponse(BaseModel):
    question: str
    answer: str
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


@app.post('/predict', response_model=PredictionResponse)
def predict(req: PredictionRequest):
    model = get_model()
    if model is None:
        raise HTTPException(status_code=503, detail='Model not loaded')

    x = torch.tensor([[req.x]], dtype=torch.float32)
    with torch.no_grad():
        y_pred = model(x).item()

    return {'x': req.x, 'y': float(y_pred)}
