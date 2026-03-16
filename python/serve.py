"""A minimal FastAPI app to serve predictions from the trained model."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import torch
import torch.nn as nn
from fastapi import FastAPI
from pydantic import BaseModel


class LinearModel(nn.Module):
    def __init__(self):
        super().__init__()
        self.linear = nn.Linear(1, 1)

    def forward(self, x):
        return self.linear(x)


class PredictionRequest(BaseModel):
    x: float


class PredictionResponse(BaseModel):
    x: float
    y: float


def load_model(model_path: str) -> nn.Module:
    model = LinearModel()
    model.load_state_dict(torch.load(model_path))
    model.eval()
    return model


app = FastAPI(title='Torch + MLflow Demo')

MODEL_PATH = Path('python/output/model.pth')
MODEL = load_model(str(MODEL_PATH)) if MODEL_PATH.exists() else None


@app.get('/')
def health():
    return {'status': 'ok', 'model_loaded': MODEL is not None}


@app.post('/predict', response_model=PredictionResponse)
def predict(req: PredictionRequest):
    if MODEL is None:
        return {'x': req.x, 'y': None}

    x = torch.tensor([[req.x]], dtype=torch.float32)
    with torch.no_grad():
        y_pred = MODEL(x).item()

    return {'x': req.x, 'y': float(y_pred)}
