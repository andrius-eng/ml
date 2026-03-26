"""Shared model definitions and data utilities."""

from __future__ import annotations

import numpy as np
import torch
import torch.nn as nn


class LinearModel(nn.Module):
    def __init__(self):
        super().__init__()
        self.linear = nn.Linear(1, 1)

    def forward(self, x):
        return self.linear(x)


class ClimateModel(nn.Module):
    """MLP that predicts daily mean temperature (°C) from temporal features.

    Inputs (3 features per time-step):
        sin_doy   — sin(2π · day_of_year / 365)  captures annual seasonality
        cos_doy   — cos(2π · day_of_year / 365)  captures annual seasonality
        year_norm — (year − 1991) / 30            captures long-term trend

    Architecture: deep residual MLP with batch normalisation and dropout.
    The skip connection lets the model learn residuals on top of a linear
    seasonal baseline, which stabilises training and improves generalisation.

    Output: predicted daily mean temperature in °C
    """

    def __init__(self, dropout: float = 0.1):
        super().__init__()
        # Shallow linear projection used as residual bypass
        self.skip = nn.Linear(3, 1)

        # Deep branch: BatchNorm → Linear → ReLU → Dropout, ×3
        self.block = nn.Sequential(
            nn.BatchNorm1d(3),
            nn.Linear(3, 128),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
        )

    def forward(self, x):
        return self.block(x) + self.skip(x)


def make_synthetic_data(n_samples: int = 500):
    rng = np.random.default_rng(42)
    x = rng.normal(size=(n_samples, 1)).astype(np.float32)
    y = 2 * x + 1 + rng.normal(0, 0.2, size=(n_samples, 1)).astype(np.float32)
    return x, y
