"""Lightweight regression metrics used by evaluation scripts."""

from __future__ import annotations

import numpy as np


def mean_squared_error(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    residuals = np.asarray(y_true, dtype=np.float32) - np.asarray(y_pred, dtype=np.float32)
    return float(np.mean(residuals ** 2))


def mean_absolute_error(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    residuals = np.asarray(y_true, dtype=np.float32) - np.asarray(y_pred, dtype=np.float32)
    return float(np.mean(np.abs(residuals)))


def r2_score(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    actual = np.asarray(y_true, dtype=np.float32)
    predicted = np.asarray(y_pred, dtype=np.float32)
    ss_res = float(np.sum((actual - predicted) ** 2))
    centered = actual - float(np.mean(actual))
    ss_tot = float(np.sum(centered ** 2))
    if ss_tot == 0.0:
        return 0.0
    return float(1.0 - (ss_res / ss_tot))