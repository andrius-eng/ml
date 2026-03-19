"""Evaluate ClimateModel on the held-out test set and persist analysis artifacts."""

from __future__ import annotations

import argparse
import csv
import json
import os

import numpy as np
import pandas as pd
import torch

from metrics import mean_absolute_error, mean_squared_error, r2_score
from model import ClimateModel


def evaluate(
    model_path: str,
    test_path: str,
    summary_path: str | None = None,
    predictions_path: str | None = None,
) -> dict:
    model = ClimateModel()
    model.load_state_dict(torch.load(model_path, weights_only=True))
    model.eval()

    df = pd.read_csv(test_path)
    X = df[['sin_doy', 'cos_doy', 'year_norm']].to_numpy(dtype=np.float32)
    y_true = df['y'].to_numpy(dtype=np.float32)

    with torch.no_grad():
        y_pred = model(torch.from_numpy(X)).numpy().reshape(-1)

    residuals = y_true - y_pred

    metrics = {
        'mse': float(mean_squared_error(y_true, y_pred)),
        'rmse': float(mean_squared_error(y_true, y_pred) ** 0.5),
        'mae': float(mean_absolute_error(y_true, y_pred)),
        'r2': float(r2_score(y_true, y_pred)),
        'residual_mean': float(np.mean(residuals)),
        'residual_std': float(np.std(residuals)),
    }

    if predictions_path:
        os.makedirs(os.path.dirname(predictions_path) or '.', exist_ok=True)
        with open(predictions_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['y_true', 'y_pred', 'residual'])
            writer.writeheader()
            for yt, yp, r in zip(y_true, y_pred, residuals):
                writer.writerow({
                    'y_true': float(yt),
                    'y_pred': float(yp),
                    'residual': float(r),
                })

    if summary_path:
        os.makedirs(os.path.dirname(summary_path) or '.', exist_ok=True)
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(metrics, f, indent=2, sort_keys=True)

    for k, v in metrics.items():
        print(f'{k.upper()}: {v:.6f}')

    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Evaluate ClimateModel on held-out test data'
    )
    parser.add_argument(
        '--model', type=str, default='python/output/climate/climate_model.pth'
    )
    parser.add_argument(
        '--test-data', type=str, default='python/output/climate/climate_test.csv'
    )
    parser.add_argument(
        '--summary-json',
        type=str,
        default='python/output/climate/climate_evaluation.json',
    )
    parser.add_argument(
        '--predictions-csv',
        type=str,
        default='python/output/climate/climate_predictions.csv',
    )
    args = parser.parse_args()

    evaluate(args.model, args.test_data, args.summary_json, args.predictions_csv)


if __name__ == '__main__':
    main()
