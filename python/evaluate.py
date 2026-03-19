"""Evaluate a saved model and persist analysis artifacts."""

from __future__ import annotations

import argparse
import csv
import json
import os

import numpy as np
import pandas as pd
import torch

from metrics import mean_absolute_error, mean_squared_error, r2_score
from model import LinearModel


def load_data(path: str):
    df = pd.read_csv(path)
    x = df['x'].to_numpy(dtype=np.float32).reshape(-1, 1)
    y = df['y'].to_numpy(dtype=np.float32).reshape(-1, 1)
    return x, y


def evaluate(
    model_path: str,
    data_path: str,
    summary_path: str | None = None,
    predictions_path: str | None = None,
):
    model = LinearModel()
    model.load_state_dict(torch.load(model_path, weights_only=True))
    model.eval()

    x, y = load_data(data_path)
    with torch.no_grad():
        preds = model(torch.from_numpy(x)).numpy()

    y_true = y.reshape(-1)
    y_pred = preds.reshape(-1)
    residuals = y_true - y_pred

    metrics = {
        'mse': mean_squared_error(y_true, y_pred),
        'rmse': mean_squared_error(y_true, y_pred) ** 0.5,
        'mae': mean_absolute_error(y_true, y_pred),
        'r2': r2_score(y_true, y_pred),
        'residual_mean': float(np.mean(residuals)),
        'residual_std': float(np.std(residuals)),
    }

    if predictions_path:
        os.makedirs(os.path.dirname(predictions_path) or '.', exist_ok=True)
        with open(predictions_path, 'w', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=['x', 'y_true', 'y_pred', 'residual'])
            writer.writeheader()
            for features, actual, predicted, residual in zip(x.reshape(-1), y_true, y_pred, residuals):
                writer.writerow(
                    {
                        'x': float(features),
                        'y_true': float(actual),
                        'y_pred': float(predicted),
                        'residual': float(residual),
                    }
                )

    if summary_path:
        os.makedirs(os.path.dirname(summary_path) or '.', exist_ok=True)
        with open(summary_path, 'w', encoding='utf-8') as handle:
            json.dump(metrics, handle, indent=2, sort_keys=True)

    for metric_name, metric_value in metrics.items():
        print(f'{metric_name.upper()}: {metric_value:.6f}')

    print(json.dumps(metrics, sort_keys=True))
    return metrics


def main():
    parser = argparse.ArgumentParser(description='Evaluate a saved model on CSV data')
    parser.add_argument('--model', type=str, default='python/output/model.pth', help='Saved model path')
    parser.add_argument('--data', type=str, default='python/data.csv', help='CSV data path')
    parser.add_argument(
        '--summary-json',
        type=str,
        default='python/output/evaluation.json',
        help='Where to save aggregate evaluation metrics as JSON',
    )
    parser.add_argument(
        '--predictions-csv',
        type=str,
        default='python/output/predictions.csv',
        help='Where to save per-row predictions and residuals',
    )
    args = parser.parse_args()

    evaluate(
        args.model,
        args.data,
        summary_path=args.summary_json,
        predictions_path=args.predictions_csv,
    )


if __name__ == '__main__':
    main()
