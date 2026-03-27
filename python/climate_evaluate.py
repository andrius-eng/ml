"""Evaluate ClimateModel on the held-out test set and persist analysis artifacts."""

from __future__ import annotations

import argparse
import csv
import json
import os
from contextlib import nullcontext

import numpy as np
import pandas as pd
import torch

from metrics import mean_absolute_error, mean_squared_error, r2_score
from model import ClimateModel

try:
    import mlflow
except Exception:
    mlflow = None


def _resume_run(run_id_path: str):
    """Return a context manager that resumes the shared MLflow run, or nullcontext."""
    if mlflow is None:
        return nullcontext()
    try:
        with open(run_id_path) as _f:
            run_id = _f.read().strip()
        tracking_uri = os.environ.get('MLFLOW_TRACKING_URI', '')
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
        return mlflow.start_run(run_id=run_id)
    except Exception as e:
        print(f'WARNING: could not resume MLflow run ({e}); evaluation metrics will not be logged')
        return nullcontext()


def evaluate(
    model_path: str,
    test_path: str,
    summary_path: str | None = None,
    predictions_path: str | None = None,
) -> dict:
    df = pd.read_csv(test_path)
    feature_cols = [c for c in df.columns if c != 'y']
    model = ClimateModel(input_dim=len(feature_cols))
    model.load_state_dict(torch.load(model_path, weights_only=True))
    model.eval()

    X = df[feature_cols].to_numpy(dtype=np.float32)
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

    metrics = evaluate(args.model, args.test_data, args.summary_json, args.predictions_csv)

    # Log test metrics and artifacts to the same MLflow run started by climate_train.py
    run_id_path = os.path.join(os.path.dirname(args.model) or '.', 'mlflow_run_id.txt')
    with _resume_run(run_id_path) as active_run:
        if mlflow is not None and active_run is not None:
            mlflow.log_metrics({f'test_{k}': v for k, v in metrics.items()})
            mlflow.set_tag('stage', 'evaluated')
            try:
                mlflow.log_artifact(args.predictions_csv, artifact_path='evaluation')
                mlflow.log_artifact(args.summary_json, artifact_path='evaluation')
            except Exception as e:
                print(f'WARNING: could not log evaluation artifacts to MLflow ({e})')


if __name__ == '__main__':
    main()
