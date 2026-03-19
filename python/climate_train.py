"""Train a ClimateModel MLP on real ERA5 Lithuania daily temperature data.

The model learns to predict daily mean temperature (°C) from three features:
  sin_doy, cos_doy  — sinusoidal day-of-year encoding (annual seasonality)
  year_norm         — normalised year (long-term warming trend)

Training data is the chronological training split produced by climate_data.py.
If MLflow is installed, params and per-epoch MSE are logged; otherwise training
runs without experiment tracking.

Usage:
  python python/climate_train.py --epochs 100 --lr 0.001
"""

from __future__ import annotations

import argparse
import csv
import os
from contextlib import nullcontext

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim

from model import ClimateModel

try:
    import mlflow
except Exception:  # pragma: no cover - optional dependency path
    mlflow = None


def train(
    train_path: str,
    epochs: int,
    lr: float,
    batch_size: int,
    tracking_uri: str,
    model_path: str,
    metrics_path: str,
) -> None:
    if mlflow is not None:
        mlflow.set_tracking_uri(tracking_uri)

    df = pd.read_csv(train_path)
    X = df[['sin_doy', 'cos_doy', 'year_norm']].to_numpy(dtype=np.float32)
    y = df['y'].to_numpy(dtype=np.float32).reshape(-1, 1)

    X_t = torch.from_numpy(X)
    y_t = torch.from_numpy(y)

    model = ClimateModel()
    optimizer = optim.Adam(model.parameters(), lr=lr)
    criterion = nn.MSELoss()

    run_ctx = mlflow.start_run(run_name='train-climate-model') if mlflow is not None else nullcontext()
    with run_ctx:
        if mlflow is not None:
            mlflow.log_params({
                'epochs': epochs,
                'lr': lr,
                'batch_size': batch_size,
                'features': 'sin_doy,cos_doy,year_norm',
                'dataset': 'ERA5-Lithuania-country-daily',
                'train_rows': len(df),
            })
        metrics = []
        for epoch in range(1, epochs + 1):
            perm = torch.randperm(X_t.size(0))
            epoch_loss = 0.0

            for i in range(0, X_t.size(0), batch_size):
                idx = perm[i: i + batch_size]
                xb, yb = X_t[idx], y_t[idx]
                optimizer.zero_grad()
                loss = criterion(model(xb), yb)
                loss.backward()
                optimizer.step()
                epoch_loss += loss.item() * xb.size(0)

            epoch_loss /= X_t.size(0)
            if mlflow is not None:
                mlflow.log_metric('mse', epoch_loss, step=epoch)
            metrics.append({'epoch': epoch, 'mse': epoch_loss})

        os.makedirs(os.path.dirname(model_path) or '.', exist_ok=True)
        torch.save(model.state_dict(), model_path)
        if mlflow is not None:
            mlflow.log_artifact(model_path)

        os.makedirs(os.path.dirname(metrics_path) or '.', exist_ok=True)
        with open(metrics_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['epoch', 'mse'])
            writer.writeheader()
            writer.writerows(metrics)

    print(f'Training complete. Model saved to {model_path}')
    print(f'Metrics written to {metrics_path}')
    if mlflow is None:
        print('MLflow not installed; skipping experiment logging.')


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Train a ClimateModel MLP on real ERA5 daily weather data'
    )
    parser.add_argument(
        '--train-data',
        type=str,
        default='python/output/climate/climate_train.csv',
    )
    parser.add_argument('--epochs', type=int, default=100)
    parser.add_argument('--lr', type=float, default=0.001)
    parser.add_argument('--batch-size', type=int, default=128)
    parser.add_argument('--tracking-uri', type=str, default='./mlruns')
    parser.add_argument(
        '--model-path', type=str, default='python/output/climate/climate_model.pth'
    )
    parser.add_argument(
        '--metrics-path', type=str, default='python/output/climate/climate_metrics.csv'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Verify imports only; do not run training.',
    )
    args = parser.parse_args()

    if args.dry_run:
        print('Dry run OK: ClimateModel imported successfully.')
        return

    train(
        args.train_data,
        args.epochs,
        args.lr,
        args.batch_size,
        args.tracking_uri,
        args.model_path,
        args.metrics_path,
    )


if __name__ == '__main__':
    main()
