"""Train a ClimateModel MLP on real ERA5 Lithuania daily temperature data.

The model learns to predict daily mean temperature (°C) from the ordered feature
manifest produced by climate_data.py. The core temporal features are always:
    sin_doy, cos_doy  — sinusoidal day-of-year encoding (annual seasonality)
    year_norm         — normalised year (long-term warming trend)

When weather-derived columns are present, the same feature contract may also
include precip_log1p, snow_log1p, sunshine_frac_day, wind_norm, and et0_norm.

Training data is the chronological training split produced by climate_data.py.

MLflow integration
------------------
``mlflow>=3.0.0`` is required (listed in requirements-airflow-runtime.txt).

* ``--tracking-uri`` must be an HTTP URL (e.g. ``http://mlflow:5000``).
  A local file path ``./mlruns`` is the CLI default but will not work inside
  a Docker worker container without a shared volume.
* MLflow 3.x does **not** auto-create the default experiment (ID 0).
  The experiment ``climate-temperature-model`` is created explicitly via
  ``mlflow.set_experiment()`` before every run.
* Training params and per-epoch MSE are always logged to MLflow when the
  package is importable.
* The PyTorch flavor artifact is logged to the run first, then the pipeline
    explicitly ensures a registered model version exists for that run.
* The local ``.pth`` file is still always written to ``--model-path`` even if
    MLflow model logging or registry operations fail.

Usage:
  python python/climate_train.py --epochs 100 --lr 0.001
"""

from __future__ import annotations

import argparse
import csv
import os
from contextlib import nullcontext
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
import torch.optim.lr_scheduler as lr_scheduler

from climate_model_contract import instantiate_climate_model, resolve_feature_spec_from_frame
from mlflow_model_registry import (
    MODEL_ARTIFACT_PATH,
    MODEL_REGISTRY_NAME,
    ensure_model_version_for_run,
    get_client,
    set_model_version_tags,
)

try:
    import mlflow
    from mlflow.models import infer_signature
    import mlflow.pytorch
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
        mlflow.set_experiment('climate-temperature-model')

    df = pd.read_csv(train_path)
    feature_spec = resolve_feature_spec_from_frame(Path(model_path).parent, df)
    feature_cols = feature_spec.columns
    X = df[feature_cols].to_numpy(dtype=np.float32)
    y = df['y'].to_numpy(dtype=np.float32).reshape(-1, 1)

    X_t = torch.from_numpy(X)
    y_t = torch.from_numpy(y)

    model = instantiate_climate_model(feature_spec, dropout=0.1)
    optimizer = optim.Adam(model.parameters(), lr=lr)
    scheduler = lr_scheduler.CosineAnnealingLR(optimizer, T_max=epochs, eta_min=lr * 0.01)
    criterion = nn.MSELoss()
    input_example = df[feature_cols].head(5).copy()

    run_ctx = mlflow.start_run(run_name='train-climate-model') if mlflow is not None else nullcontext()
    with run_ctx as active_run:
        if mlflow is not None:
            mlflow.log_params({
                'epochs': epochs,
                'lr': lr,
                'batch_size': batch_size,
                'features': ','.join(feature_cols),
                'feature_count': len(feature_cols),
                'dataset': 'ERA5-Lithuania-country-daily',
                'train_rows': len(df),
            })
            mlflow.set_tags({'stage': 'training', 'framework': 'pytorch'})
            try:
                mlflow.log_dict(feature_spec.columns, 'model_contract/feature_columns.json')
                mlflow.log_dict(feature_spec.defaults, 'model_contract/feature_defaults.json')
            except Exception as exc:
                print(f'WARNING: could not log model contract artifacts to MLflow ({exc})')
            # Write run_id so downstream tasks (evaluate, plot, quality_gate) can resume this run
            run_id_path = os.path.join(os.path.dirname(model_path) or '.', 'mlflow_run_id.txt')
            os.makedirs(os.path.dirname(run_id_path) or '.', exist_ok=True)
            with open(run_id_path, 'w') as _f:
                _f.write(active_run.info.run_id)
        metrics = []
        for epoch in range(1, epochs + 1):
            model.train()
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
            scheduler.step()
            # Skip epoch 1: random-init spike inflates Y-axis and obscures real convergence
            if mlflow is not None and epoch > 1:
                mlflow.log_metric('mse', epoch_loss, step=epoch)
                mlflow.log_metric('lr', scheduler.get_last_lr()[0], step=epoch)
            metrics.append({'epoch': epoch, 'mse': epoch_loss})

        os.makedirs(os.path.dirname(model_path) or '.', exist_ok=True)
        torch.save(model.state_dict(), model_path)
        if mlflow is not None:
            try:
                model.eval()
                with torch.no_grad():
                    example_outputs = model(
                        torch.from_numpy(input_example.to_numpy(dtype=np.float32))
                    ).cpu().numpy()
                signature = infer_signature(input_example, example_outputs)

                # Log the PyTorch flavor artifact, then create/find the registry version explicitly.
                mlflow.pytorch.log_model(
                    model,
                    artifact_path=MODEL_ARTIFACT_PATH,
                    input_example=input_example,
                    signature=signature,
                )
                client = get_client(tracking_uri)
                if client is not None and active_run is not None:
                    version = ensure_model_version_for_run(
                        client,
                        active_run.info.run_id,
                        model_name=MODEL_REGISTRY_NAME,
                        artifact_path=MODEL_ARTIFACT_PATH,
                    )
                    if version is not None:
                        set_model_version_tags(
                            client,
                            MODEL_REGISTRY_NAME,
                            str(version.version),
                            {
                                'run_id': active_run.info.run_id,
                                'feature_count': str(len(feature_cols)),
                                'features': ','.join(feature_cols),
                                'artifact_path': MODEL_ARTIFACT_PATH,
                            },
                        )
                        version_path = os.path.join(os.path.dirname(model_path) or '.', 'mlflow_model_version.txt')
                        with open(version_path, 'w', encoding='utf-8') as handle:
                            handle.write(str(version.version))
                        print(f'Registered {MODEL_REGISTRY_NAME} v{version.version} for run {active_run.info.run_id}')
            except Exception as e:
                print(f'WARNING: could not log/register pytorch model in MLflow ({e}); trying log_artifact fallback ...')
                try:
                    mlflow.log_artifact(model_path)
                except Exception as e2:
                    print(f'WARNING: artifact logging failed entirely ({e2}); model already saved to {model_path}')

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
