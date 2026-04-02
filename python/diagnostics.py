"""Create diagnostic plots from evaluation predictions."""

from __future__ import annotations

import argparse
import os
from contextlib import nullcontext

import matplotlib


matplotlib.use('Agg')

import matplotlib.pyplot as plt
import pandas as pd

try:
    import mlflow
except Exception:
    mlflow = None


def _resume_run(run_id_path: str):
    if mlflow is None:
        return nullcontext()
    try:
        with open(run_id_path) as _f:
            run_id = _f.read().strip()
        tracking_uri = os.environ.get('MLFLOW_TRACKING_URI', '')
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
        return mlflow.start_run(run_id=run_id)
    except Exception:
        return nullcontext()


def main():
    parser = argparse.ArgumentParser(description='Create residual and parity plots from predictions')
    parser.add_argument(
        '--predictions',
        type=str,
        default='python/output/predictions.csv',
        help='CSV file with y_true, y_pred, and residual columns',
    )
    parser.add_argument(
        '--output',
        type=str,
        default='python/output/diagnostics.png',
        help='Where to save the diagnostic figure',
    )
    args = parser.parse_args()

    df = pd.read_csv(args.predictions)

    fig, axes = plt.subplots(1, 2, figsize=(10, 4))

    axes[0].scatter(df['y_true'], df['y_pred'], alpha=0.7)
    min_value = min(df['y_true'].min(), df['y_pred'].min())
    max_value = max(df['y_true'].max(), df['y_pred'].max())
    axes[0].plot([min_value, max_value], [min_value, max_value], linestyle='--')
    axes[0].set_title('Predicted vs Actual')
    axes[0].set_xlabel('Actual')
    axes[0].set_ylabel('Predicted')

    axes[1].hist(df['residual'], bins=20, alpha=0.8)
    axes[1].set_title('Residual Distribution')
    axes[1].set_xlabel('Residual')
    axes[1].set_ylabel('Count')

    fig.tight_layout()
    fig.savefig(args.output, dpi=150)
    print(f'Saved diagnostics plot to {args.output}')

    # Log the plot to the shared MLflow run
    run_id_path = os.path.join(os.path.dirname(args.predictions) or '.', 'mlflow_run_id.txt')
    with _resume_run(run_id_path) as active_run:
        if mlflow is not None and active_run is not None:
            try:
                mlflow.log_artifact(args.output, artifact_path='plots')
            except Exception as e:
                print(f'WARNING: could not log diagnostics plot to MLflow ({e})')


if __name__ == '__main__':
    main()