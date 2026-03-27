"""Plot training metrics saved during training."""

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
    parser = argparse.ArgumentParser(description='Plot training metrics')
    parser.add_argument(
        '--metrics',
        type=str,
        default='python/output/metrics.csv',
        help='CSV file containing metrics logged during training',
    )
    parser.add_argument(
        '--output',
        type=str,
        default='python/output/training_mse.png',
        help='Where to save the training curve image',
    )
    args = parser.parse_args()

    df = pd.read_csv(args.metrics)
    plt.plot(df['epoch'], df['mse'], marker='o')
    plt.title('Training MSE')
    plt.xlabel('epoch')
    plt.ylabel('mse')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(args.output, dpi=150)
    print(f'Saved training plot to {args.output}')

    # Log the plot to the shared MLflow run
    run_id_path = os.path.join(os.path.dirname(args.metrics) or '.', 'mlflow_run_id.txt')
    with _resume_run(run_id_path) as active_run:
        if mlflow is not None and active_run is not None:
            try:
                mlflow.log_artifact(args.output, artifact_path='plots')
            except Exception as e:
                print(f'WARNING: could not log training plot to MLflow ({e})')


if __name__ == '__main__':
    main()
