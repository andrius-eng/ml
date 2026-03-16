"""Run a full synthetic workflow end to end.

This script generates data, trains a model, evaluates it, and prints a summary.
"""

from __future__ import annotations

import argparse
import os

from data import save_csv, make_synthetic_data
from evaluate import evaluate
from train import train


def main():
    parser = argparse.ArgumentParser(description='Run the full demo workflow')
    parser.add_argument('--output-dir', type=str, default='python/output', help='Where to store artifacts')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # 1) Generate data
    x, y = make_synthetic_data(400)
    data_path = os.path.join(args.output_dir, 'data.csv')
    save_csv(x, y, data_path)

    # 2) Train
    model_path = os.path.join(args.output_dir, 'model.pth')
    metrics_path = os.path.join(args.output_dir, 'metrics.csv')
    train(
        epochs=80,
        lr=0.01,
        batch_size=32,
        tracking_uri='./mlruns',
        model_path=model_path,
        metrics_path=metrics_path,
        data_path=data_path,
    )

    # 3) Evaluate
    evaluation = evaluate(
        model_path=model_path,
        data_path=data_path,
        summary_path=os.path.join(args.output_dir, 'evaluation.json'),
        predictions_path=os.path.join(args.output_dir, 'predictions.csv'),
    )

    print('---')
    print(f"Finished run. Model saved {model_path}, MSE={evaluation['mse']:.6f}, R2={evaluation['r2']:.6f}")


if __name__ == '__main__':
    main()
