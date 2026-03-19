"""Run a full synthetic workflow end to end.

This script generates data, trains a model, evaluates it, and prints a summary.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Ensure sibling modules are importable regardless of cwd
sys.path.insert(0, str(Path(__file__).resolve().parent))

import numpy as np

from data import save_csv
from evaluate import evaluate
from model import make_synthetic_data
from train import train


def main():
    parser = argparse.ArgumentParser(description='Run the full demo workflow')
    parser.add_argument('--output-dir', type=str, default='python/output', help='Where to store artifacts')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # 1) Generate data with train/test split
    x, y = make_synthetic_data(500)
    n_test = int(len(x) * 0.2)
    rng = np.random.default_rng(0)
    perm = rng.permutation(len(x))
    train_path = os.path.join(args.output_dir, 'train.csv')
    test_path = os.path.join(args.output_dir, 'test.csv')
    save_csv(x[perm[n_test:]], y[perm[n_test:]], train_path)
    save_csv(x[perm[:n_test]], y[perm[:n_test]], test_path)

    # 2) Train on training set
    model_path = os.path.join(args.output_dir, 'model.pth')
    metrics_path = os.path.join(args.output_dir, 'metrics.csv')
    train(
        epochs=80,
        lr=0.01,
        batch_size=32,
        tracking_uri='./mlruns',
        model_path=model_path,
        metrics_path=metrics_path,
        data_path=train_path,
    )

    # 3) Evaluate on held-out test set
    evaluation = evaluate(
        model_path=model_path,
        data_path=test_path,
        summary_path=os.path.join(args.output_dir, 'evaluation.json'),
        predictions_path=os.path.join(args.output_dir, 'predictions.csv'),
    )

    print('---')
    print(f"Finished run. Model saved {model_path}, MSE={evaluation['mse']:.6f}, R2={evaluation['r2']:.6f}")


if __name__ == '__main__':
    main()
