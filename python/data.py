"""Data generation helpers.

Creates a synthetic regression dataset and saves it to CSV for training/evaluation.
"""

from __future__ import annotations

import argparse
import os

import numpy as np
import pandas as pd


def make_synthetic_data(n_samples: int = 500):
    rng = np.random.default_rng(42)
    x = rng.normal(size=(n_samples, 1)).astype(np.float32)
    y = 2 * x + 1 + rng.normal(0, 0.2, size=(n_samples, 1)).astype(np.float32)
    return x, y


def save_csv(x: np.ndarray, y: np.ndarray, output_path: str):
    df = pd.DataFrame({'x': x.flatten(), 'y': y.flatten()})
    df.to_csv(output_path, index=False)


def main():
    parser = argparse.ArgumentParser(description='Generate synthetic regression data')
    parser.add_argument('--output', type=str, default='python/data.csv', help='Output CSV path')
    parser.add_argument('--samples', type=int, default=500, help='Number of samples to generate')
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
    x, y = make_synthetic_data(args.samples)
    save_csv(x, y, args.output)
    print(f'Wrote {args.output} ({args.samples} samples)')


if __name__ == '__main__':
    main()
