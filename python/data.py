"""Data generation helpers.

Creates a synthetic regression dataset and saves it to CSV for training/evaluation.
"""

from __future__ import annotations

import argparse
import os

import numpy as np
import pandas as pd

from model import make_synthetic_data


def save_csv(x: np.ndarray, y: np.ndarray, output_path: str):
    df = pd.DataFrame({'x': x.flatten(), 'y': y.flatten()})
    df.to_csv(output_path, index=False)


def main():
    parser = argparse.ArgumentParser(description='Generate synthetic regression data')
    parser.add_argument('--output', type=str, default='python/data.csv', help='Output CSV path')
    parser.add_argument('--samples', type=int, default=500, help='Number of samples to generate')
    parser.add_argument('--test-output', type=str, default=None, help='Optional test-set CSV path')
    parser.add_argument('--test-ratio', type=float, default=0.2, help='Fraction to hold out for testing')
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
    x, y = make_synthetic_data(args.samples)

    if args.test_output:
        n_test = int(len(x) * args.test_ratio)
        rng = np.random.default_rng(0)
        perm = rng.permutation(len(x))
        save_csv(x[perm[n_test:]], y[perm[n_test:]], args.output)
        os.makedirs(os.path.dirname(args.test_output) or '.', exist_ok=True)
        save_csv(x[perm[:n_test]], y[perm[:n_test]], args.test_output)
        print(f'Wrote {args.output} ({len(x) - n_test} train) and {args.test_output} ({n_test} test)')
    else:
        save_csv(x, y, args.output)
        print(f'Wrote {args.output} ({args.samples} samples)')


if __name__ == '__main__':
    main()
