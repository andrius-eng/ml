"""Create diagnostic plots from evaluation predictions."""

from __future__ import annotations

import argparse

import matplotlib


matplotlib.use('Agg')

import matplotlib.pyplot as plt
import pandas as pd


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


if __name__ == '__main__':
    main()