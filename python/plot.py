"""Plot training metrics saved during training."""

from __future__ import annotations

import argparse

import matplotlib


matplotlib.use('Agg')

import matplotlib.pyplot as plt
import pandas as pd


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


if __name__ == '__main__':
    main()
