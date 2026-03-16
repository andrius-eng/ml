"""Fail the pipeline when evaluation metrics miss configured thresholds."""

from __future__ import annotations

import argparse
import json
import sys


def main():
    parser = argparse.ArgumentParser(description='Validate evaluation metrics against thresholds')
    parser.add_argument(
        '--summary-json',
        type=str,
        default='python/output/evaluation.json',
        help='Path to the evaluation summary JSON file',
    )
    parser.add_argument('--max-mse', type=float, default=0.08, help='Maximum acceptable MSE')
    parser.add_argument('--min-r2', type=float, default=0.97, help='Minimum acceptable R2')
    args = parser.parse_args()

    with open(args.summary_json, 'r', encoding='utf-8') as handle:
        summary = json.load(handle)

    mse = float(summary['mse'])
    r2 = float(summary['r2'])

    print(f"Quality gate metrics: mse={mse:.6f}, r2={r2:.6f}")

    if mse > args.max_mse:
        raise SystemExit(f'MSE {mse:.6f} exceeds threshold {args.max_mse:.6f}')

    if r2 < args.min_r2:
        raise SystemExit(f'R2 {r2:.6f} is below threshold {args.min_r2:.6f}')

    print('Quality gate passed.')
    return 0


if __name__ == '__main__':
    sys.exit(main())