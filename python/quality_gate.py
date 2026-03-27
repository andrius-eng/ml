"""Fail the pipeline when evaluation metrics miss configured thresholds."""

from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import nullcontext

try:
    import mlflow
    from mlflow import MlflowClient
except Exception:
    mlflow = None
    MlflowClient = None


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
    parser = argparse.ArgumentParser(description='Validate evaluation metrics against thresholds')
    parser.add_argument(
        '--summary-json',
        type=str,
        default='python/output/evaluation.json',
        help='Path to the evaluation summary JSON file',
    )
    parser.add_argument('--max-mse', type=float, default=50.0, help='Maximum acceptable MSE')
    parser.add_argument('--min-r2', type=float, default=0.65, help='Minimum acceptable R2')
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

    # Tag the MLflow run and promote @champion
    run_id_path = os.path.join(os.path.dirname(args.summary_json) or '.', 'mlflow_run_id.txt')
    with _resume_run(run_id_path) as active_run:
        if mlflow is not None and active_run is not None:
            mlflow.set_tags({'quality_gate': 'passed', 'stage': 'ready'})

    # Promote the registered model version to @champion
    if mlflow is not None and MlflowClient is not None:
        try:
            run_id = ''
            with open(run_id_path) as _f:
                run_id = _f.read().strip()
            client = MlflowClient()
            versions = client.search_model_versions(f"run_id='{run_id}'")
            if versions:
                version = versions[0].version
                client.set_registered_model_alias('ClimateTemperatureModel', 'champion', version)
                print(f'Promoted ClimateTemperatureModel v{version} to @champion')
            else:
                print('WARNING: no registered model version found for this run; @champion not updated')
        except Exception as _e:
            print(f'WARNING: could not set @champion alias: {_e}')

    return 0


if __name__ == '__main__':
    sys.exit(main())