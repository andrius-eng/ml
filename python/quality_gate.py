"""Fail the pipeline when evaluation metrics miss configured thresholds."""

from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import nullcontext
from pathlib import Path

try:
    import mlflow
except Exception:
    mlflow = None

from mlflow_model_registry import (
    MODEL_ALIAS,
    MODEL_REGISTRY_NAME,
    configure_tracking_uri,
    get_client,
    promote_model_alias_for_run,
    set_model_version_tags,
)


def _resume_run(run_id_path: str):
    if mlflow is None:
        return nullcontext()
    try:
        with open(run_id_path) as _f:
            run_id = _f.read().strip()
        configure_tracking_uri(os.environ.get('MLFLOW_TRACKING_URI', ''))
        return mlflow.start_run(run_id=run_id)
    except Exception:
        return nullcontext()


def main():
    parser = argparse.ArgumentParser(description='Validate evaluation metrics against thresholds')
    parser.add_argument(
        '--summary-json',
        type=str,
        default='python/output/climate/climate_evaluation.json',
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
            mlflow.set_tags({
                'quality_gate': 'passed',
                'stage': 'ready',
                'registered_model_name': MODEL_REGISTRY_NAME,
            })

    # Promote the registered model version to @champion, creating it first if needed.
    if mlflow is not None:
        try:
            run_id = Path(run_id_path).read_text(encoding='utf-8').strip()
            client = get_client(os.environ.get('MLFLOW_TRACKING_URI', ''))
            if client is None:
                print('WARNING: MLflow client unavailable; model alias not updated')
                return 0

            version = promote_model_alias_for_run(client, run_id)
            if version is None:
                print('WARNING: model version could not be created or promoted')
                return 0

            set_model_version_tags(
                client,
                MODEL_REGISTRY_NAME,
                str(version.version),
                {
                    'quality_gate': 'passed',
                    'summary_json': os.path.basename(args.summary_json),
                    'mse': f'{mse:.6f}',
                    'r2': f'{r2:.6f}',
                    'promoted_alias': MODEL_ALIAS,
                },
            )
            version_path = os.path.join(os.path.dirname(args.summary_json) or '.', 'mlflow_model_version.txt')
            with open(version_path, 'w', encoding='utf-8') as handle:
                handle.write(str(version.version))
            print(f'Promoted {MODEL_REGISTRY_NAME} v{version.version} to @{MODEL_ALIAS}')
        except Exception as _e:
            print(f'WARNING: could not set @{MODEL_ALIAS} alias: {_e}')

    return 0


if __name__ == '__main__':
    sys.exit(main())