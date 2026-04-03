"""Register or promote the current climate model run in MLflow.

Usage:
    MLFLOW_TRACKING_URI=http://mlflow:5000 python python/scripts/register_climate_model.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

from mlflow_model_registry import (
    MODEL_ALIAS,
    MODEL_REGISTRY_NAME,
    configure_tracking_uri,
    get_client,
    promote_model_alias_for_run,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Register and promote the climate model run in MLflow")
    parser.add_argument(
        "--run-id-path",
        type=str,
        default="python/output/climate/mlflow_run_id.txt",
        help="Path to the run-id file produced by climate_train.py",
    )
    parser.add_argument(
        "--tracking-uri",
        type=str,
        default="",
        help="Optional MLflow tracking URI override",
    )
    args = parser.parse_args()

    run_id = Path(args.run_id_path).read_text(encoding="utf-8").strip()
    tracking_uri = configure_tracking_uri(args.tracking_uri)
    client = get_client(tracking_uri)
    if client is None:
        raise SystemExit("MLflow is not available in this environment")

    version = promote_model_alias_for_run(client, run_id)
    if version is None:
        raise SystemExit("Could not register the climate model run in MLflow")

    print(
        f"Promoted {MODEL_REGISTRY_NAME} v{version.version} to @{MODEL_ALIAS} "
        f"for run {run_id} ({tracking_uri or 'default tracking URI'})"
    )


if __name__ == "__main__":
    main()