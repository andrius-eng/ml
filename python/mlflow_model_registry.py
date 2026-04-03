"""Helpers for explicit MLflow model version registration and alias promotion."""

from __future__ import annotations

import os
import time

try:
    import mlflow
    from mlflow import MlflowClient
except Exception:  # pragma: no cover - optional dependency
    mlflow = None
    MlflowClient = None


MODEL_REGISTRY_NAME = os.environ.get("MODEL_REGISTRY_NAME", "ClimateTemperatureModel")
MODEL_ALIAS = os.environ.get("MODEL_ALIAS", "champion")
MODEL_ARTIFACT_PATH = "model"


def configure_tracking_uri(tracking_uri: str | None = None) -> str:
    resolved = (tracking_uri or os.environ.get("MLFLOW_TRACKING_URI", "")).strip()
    if mlflow is not None and resolved:
        mlflow.set_tracking_uri(resolved)
    return resolved


def get_client(tracking_uri: str | None = None):
    if mlflow is None or MlflowClient is None:
        return None
    configure_tracking_uri(tracking_uri)
    return MlflowClient()


def ensure_registered_model(client, model_name: str = MODEL_REGISTRY_NAME):
    try:
        return client.get_registered_model(model_name)
    except Exception:
        try:
            return client.create_registered_model(model_name)
        except Exception:
            return client.get_registered_model(model_name)


def _version_sort_key(version) -> int:
    try:
        return int(version.version)
    except Exception:
        return -1


def find_model_version_for_run(client, run_id: str, model_name: str = MODEL_REGISTRY_NAME):
    if not run_id:
        return None
    versions = [
        version
        for version in client.search_model_versions(f"name='{model_name}'")
        if getattr(version, "run_id", "") == run_id
    ]
    if not versions:
        return None
    return max(versions, key=_version_sort_key)


def wait_for_model_version_ready(
    client,
    model_name: str,
    version: str,
    timeout_seconds: int = 120,
):
    deadline = time.time() + timeout_seconds
    latest = client.get_model_version(model_name, version)
    while getattr(latest, "status", "READY") == "PENDING_REGISTRATION" and time.time() < deadline:
        time.sleep(2)
        latest = client.get_model_version(model_name, version)
    return latest


def ensure_model_version_for_run(
    client,
    run_id: str,
    *,
    model_name: str = MODEL_REGISTRY_NAME,
    artifact_path: str = MODEL_ARTIFACT_PATH,
    timeout_seconds: int = 120,
):
    existing = find_model_version_for_run(client, run_id, model_name=model_name)
    if existing is not None:
        return existing

    ensure_registered_model(client, model_name=model_name)
    # Use runs:/ URI so the model version source resolves through the tracking
    # server's artifact proxy endpoint rather than a bare file:// path.
    source = f"runs:/{run_id}/{artifact_path}"
    created = client.create_model_version(name=model_name, source=source, run_id=run_id)
    return wait_for_model_version_ready(client, model_name, str(created.version), timeout_seconds=timeout_seconds)


def set_model_version_tags(client, model_name: str, version: str, tags: dict[str, str]) -> None:
    for key, value in tags.items():
        client.set_model_version_tag(model_name, version, key, value)


def promote_model_alias_for_run(
    client,
    run_id: str,
    *,
    model_name: str = MODEL_REGISTRY_NAME,
    alias: str = MODEL_ALIAS,
    artifact_path: str = MODEL_ARTIFACT_PATH,
    timeout_seconds: int = 120,
):
    version = ensure_model_version_for_run(
        client,
        run_id,
        model_name=model_name,
        artifact_path=artifact_path,
        timeout_seconds=timeout_seconds,
    )
    if version is None:
        return None
    client.set_registered_model_alias(model_name, alias, version.version)
    return version