"""Shared helpers for loading and inferring with the climate temperature model."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Mapping

import torch

from model import ClimateModel

DEFAULT_FEATURE_COLUMNS = ["sin_doy", "cos_doy", "year_norm"]
FEATURE_COLUMNS_FILE = "feature_columns.json"
FEATURE_DEFAULTS_FILE = "feature_defaults.json"


@dataclass(frozen=True)
class ClimateFeatureSpec:
    columns: list[str]
    defaults: dict[str, float]

    @property
    def input_dim(self) -> int:
        return len(self.columns)


def _resolve_feature_dir(base_path: Path) -> Path:
    path = Path(base_path)
    if path.is_file():
        return path.parent
    if (path / FEATURE_COLUMNS_FILE).exists() or (path / FEATURE_DEFAULTS_FILE).exists():
        return path
    climate_dir = path / "climate"
    if climate_dir.exists():
        return climate_dir
    return path


def load_climate_feature_spec(base_path: Path) -> ClimateFeatureSpec:
    feature_dir = _resolve_feature_dir(base_path)
    columns_path = feature_dir / FEATURE_COLUMNS_FILE
    defaults_path = feature_dir / FEATURE_DEFAULTS_FILE

    columns = DEFAULT_FEATURE_COLUMNS.copy()
    if columns_path.exists():
        raw_columns = json.loads(columns_path.read_text(encoding="utf-8"))
        if isinstance(raw_columns, list):
            parsed = [str(item) for item in raw_columns if str(item).strip()]
            if parsed:
                columns = parsed

    defaults = {name: 0.0 for name in columns}
    if defaults_path.exists():
        raw_defaults = json.loads(defaults_path.read_text(encoding="utf-8"))
        if isinstance(raw_defaults, dict):
            for name, value in raw_defaults.items():
                if name in defaults:
                    try:
                        defaults[name] = float(value)
                    except (TypeError, ValueError):
                        continue

    return ClimateFeatureSpec(columns=columns, defaults=defaults)


def instantiate_climate_model(feature_spec: ClimateFeatureSpec, dropout: float = 0.1, output_dim: int = 3) -> ClimateModel:
    return ClimateModel(input_dim=feature_spec.input_dim, output_dim=output_dim, dropout=dropout)


_NON_FEATURE_COLS: frozenset[str] = frozenset({"year", "y", "y_min", "y_max"})


def resolve_feature_spec_from_frame(
    base_path: Path,
    frame,
    *,
    target_column: str = "y",
) -> ClimateFeatureSpec:
    loaded_spec = load_climate_feature_spec(base_path)
    frame_columns = [column for column in frame.columns if column not in _NON_FEATURE_COLS]
    if loaded_spec.columns and all(column in frame.columns for column in loaded_spec.columns):
        columns = loaded_spec.columns
    else:
        columns = frame_columns

    defaults = {}
    for column in columns:
        if column in frame.columns:
            defaults[column] = float(frame[column].mean())
        else:
            defaults[column] = float(loaded_spec.defaults.get(column, 0.0))

    return ClimateFeatureSpec(columns=columns, defaults=defaults)


def attach_feature_spec(model: object, feature_spec: ClimateFeatureSpec) -> object:
    try:
        setattr(model, "_climate_feature_spec", feature_spec)
    except Exception:
        pass
    return model


def build_feature_values_for_date(
    target_date: date,
    feature_spec: ClimateFeatureSpec,
    overrides: Mapping[str, float | int | None] | None = None,
    *,
    year_ref: int = 1991,
    year_scale: float = 30.0,
) -> dict[str, float]:
    values = {name: float(feature_spec.defaults.get(name, 0.0)) for name in feature_spec.columns}

    doy = target_date.timetuple().tm_yday
    values["sin_doy"] = math.sin(2 * math.pi * doy / 365)
    values["cos_doy"] = math.cos(2 * math.pi * doy / 365)
    values["year_norm"] = (target_date.year - year_ref) / year_scale

    if overrides:
        for name, value in overrides.items():
            if name in values and value is not None:
                values[name] = float(value)

    return values


def build_input_tensor(
    feature_spec: ClimateFeatureSpec,
    values: Mapping[str, float | int | None],
) -> torch.Tensor:
    merged = {name: float(feature_spec.defaults.get(name, 0.0)) for name in feature_spec.columns}
    for name, value in values.items():
        if name in merged and value is not None:
            merged[name] = float(value)
    return torch.tensor([[merged[name] for name in feature_spec.columns]], dtype=torch.float32)


def build_input_tensor_for_date(
    target_date: date,
    feature_spec: ClimateFeatureSpec,
    overrides: Mapping[str, float | int | None] | None = None,
    *,
    year_ref: int = 1991,
    year_scale: float = 30.0,
) -> torch.Tensor:
    values = build_feature_values_for_date(
        target_date,
        feature_spec,
        overrides=overrides,
        year_ref=year_ref,
        year_scale=year_scale,
    )
    return build_input_tensor(feature_spec, values)


def load_local_climate_model(model_path: Path, feature_spec: ClimateFeatureSpec) -> ClimateModel | None:
    path = Path(model_path)
    if not path.exists():
        return None
    model = instantiate_climate_model(feature_spec)
    model.load_state_dict(torch.load(str(path), weights_only=True))
    model.eval()
    return attach_feature_spec(model, feature_spec)