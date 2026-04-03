from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from climate_model_contract import (
    build_input_tensor_for_date,
    load_climate_feature_spec,
    resolve_feature_spec_from_frame,
)


def test_resolve_feature_spec_from_frame_prefers_manifest_order(tmp_path: Path):
    climate_dir = tmp_path / "climate"
    climate_dir.mkdir()
    (climate_dir / "feature_columns.json").write_text(
        '["sin_doy", "cos_doy", "year_norm", "precip_log1p"]',
        encoding="utf-8",
    )
    (climate_dir / "feature_defaults.json").write_text(
        '{"sin_doy": 0.0, "cos_doy": 0.0, "year_norm": 0.0, "precip_log1p": 0.5}',
        encoding="utf-8",
    )

    frame = pd.DataFrame(
        {
            "y": [1.0, 2.0],
            "year_norm": [0.1, 0.2],
            "precip_log1p": [0.7, 1.1],
            "cos_doy": [0.4, 0.5],
            "sin_doy": [0.8, 0.9],
        }
    )

    spec = resolve_feature_spec_from_frame(climate_dir, frame)

    assert spec.columns == ["sin_doy", "cos_doy", "year_norm", "precip_log1p"]
    assert spec.defaults["precip_log1p"] == pytest.approx(0.9)


def test_build_input_tensor_for_date_uses_manifest_defaults(tmp_path: Path):
    climate_dir = tmp_path / "climate"
    climate_dir.mkdir()
    (climate_dir / "feature_columns.json").write_text(
        '["sin_doy", "cos_doy", "year_norm", "wind_norm", "et0_norm"]',
        encoding="utf-8",
    )
    (climate_dir / "feature_defaults.json").write_text(
        '{"sin_doy": 0.0, "cos_doy": 0.0, "year_norm": 0.0, "wind_norm": 0.33, "et0_norm": 0.18}',
        encoding="utf-8",
    )

    spec = load_climate_feature_spec(climate_dir)
    tensor = build_input_tensor_for_date(date(2026, 4, 3), spec)

    assert tuple(tensor.shape) == (1, 5)
    assert float(tensor[0, 3]) == pytest.approx(0.33)
    assert float(tensor[0, 4]) == pytest.approx(0.18)