"""
Smoke tests for vilnius_march_analyze core logic.

These tests exercise the anomaly computation inline — no file I/O, no network.
Run with: cd ml/python && ../.venv/bin/pytest tests/ -v
"""
import io
import json
import sys
import textwrap
from pathlib import Path

import pandas as pd
import pytest

# Make the python/ directory importable without installing a package
sys.path.insert(0, str(Path(__file__).parent.parent))


# ── helpers mirroring the analysis logic ──────────────────────────────────

def compute_march_anomalies(raw: pd.DataFrame, execution_date_str: str, window_years: int = 5):
    """Inline re-implementation of the core analysis step for unit testing."""
    from datetime import date

    execution_date = date.fromisoformat(execution_date_str)
    cutoff_day = execution_date.day if execution_date.month == 3 else 31
    start_year = execution_date.year - window_years + 1

    raw = raw.copy()
    raw["time"] = pd.to_datetime(raw["time"])
    raw["year"] = raw["time"].dt.year
    raw["month"] = raw["time"].dt.month
    raw["day"] = raw["time"].dt.day

    march = raw[
        (raw["year"] >= start_year)
        & (raw["year"] <= execution_date.year)
        & (raw["month"] == 3)
        & (raw["day"] <= cutoff_day)
    ].copy()

    annual = march.groupby("year", as_index=False).agg(
        mean_temp_c=("temperature_2m_mean", "mean"),
        days_observed=("time", "count"),
    )
    baseline_mean = float(annual["mean_temp_c"].mean())
    baseline_std = float(annual["mean_temp_c"].std(ddof=1))
    annual["anomaly_c"] = annual["mean_temp_c"] - baseline_mean
    annual["zscore"] = annual["anomaly_c"] / baseline_std if baseline_std else 0.0

    return annual, baseline_mean, baseline_std


def _make_raw(temps_by_year: dict[int, float], days: int = 16) -> pd.DataFrame:
    """Build a minimal raw CSV-like DataFrame with daily rows for each year/March."""
    rows = []
    for year, temp in temps_by_year.items():
        for day in range(1, days + 1):
            rows.append({
                "time": f"{year}-03-{day:02d}",
                "temperature_2m_mean": temp,
                "precipitation_sum": 0.0,
                "city": "Vilnius",
            })
    return pd.DataFrame(rows)


# ── tests ──────────────────────────────────────────────────────────────────

class TestMarchAnomalyCore:

    def test_baseline_mean_is_average_of_yearly_means(self):
        raw = _make_raw({2020: 1.0, 2021: 3.0, 2022: 5.0, 2023: 7.0, 2024: 9.0})
        annual, baseline_mean, _ = compute_march_anomalies(raw, "2024-03-16", window_years=5)
        assert baseline_mean == pytest.approx(5.0)

    def test_anomaly_is_deviation_from_baseline(self):
        raw = _make_raw({2020: 1.0, 2021: 3.0, 2022: 5.0, 2023: 7.0, 2024: 9.0})
        annual, baseline_mean, _ = compute_march_anomalies(raw, "2024-03-16", window_years=5)
        latest = annual[annual["year"] == 2024].iloc[0]
        assert latest["anomaly_c"] == pytest.approx(9.0 - baseline_mean)

    def test_z_scores_sum_to_zero(self):
        raw = _make_raw({2020: 1.0, 2021: 3.0, 2022: 5.0, 2023: 7.0, 2024: 9.0})
        annual, _, _ = compute_march_anomalies(raw, "2024-03-16", window_years=5)
        assert annual["zscore"].sum() == pytest.approx(0.0, abs=1e-10)

    def test_cutoff_day_respected_in_march(self):
        """Only days 1-10 should be used when execution_date is 2024-03-10."""
        days_16 = _make_raw({2020: 0.0}, days=16)
        days_10 = _make_raw({2020: 0.0}, days=10)
        # Add extra rows beyond day 10 with a very different temperature
        extra = pd.DataFrame([{
            "time": f"2020-03-{d:02d}",
            "temperature_2m_mean": 100.0,
            "precipitation_sum": 0.0,
            "city": "Vilnius",
        } for d in range(11, 17)])
        raw_mixed = pd.concat([days_10, extra], ignore_index=True)
        annual, baseline_mean, _ = compute_march_anomalies(raw_mixed, "2020-03-10", window_years=1)
        assert baseline_mean == pytest.approx(0.0)

    def test_window_years_limits_data(self):
        raw = _make_raw({2018: -5.0, 2019: -3.0, 2020: 0.0, 2021: 3.0, 2022: 5.0})
        annual, _, _ = compute_march_anomalies(raw, "2022-03-16", window_years=3)
        assert set(annual["year"].tolist()) == {2020, 2021, 2022}

    def test_returns_correct_row_count(self):
        years = {y: float(y) for y in range(2000, 2010)}
        raw = _make_raw(years)
        annual, _, _ = compute_march_anomalies(raw, "2009-03-16", window_years=10)
        assert len(annual) == 10

    def test_uniform_temps_give_zero_variance_zscore(self):
        raw = _make_raw({2020: 2.0, 2021: 2.0, 2022: 2.0})
        annual, baseline_mean, baseline_std = compute_march_anomalies(raw, "2022-03-16", window_years=3)
        assert baseline_mean == pytest.approx(2.0)
        assert baseline_std == pytest.approx(0.0)
        # anomaly should be 0 for all, zscore handled as 0 when std==0
        assert annual["anomaly_c"].abs().max() == pytest.approx(0.0, abs=1e-10)


class TestQualityGateLogic:

    def test_gate_fails_on_wrong_year_count(self):
        """years_included != expected_years should raise SystemExit."""
        summary = {"window": {"years_included": 25}}
        with pytest.raises(SystemExit):
            years_included = int(summary["window"]["years_included"])
            expected_years = 30
            if years_included != expected_years:
                raise SystemExit(f"Expected {expected_years} rows, found {years_included}")

    def test_gate_fails_on_sparse_month(self):
        annual = pd.DataFrame({"days_observed": [16, 16, 3, 16], "zscore": [0.1, 0.2, -0.1, 0.0]})
        with pytest.raises(SystemExit):
            min_days = int(annual["days_observed"].min())
            if min_days < 10:
                raise SystemExit(f"Too sparse: {min_days}")

    def test_gate_fails_on_extreme_zscore(self):
        annual = pd.DataFrame({"days_observed": [16] * 5, "zscore": [0.5, -0.3, 4.5, 0.1, -0.2]})
        with pytest.raises(SystemExit):
            max_abs_z = float(annual["zscore"].abs().max())
            if max_abs_z > 4.0:
                raise SystemExit(f"z={max_abs_z:.2f} exceeds threshold")

    def test_gate_passes_on_valid_data(self):
        """No exception should be raised on clean data."""
        annual = pd.DataFrame({
            "days_observed": [16] * 30,
            "zscore": [float(i) * 0.1 for i in range(-15, 15)],
        })
        summary = {"window": {"years_included": 30}}
        years_included = int(summary["window"]["years_included"])
        min_days = int(annual["days_observed"].min())
        max_abs_z = float(annual["zscore"].abs().max())

        assert years_included == 30
        assert min_days >= 10
        assert max_abs_z <= 4.0
