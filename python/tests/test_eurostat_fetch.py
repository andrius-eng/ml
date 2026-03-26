"""Tests for eurostat_fetch.py — parse and summarise HDD data."""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from eurostat_fetch import build_hdd_summary, parse_hdd_series

# ---------------------------------------------------------------------------
# Minimal fake Eurostat JSON payload
# ---------------------------------------------------------------------------

def _make_raw(months: list[str], values: list[float]) -> dict:
    """Build a minimal Eurostat JSON response for the given months/values."""
    index = {m: i for i, m in enumerate(months)}
    return {
        "dimension": {
            "time": {
                "category": {
                    "index": index,
                    "label": {m: m for m in months},
                }
            }
        },
        "value": {str(i): v for i, v in enumerate(values)},
    }


# ---------------------------------------------------------------------------
# parse_hdd_series
# ---------------------------------------------------------------------------

class TestParseHddSeries:
    def test_returns_sorted_list(self):
        raw = _make_raw(["2024-03", "2024-01", "2024-02"], [300.0, 500.0, 420.0])
        result = parse_hdd_series(raw)
        assert [r["month"] for r in result] == ["2024-01", "2024-02", "2024-03"]

    def test_values_correct(self):
        raw = _make_raw(["2024-01", "2024-02"], [500.0, 420.0])
        result = parse_hdd_series(raw)
        assert result[0]["hdd"] == pytest.approx(500.0)
        assert result[1]["hdd"] == pytest.approx(420.0)

    def test_skips_missing_values(self):
        """Index entries with no value entry should be excluded."""
        raw = {
            "dimension": {"time": {"category": {"index": {"2024-01": 0, "2024-02": 1}, "label": {}}}},
            "value": {"0": 400.0},  # only index 0 present
        }
        result = parse_hdd_series(raw)
        assert len(result) == 1
        assert result[0]["month"] == "2024-01"

    def test_normalises_yyyymm_labels(self):
        """Six-character labels like '202401' should be normalised to '2024-01'."""
        raw = _make_raw(["202401", "202402"], [500.0, 420.0])
        result = parse_hdd_series(raw)
        assert result[0]["month"] == "2024-01"
        assert result[1]["month"] == "2024-02"


# ---------------------------------------------------------------------------
# build_hdd_summary — helpers
# ---------------------------------------------------------------------------

def _series_for_years(years: range, months: list[int], hdd_per_month: float = 300.0) -> list[dict]:
    rows = []
    for yr in years:
        for mo in months:
            rows.append({"month": f"{yr}-{mo:02d}", "hdd": hdd_per_month})
    return sorted(rows, key=lambda r: r["month"])


# ---------------------------------------------------------------------------
# build_hdd_summary
# ---------------------------------------------------------------------------

class TestBuildHddSummary:
    def test_ytd_total_sums_jan_to_current_month(self):
        series = _series_for_years(range(1991, 2027), [1, 2, 3], hdd_per_month=200.0)
        result = build_hdd_summary(series, today=date(2026, 3, 15))
        # Jan + Feb + Mar data available for 2026
        assert result["ytd"]["total_hdd"] == pytest.approx(600.0)

    def test_ytd_baseline_mean_computed_from_1991_2020(self):
        # All years same HDD — baseline == current
        series = _series_for_years(range(1991, 2027), [1, 2, 3], hdd_per_month=300.0)
        result = build_hdd_summary(series, today=date(2026, 3, 15))
        assert result["ytd"]["baseline_mean_1991_2020"] == pytest.approx(900.0)
        assert result["ytd"]["anomaly"] == pytest.approx(0.0)

    def test_ytd_anomaly_negative_when_warmer(self):
        # Current year has half the HDD of baseline
        baseline = _series_for_years(range(1991, 2026), [1, 2, 3], hdd_per_month=300.0)
        current = [{"month": f"2026-{m:02d}", "hdd": 150.0} for m in [1, 2, 3]]
        series = sorted(baseline + current, key=lambda r: r["month"])
        result = build_hdd_summary(series, today=date(2026, 3, 15))
        assert result["ytd"]["anomaly"] < 0

    def test_recent_months_capped_at_12(self):
        series = _series_for_years(range(2020, 2027), [1, 2, 3, 4, 5, 6], hdd_per_month=100.0)
        result = build_hdd_summary(series, today=date(2026, 3, 15))
        assert len(result["recent_months"]) <= 12

    def test_output_keys_present(self):
        series = _series_for_years(range(1991, 2027), [1, 2, 3], hdd_per_month=300.0)
        result = build_hdd_summary(series, today=date(2026, 3, 15))
        assert "fetched_at" in result
        assert "country" in result
        assert "unit" in result
        assert "ytd" in result
        assert "heating_season" in result

    def test_empty_series_returns_zero_ytd(self):
        result = build_hdd_summary([], today=date(2026, 3, 15))
        assert result["ytd"]["total_hdd"] == pytest.approx(0.0)
        assert result["ytd"]["baseline_mean_1991_2020"] == pytest.approx(0.0)
