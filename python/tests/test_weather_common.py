"""Tests for weather_common.py core helpers."""

import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from weather_common import (
    build_annual_summary,
    build_city_rankings,
    build_country_daily,
    build_daily_climatology,
    build_heat_stress_summary,
    compute_weather_summary,
)


def _daily_frame(years, cities=None, temp=5.0, precip=2.0):
    """Build a minimal city-level raw daily DataFrame."""
    rows = []
    for year in years:
        for month in range(1, 4):
            for day in range(1, 29):
                base = {
                    "time": f"{year}-{month:02d}-{day:02d}",
                    "temperature_2m_mean": temp + (year - min(years)) * 0.1,
                    "precipitation_sum": precip,
                }
                if cities:
                    for city in cities:
                        rows.append({**base, "city": city})
                else:
                    rows.append(base)
    return pd.DataFrame(rows)


class TestBuildCountryDaily:
    def test_filters_by_month_day(self):
        raw = _daily_frame(range(2020, 2023), cities=["Vilnius"])
        result = build_country_daily(raw, date(2022, 2, 15))
        assert result["month_day"].max() <= "02-15"

    def test_averages_across_cities(self):
        raw = _daily_frame(range(2020, 2022), cities=["A", "B"])
        result = build_country_daily(raw, date(2021, 3, 28))
        # Both cities have same temp, so std should be near-zero
        assert result["temperature_2m_mean"].std() < 1.0


class TestBuildAnnualSummary:
    def test_one_row_per_year(self):
        df = pd.DataFrame({
            "time": pd.to_datetime(["2020-01-01", "2020-01-02", "2021-01-01"]),
            "year": [2020, 2020, 2021],
            "temperature_2m_mean": [1.0, 3.0, 5.0],
            "precipitation_sum": [0.5, 0.5, 1.0],
            "month_day": ["01-01", "01-02", "01-01"],
        })
        annual = build_annual_summary(df)
        assert len(annual) == 2
        assert set(annual["year"].tolist()) == {2020, 2021}

    def test_mean_temp_correct(self):
        df = pd.DataFrame({
            "time": pd.to_datetime(["2020-01-01", "2020-01-02"]),
            "year": [2020, 2020],
            "temperature_2m_mean": [2.0, 4.0],
            "precipitation_sum": [1.0, 1.0],
            "month_day": ["01-01", "01-02"],
        })
        annual = build_annual_summary(df)
        assert annual.iloc[0]["ytd_mean_temp"] == pytest.approx(3.0)


class TestBuildDailyClimatology:
    def test_baseline_years_only(self):
        df = pd.DataFrame({
            "time": pd.to_datetime(
                [f"{y}-01-15" for y in range(1985, 2025)]
            ),
            "year": list(range(1985, 2025)),
            "month_day": ["01-15"] * 40,
            "temperature_2m_mean": [float(y) for y in range(1985, 2025)],
            "precipitation_sum": [1.0] * 40,
        })
        clim = build_daily_climatology(df)
        expected_mean = np.mean(list(range(1991, 2021)))
        assert clim.iloc[0]["climatology_temp_mean"] == pytest.approx(expected_mean, rel=1e-5)


class TestComputeWeatherSummary:
    def test_deviation_sign(self):
        annual = pd.DataFrame({
            "year": list(range(1991, 2027)),
            "ytd_mean_temp": [5.0] * 30 + [5.0] * 5 + [8.0],
            "ytd_total_precip": [100.0] * 36,
            "days": [90] * 36,
        })
        summary = compute_weather_summary(annual, current_year=2026)
        assert summary["temperature"]["deviation_vs_1991_2020_mean"] > 0

    def test_zero_deviation_at_mean(self):
        annual = pd.DataFrame({
            "year": list(range(1991, 2027)),
            "ytd_mean_temp": [5.0] * 36,
            "ytd_total_precip": [100.0] * 36,
            "days": [90] * 36,
        })
        summary = compute_weather_summary(annual, current_year=2026)
        assert summary["temperature"]["deviation_vs_1991_2020_mean"] == pytest.approx(0.0)


class TestBuildCityRankings:
    def test_ranks_by_abs_zscore(self):
        summaries = [
            {
                "city": "A",
                "temperature": {"deviation_vs_1991_2020_mean": 1.0, "z_score_vs_baseline": 0.5},
                "precipitation": {"deviation_vs_1991_2020_mean": -10.0, "z_score_vs_baseline": -0.3},
            },
            {
                "city": "B",
                "temperature": {"deviation_vs_1991_2020_mean": 3.0, "z_score_vs_baseline": 2.0},
                "precipitation": {"deviation_vs_1991_2020_mean": -5.0, "z_score_vs_baseline": -0.1},
            },
        ]
        rankings = build_city_rankings(summaries)
        assert rankings["temperature"][0]["city"] == "B"
        assert rankings["combined"][0]["city"] == "B"

    def test_all_sections_present(self):
        summaries = [
            {
                "city": "X",
                "temperature": {"deviation_vs_1991_2020_mean": 0.0, "z_score_vs_baseline": 0.0},
                "precipitation": {"deviation_vs_1991_2020_mean": 0.0, "z_score_vs_baseline": 0.0},
            },
        ]
        rankings = build_city_rankings(summaries)
        assert "temperature" in rankings
        assert "precipitation" in rankings
        assert "combined" in rankings


def _heat_stress_frame(years, tmin_base=3.0, tmax_base=10.0, cities=None):
    """Build a minimal raw daily DataFrame with Tmin/Tmax columns."""
    rows = []
    for year in years:
        for month in range(1, 4):
            for day in range(1, 15):  # 14 days per month
                row = {
                    "time": f"{year}-{month:02d}-{day:02d}",
                    "temperature_2m_mean": tmin_base + (tmax_base - tmin_base) / 2,
                    "temperature_2m_min": tmin_base + (year - min(years)) * 0.5,
                    "temperature_2m_max": tmax_base + (year - min(years)) * 0.3,
                    "precipitation_sum": 2.0,
                }
                if cities:
                    for city in cities:
                        rows.append({**row, "city": city})
                else:
                    rows.append(row)
    return pd.DataFrame(rows)


class TestBuildHeatStressSummary:
    def test_returns_empty_dict_without_tmin_tmax(self):
        """Falls back gracefully when old cached CSV lacks min/max columns."""
        raw = _daily_frame(list(range(1991, 2027)))
        result = build_heat_stress_summary(raw, 2026, date(2026, 3, 14))
        assert result == {}

    def test_returns_expected_keys(self):
        years = list(range(1991, 2027))
        raw = _heat_stress_frame(years, cities=["Vilnius", "Kaunas"])
        result = build_heat_stress_summary(raw, 2026, date(2026, 3, 14))
        assert "frost_days" in result
        assert "hot_days" in result
        assert "tropical_nights" in result
        assert "cold_nights" in result
        assert result["current_year"] == 2026

    def test_frost_days_counted_correctly(self):
        """With tmin = 3.0 + offset for all years, no frost days expected."""
        years = list(range(1991, 2027))
        # tmin_base=10 ensures all tmin > 0 even for earliest years
        raw = _heat_stress_frame(years, tmin_base=10.0, tmax_base=20.0, cities=["Vilnius"])
        result = build_heat_stress_summary(raw, 2026, date(2026, 3, 14))
        assert result["frost_days"]["current"] == 0

    def test_frost_days_positive_when_tmin_below_zero(self):
        """With tmin_base=-30, all tmin values stay below 0 for the full year range."""
        years = list(range(1991, 2027))
        # tmin = -30 + offset * 0.5; max offset = 35 → tmin_2026 = -12.5 < 0
        raw = _heat_stress_frame(years, tmin_base=-30.0, tmax_base=5.0, cities=["Vilnius"])
        result = build_heat_stress_summary(raw, 2026, date(2026, 3, 14))
        assert result["frost_days"]["current"] > 0
        assert result["frost_days"]["baseline_mean_1991_2020"] > 0

    def test_anomaly_is_current_minus_baseline(self):
        years = list(range(1991, 2027))
        raw = _heat_stress_frame(years, tmin_base=-5.0, tmax_base=5.0, cities=["Vilnius"])
        result = build_heat_stress_summary(raw, 2026, date(2026, 3, 14))
        fd = result["frost_days"]
        assert fd["anomaly"] == pytest.approx(
            fd["current"] - fd["baseline_mean_1991_2020"], abs=0.2
        )
