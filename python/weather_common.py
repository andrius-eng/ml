"""Shared helpers for Lithuania weather analysis."""

from __future__ import annotations

import json
import time
from datetime import date
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd


LITHUANIA_PROXY_CITIES = {
    "Vilnius": (54.6872, 25.2797),
    "Kaunas": (54.8985, 23.9036),
    "Klaipeda": (55.7033, 21.1443),
    "Siauliai": (55.9349, 23.3137),
    "Panevezys": (55.7348, 24.3575),
}

# Extended set: Lithuanian cities + neighbouring capitals for regional comparison
REGION_CITIES = {
    **LITHUANIA_PROXY_CITIES,
    "Riga": (56.9496, 24.1052),
    "Warsaw": (52.2297, 21.0122),
    "Tallinn": (59.4370, 24.7536),
    "Minsk": (53.9045, 27.5615),
}


def fetch_daily_weather(lat: float, lon: float, start: str, end: str) -> pd.DataFrame:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "daily": ["temperature_2m_mean", "precipitation_sum"],
        "timezone": "Europe/Vilnius",
    }
    url = "https://archive-api.open-meteo.com/v1/archive?" + urlencode(params, doseq=True)
    for attempt in range(5):
        try:
            with urlopen(url, timeout=60) as response:
                payload = json.load(response)["daily"]
            break
        except (TimeoutError, URLError) as e:
            if attempt == 4:
                raise
            # Back off longer on rate-limit (429) responses
            delay = 10 * (attempt + 1) if isinstance(e, HTTPError) and e.code == 429 else 2 * (attempt + 1)
            time.sleep(delay)
    return pd.DataFrame(payload)


def build_country_daily(raw_daily: pd.DataFrame, current_end: date) -> pd.DataFrame:
    raw = raw_daily.copy()
    raw["time"] = pd.to_datetime(raw["time"])
    raw["year"] = raw["time"].dt.year
    raw["month_day"] = raw["time"].dt.strftime("%m-%d")
    raw = raw[raw["month_day"] <= current_end.strftime("%m-%d")].copy()

    country_daily = (
        raw.groupby("time", as_index=False)[["temperature_2m_mean", "precipitation_sum"]].mean()
    )
    country_daily["year"] = country_daily["time"].dt.year
    country_daily["month_day"] = country_daily["time"].dt.strftime("%m-%d")
    return country_daily


def build_city_daily(raw_daily: pd.DataFrame, current_end: date) -> pd.DataFrame:
    raw = raw_daily.copy()
    raw["time"] = pd.to_datetime(raw["time"])
    raw["year"] = raw["time"].dt.year
    raw["month_day"] = raw["time"].dt.strftime("%m-%d")
    return raw[raw["month_day"] <= current_end.strftime("%m-%d")].copy()


def build_annual_summary(country_daily: pd.DataFrame) -> pd.DataFrame:
    return country_daily.groupby("year", as_index=False).agg(
        ytd_mean_temp=("temperature_2m_mean", "mean"),
        ytd_total_precip=("precipitation_sum", "sum"),
        days=("time", "count"),
    )


def build_city_annual_summary(city_daily: pd.DataFrame) -> pd.DataFrame:
    return city_daily.groupby(["city", "year"], as_index=False).agg(
        ytd_mean_temp=("temperature_2m_mean", "mean"),
        ytd_total_precip=("precipitation_sum", "sum"),
        days=("time", "count"),
    )


def build_daily_climatology(daily: pd.DataFrame, group_cols: list[str] | None = None) -> pd.DataFrame:
    if group_cols is None:
        group_cols = []

    baseline = daily[(daily["year"] >= 1991) & (daily["year"] <= 2020)].copy()
    keys = [*group_cols, "month_day"]
    climatology = baseline.groupby(keys, as_index=False).agg(
        climatology_temp_mean=("temperature_2m_mean", "mean"),
        climatology_temp_std=("temperature_2m_mean", "std"),
        climatology_precip_mean=("precipitation_sum", "mean"),
        climatology_precip_std=("precipitation_sum", "std"),
    )
    return climatology


def apply_daily_climatology(daily: pd.DataFrame, climatology: pd.DataFrame, group_cols: list[str] | None = None) -> pd.DataFrame:
    if group_cols is None:
        group_cols = []

    keys = [*group_cols, "month_day"]
    enriched = daily.merge(climatology, on=keys, how="left")
    enriched = enriched.sort_values([*group_cols, "time"]).reset_index(drop=True)
    enriched["temp_anomaly"] = enriched["temperature_2m_mean"] - enriched["climatology_temp_mean"]
    enriched["precip_anomaly"] = enriched["precipitation_sum"] - enriched["climatology_precip_mean"]

    if group_cols:
        grouped = enriched.groupby(group_cols, group_keys=False)
        enriched["rolling_7d_temp_anomaly"] = grouped["temp_anomaly"].transform(
            lambda s: s.rolling(window=7, min_periods=1).mean()
        )
        enriched["rolling_30d_temp_anomaly"] = grouped["temp_anomaly"].transform(
            lambda s: s.rolling(window=30, min_periods=1).mean()
        )
        enriched["rolling_7d_precip_anomaly"] = grouped["precip_anomaly"].transform(
            lambda s: s.rolling(window=7, min_periods=1).sum()
        )
        enriched["cumulative_precip_anomaly"] = grouped["precip_anomaly"].cumsum()
    else:
        enriched["rolling_7d_temp_anomaly"] = enriched["temp_anomaly"].rolling(window=7, min_periods=1).mean()
        enriched["rolling_30d_temp_anomaly"] = enriched["temp_anomaly"].rolling(window=30, min_periods=1).mean()
        enriched["rolling_7d_precip_anomaly"] = enriched["precip_anomaly"].rolling(window=7, min_periods=1).sum()
        enriched["cumulative_precip_anomaly"] = enriched["precip_anomaly"].cumsum()

    return enriched


def build_monthly_anomalies(daily: pd.DataFrame, group_cols: list[str] | None = None, current_year: int = 2026) -> pd.DataFrame:
    if group_cols is None:
        group_cols = []

    frame = daily.copy()
    frame["month"] = pd.to_datetime(frame["time"]).dt.month

    monthly_per_year = frame.groupby([*group_cols, "year", "month"], as_index=False).agg(
        temp_mean=("temperature_2m_mean", "mean"),
        precip_total=("precipitation_sum", "sum"),
        days=("time", "count"),
    )

    baseline = monthly_per_year[(monthly_per_year["year"] >= 1991) & (monthly_per_year["year"] <= 2020)].copy()
    current_monthly = monthly_per_year[monthly_per_year["year"] == current_year].copy()

    keys = [*group_cols, "month"]
    baseline_monthly = baseline.groupby(keys, as_index=False).agg(
        climatology_temp_mean=("temp_mean", "mean"),
        climatology_temp_std=("temp_mean", "std"),
        climatology_precip_mean=("precip_total", "mean"),
        climatology_precip_std=("precip_total", "std"),
    )
    current_monthly = current_monthly.rename(
        columns={"temp_mean": "current_temp_mean", "precip_total": "current_precip_total"}
    )
    monthly = current_monthly.merge(baseline_monthly, on=keys, how="left")
    monthly["temp_anomaly"] = monthly["current_temp_mean"] - monthly["climatology_temp_mean"]
    monthly["precip_anomaly"] = monthly["current_precip_total"] - monthly["climatology_precip_mean"]
    monthly["temp_zscore"] = monthly.apply(
        lambda row: (row["temp_anomaly"] / row["climatology_temp_std"]) if pd.notna(row["climatology_temp_std"]) and row["climatology_temp_std"] else None,
        axis=1,
    )
    monthly["precip_zscore"] = monthly.apply(
        lambda row: (row["precip_anomaly"] / row["climatology_precip_std"]) if pd.notna(row["climatology_precip_std"]) and row["climatology_precip_std"] else None,
        axis=1,
    )
    return monthly.sort_values(keys).reset_index(drop=True)


def compute_weather_summary(annual: pd.DataFrame, current_year: int = 2026) -> dict:
    baseline = annual[(annual["year"] >= 1991) & (annual["year"] <= 2020)].copy()
    recent = annual[(annual["year"] >= 2021) & (annual["year"] <= current_year - 1)].copy()
    current = annual[annual["year"] == current_year].iloc[0]

    expected = {
        "temp_mean": float(baseline["ytd_mean_temp"].mean()),
        "temp_std": float(baseline["ytd_mean_temp"].std(ddof=1)),
        "precip_mean": float(baseline["ytd_total_precip"].mean()),
        "precip_std": float(baseline["ytd_total_precip"].std(ddof=1)),
    }

    result = {
        "coverage": {
            "baseline_years": [1991, 2020],
            "proxy_cities": list(LITHUANIA_PROXY_CITIES.keys()),
            "days_observed": int(current["days"]),
        },
        "current_year": int(current_year),
        "current": {
            "ytd_mean_temp": float(current["ytd_mean_temp"]),
            "ytd_total_precip": float(current["ytd_total_precip"]),
        },
        "expected": expected,
    }

    for column, label, expected_mean_key, expected_std_key in [
        ("ytd_mean_temp", "temperature", "temp_mean", "temp_std"),
        ("ytd_total_precip", "precipitation", "precip_mean", "precip_std"),
    ]:
        actual_current = float(current[column])
        expected_mean = expected[expected_mean_key]
        expected_std = expected[expected_std_key]

        percent_deviation = None
        if expected_mean != 0:
            percent_deviation = float(((actual_current - expected_mean) / abs(expected_mean)) * 100.0)

        result[label] = {
            "deviation_vs_1991_2020_mean": float(actual_current - expected_mean),
            "percent_deviation_vs_1991_2020_mean": percent_deviation,
            "z_score_vs_baseline": float((actual_current - expected_mean) / expected_std) if expected_std else None,
        }

    return result


def compute_city_weather_summary(city_annual: pd.DataFrame, current_year: int = 2026) -> list[dict]:
    summaries: list[dict] = []
    for city in sorted(city_annual["city"].unique()):
        city_frame = city_annual[city_annual["city"] == city].drop(columns=["city"])
        city_summary = compute_weather_summary(city_frame, current_year=current_year)
        city_summary["city"] = city
        summaries.append(city_summary)
    return summaries


def attach_current_anomaly_metrics(summary: dict, current_daily: pd.DataFrame) -> dict:
    enriched = summary.copy()
    latest = current_daily.sort_values("time").iloc[-1]
    enriched["temperature"].update(
        {
            "climatology_ytd_mean": float(current_daily["climatology_temp_mean"].mean()),
            "latest_daily_anomaly": float(latest["temp_anomaly"]),
            "latest_7d_anomaly": float(latest["rolling_7d_temp_anomaly"]),
            "latest_30d_anomaly": float(latest["rolling_30d_temp_anomaly"]),
        }
    )
    enriched["precipitation"].update(
        {
            "climatology_ytd_total": float(current_daily["climatology_precip_mean"].sum()),
            "latest_daily_anomaly": float(latest["precip_anomaly"]),
            "latest_7d_anomaly": float(latest["rolling_7d_precip_anomaly"]),
            "latest_cumulative_anomaly": float(latest["cumulative_precip_anomaly"]),
        }
    )
    return enriched


def build_city_rankings(city_summaries: list[dict]) -> dict[str, list[dict]]:
    temp_rank = sorted(
        (
            {
                "city": item["city"],
                "anomaly": item["temperature"]["deviation_vs_1991_2020_mean"],
                "z_score": item["temperature"]["z_score_vs_baseline"],
            }
            for item in city_summaries
        ),
        key=lambda row: abs(row["z_score"]),
        reverse=True,
    )
    precip_rank = sorted(
        (
            {
                "city": item["city"],
                "anomaly": item["precipitation"]["deviation_vs_1991_2020_mean"],
                "z_score": item["precipitation"]["z_score_vs_baseline"],
            }
            for item in city_summaries
        ),
        key=lambda row: abs(row["z_score"]),
        reverse=True,
    )

    combined = []
    for item in city_summaries:
        combined.append(
            {
                "city": item["city"],
                "combined_score": float(
                    max(
                        abs(item["temperature"]["z_score_vs_baseline"]),
                        abs(item["precipitation"]["z_score_vs_baseline"]),
                    )
                ),
                "temperature_z": item["temperature"]["z_score_vs_baseline"],
                "precipitation_z": item["precipitation"]["z_score_vs_baseline"],
            }
        )
    combined.sort(key=lambda row: row["combined_score"], reverse=True)

    return {
        "temperature": temp_rank,
        "precipitation": precip_rank,
        "combined": combined,
    }


def ensure_parent(path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)