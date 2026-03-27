"""Shared helpers for Lithuania weather analysis.

Provides city/region definitions, a dual-endpoint weather fetch function,
and aggregation/climatology utilities used by all weather pipeline scripts.

Weather fetch strategy
-----------------------
• Primary endpoint  : ``archive-api.open-meteo.com/v1/archive``
  Full ERA5 history from 1940-onwards; subject to an hourly 10 000-call quota
  (HTTP 429 when exhausted).

• Fallback endpoint : ``api.open-meteo.com/v1/forecast?past_days=<N>``
  Covers the most-recent ≤92 days; operates on a separate quota so it stays
  available when the archive quota is exhausted.

  Limitations of the fallback:
  - Only the last 92 days are returned, not the full 1991-present history
  - Use as a data-freshness safety-net while the archive 429 window resets

Retry / backoff schedule (``fetch_daily_weather``)
---------------------------------------------------
  attempt 1-5:  try archive API
  on 429      : immediately try forecast fallback; on fallback failure wait
                60 × (attempt+1) seconds before next archive attempt
  on other err: wait 2 × (attempt+1) seconds before retry
"""

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


def _fetch_url(url: str, timeout: int = 60) -> dict:
    """Fetch JSON from url and return the 'daily' key."""
    with urlopen(url, timeout=timeout) as response:
        return json.load(response)["daily"]


def fetch_daily_weather(lat: float, lon: float, start: str, end: str) -> pd.DataFrame:
    """Return a DataFrame of daily weather for a single lat/lon point.

    Tries the open-meteo archive API first (full ERA5 history).  On an HTTP 429
    it immediately falls back to the forecast API (last ≤92 days).  Up to five
    attempts are made total, with exponential backoff between retries.

    Parameters
    ----------
    lat, lon: float
        WGS-84 coordinates of the location.
    start, end: str
        ISO-8601 date strings (``YYYY-MM-DD``) defining the requested range.
        Note: the forecast fallback truncates to the last 92 days regardless of
        ``start``.

    Returns
    -------
    pd.DataFrame
        Columns: ``time``, ``temperature_2m_mean``, ``temperature_2m_min``,
        ``temperature_2m_max``, ``precipitation_sum``.  One row per calendar day.

    Raises
    ------
    URLError / HTTPError
        Re-raised after all five attempts are exhausted (both archive and
        forecast fallback failed on every attempt).
    """
    common_params = {
        "latitude": lat,
        "longitude": lon,
        "daily": [
            "temperature_2m_mean", "temperature_2m_min", "temperature_2m_max",
            "precipitation_sum", "snowfall_sum", "sunshine_duration",
            "wind_speed_10m_max", "et0_fao_evapotranspiration",
        ],
        "timezone": "Europe/Vilnius",
    }
    archive_url = "https://archive-api.open-meteo.com/v1/archive?" + urlencode(
        {**common_params, "start_date": start, "end_date": end}, doseq=True
    )
    # Fallback: free forecast API supports up to 92 past days, no hourly quota
    from datetime import date as _date, timedelta
    past_days = (_date.today() - _date.fromisoformat(start)).days
    past_days = min(past_days, 92)
    forecast_url = "https://api.open-meteo.com/v1/forecast?" + urlencode(
        {**common_params, "past_days": past_days, "forecast_days": 1}, doseq=True
    )

    last_exc = None
    for attempt in range(5):
        try:
            print(f"  [{attempt+1}/5] Trying archive API ...", flush=True)
            payload = _fetch_url(archive_url)
            return pd.DataFrame(payload)
        except (TimeoutError, URLError) as e:
            last_exc = e
            is_429 = isinstance(e, HTTPError) and e.code == 429
            if is_429:
                print(f"  [{attempt+1}/5] Archive API 429 — trying forecast fallback ...", flush=True)
                try:
                    payload = _fetch_url(forecast_url)
                    print(f"  Forecast fallback OK ({past_days} past days)", flush=True)
                    return pd.DataFrame(payload)
                except Exception as fe:
                    print(f"  Forecast fallback also failed: {fe}", flush=True)
            if attempt == 4:
                raise last_exc
            delay = 60 * (attempt + 1) if is_429 else 2 * (attempt + 1)
            print(
                f"  [{attempt+1}/5] failed: {e} -- retrying in {delay}s ...",
                flush=True,
            )
            time.sleep(delay)
    raise last_exc


def build_country_daily(raw_daily: pd.DataFrame, current_end: date) -> pd.DataFrame:
    raw = raw_daily.copy()
    raw["time"] = pd.to_datetime(raw["time"])
    raw["year"] = raw["time"].dt.year
    raw["month_day"] = raw["time"].dt.strftime("%m-%d")
    raw = raw[raw["month_day"] <= current_end.strftime("%m-%d")].copy()

    _country_cols = [c for c in [
        "temperature_2m_mean", "precipitation_sum",
        "snowfall_sum", "sunshine_duration", "wind_speed_10m_max", "et0_fao_evapotranspiration",
    ] if c in raw.columns]
    country_daily = (
        raw.groupby("time", as_index=False)[_country_cols].mean()
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
    _agg: dict = dict(
        ytd_mean_temp=("temperature_2m_mean", "mean"),
        ytd_total_precip=("precipitation_sum", "sum"),
        days=("time", "count"),
    )
    for col, name, fn in [
        ("snowfall_sum",              "ytd_total_snowfall",  "sum"),
        ("sunshine_duration",         "ytd_total_sunshine",  "sum"),
        ("wind_speed_10m_max",        "ytd_mean_wind_speed", "mean"),
        ("et0_fao_evapotranspiration","ytd_total_et0",       "sum"),
    ]:
        if col in country_daily.columns:
            _agg[name] = (col, fn)
    return country_daily.groupby("year", as_index=False).agg(**_agg)


def build_city_annual_summary(city_daily: pd.DataFrame) -> pd.DataFrame:
    _agg: dict = dict(
        ytd_mean_temp=("temperature_2m_mean", "mean"),
        ytd_total_precip=("precipitation_sum", "sum"),
        days=("time", "count"),
    )
    for col, name, fn in [
        ("snowfall_sum",              "ytd_total_snowfall",  "sum"),
        ("sunshine_duration",         "ytd_total_sunshine",  "sum"),
        ("wind_speed_10m_max",        "ytd_mean_wind_speed", "mean"),
        ("et0_fao_evapotranspiration","ytd_total_et0",       "sum"),
    ]:
        if col in city_daily.columns:
            _agg[name] = (col, fn)
    return city_daily.groupby(["city", "year"], as_index=False).agg(**_agg)


def build_daily_climatology(daily: pd.DataFrame, group_cols: list[str] | None = None) -> pd.DataFrame:
    if group_cols is None:
        group_cols = []

    baseline = daily[(daily["year"] >= 1991) & (daily["year"] <= 2020)].copy()
    keys = [*group_cols, "month_day"]
    _clim_agg: dict = dict(
        climatology_temp_mean=("temperature_2m_mean", "mean"),
        climatology_temp_std=("temperature_2m_mean", "std"),
        climatology_precip_mean=("precipitation_sum", "mean"),
        climatology_precip_std=("precipitation_sum", "std"),
    )
    if "snowfall_sum" in baseline.columns:
        _clim_agg["climatology_snow_mean"] = ("snowfall_sum", "mean")
        _clim_agg["climatology_snow_std"] = ("snowfall_sum", "std")
    if "sunshine_duration" in baseline.columns:
        _clim_agg["climatology_sunshine_mean"] = ("sunshine_duration", "mean")
    climatology = baseline.groupby(keys, as_index=False).agg(**_clim_agg)
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

    if "snowfall_sum" in enriched.columns and "climatology_snow_mean" in enriched.columns:
        enriched["snow_anomaly"] = enriched["snowfall_sum"] - enriched["climatology_snow_mean"]
        _grp = enriched.groupby(group_cols, group_keys=False) if group_cols else None
        enriched["rolling_7d_snow_anomaly"] = (
            _grp["snow_anomaly"].transform(lambda s: s.rolling(window=7, min_periods=1).sum())
            if _grp is not None else
            enriched["snow_anomaly"].rolling(window=7, min_periods=1).sum()
        )

    return enriched


def build_monthly_anomalies(daily: pd.DataFrame, group_cols: list[str] | None = None, current_year: int = 2026) -> pd.DataFrame:
    if group_cols is None:
        group_cols = []

    frame = daily.copy()
    frame["month"] = pd.to_datetime(frame["time"]).dt.month

    _monthly_agg: dict = dict(
        temp_mean=("temperature_2m_mean", "mean"),
        precip_total=("precipitation_sum", "sum"),
        days=("time", "count"),
    )
    if "snowfall_sum" in frame.columns:
        _monthly_agg["snowfall_total"] = ("snowfall_sum", "sum")
    if "sunshine_duration" in frame.columns:
        _monthly_agg["sunshine_total"] = ("sunshine_duration", "sum")
    monthly_per_year = frame.groupby([*group_cols, "year", "month"], as_index=False).agg(**_monthly_agg)

    baseline = monthly_per_year[(monthly_per_year["year"] >= 1991) & (monthly_per_year["year"] <= 2025)].copy()
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
    baseline = annual[(annual["year"] >= 1991) & (annual["year"] <= 2025)].copy()
    recent = annual[(annual["year"] >= 2026) & (annual["year"] <= current_year - 1)].copy()
    current = annual[annual["year"] == current_year].iloc[0]

    expected = {
        "temp_mean": float(baseline["ytd_mean_temp"].mean()),
        "temp_std": float(baseline["ytd_mean_temp"].std(ddof=1)),
        "precip_mean": float(baseline["ytd_total_precip"].mean()),
        "precip_std": float(baseline["ytd_total_precip"].std(ddof=1)),
    }

    result = {
        "coverage": {
            "baseline_years": [1991, 2025],
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

    if "ytd_total_snowfall" in annual.columns:
        snow_curr = float(current["ytd_total_snowfall"])
        snow_base_mean = float(baseline["ytd_total_snowfall"].mean())
        result["current"]["ytd_total_snowfall_cm"] = snow_curr
        result["snowfall"] = {
            "ytd_total_cm": snow_curr,
            "baseline_mean_cm": snow_base_mean,
            "deviation_vs_baseline_cm": snow_curr - snow_base_mean,
        }
    if "ytd_total_sunshine" in annual.columns:
        sun_curr_s = float(current["ytd_total_sunshine"])
        sun_base_s = float(baseline["ytd_total_sunshine"].mean())
        result["current"]["ytd_total_sunshine_h"] = round(sun_curr_s / 3600.0, 1)
        result["sunshine"] = {
            "ytd_total_hours": round(sun_curr_s / 3600.0, 1),
            "baseline_mean_hours": round(sun_base_s / 3600.0, 1),
            "deviation_vs_baseline_hours": round((sun_curr_s - sun_base_s) / 3600.0, 1),
        }
    if "ytd_mean_wind_speed" in annual.columns:
        result["current"]["ytd_mean_wind_kmh"] = float(current["ytd_mean_wind_speed"])
    if "ytd_total_et0" in annual.columns:
        result["current"]["ytd_total_et0_mm"] = float(current["ytd_total_et0"])

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
    if "snow_anomaly" in current_daily.columns:
        enriched.setdefault("snowfall", {})["latest_daily_anomaly_cm"] = float(latest.get("snow_anomaly", 0.0))
        if "rolling_7d_snow_anomaly" in current_daily.columns:
            enriched["snowfall"]["latest_7d_anomaly_cm"] = float(latest.get("rolling_7d_snow_anomaly", 0.0))
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


def build_heat_stress_summary(
    raw_daily: pd.DataFrame,
    current_year: int,
    current_end: date,
) -> dict:
    """Count frost/heat threshold days for current year vs 1991-2020 baseline.

    Requires ``temperature_2m_min`` and ``temperature_2m_max`` columns in
    *raw_daily* (added by Open-Meteo fetch since this feature was introduced).
    Returns an empty dict if those columns are absent (e.g. old cached CSV).
    """
    if "temperature_2m_min" not in raw_daily.columns or "temperature_2m_max" not in raw_daily.columns:
        return {}

    df = raw_daily.copy()
    df["time"] = pd.to_datetime(df["time"])
    df["year"] = df["time"].dt.year
    df["month_day"] = df["time"].dt.strftime("%m-%d")
    cutoff = current_end.strftime("%m-%d")
    ytd = df[df["month_day"] <= cutoff].copy()

    # Country-average Tmin/Tmax per day (mean over proxy cities)
    daily_avg = ytd.groupby(["year", "time", "month_day"], as_index=False).agg(
        tmin=("temperature_2m_min", "mean"),
        tmax=("temperature_2m_max", "mean"),
    )

    def _counts(subset: pd.DataFrame) -> dict:
        return {
            "frost_days": int((subset["tmin"] < 0).sum()),
            "hot_days": int((subset["tmax"] > 25).sum()),
            "tropical_nights": int((subset["tmin"] > 20).sum()),
            "cold_nights": int((subset["tmin"] < -15).sum()),
        }

    current_counts = _counts(daily_avg[daily_avg["year"] == current_year])
    per_year_baseline = {
        yr: _counts(grp)
        for yr, grp in daily_avg[
            (daily_avg["year"] >= 1991) & (daily_avg["year"] <= 2020)
        ].groupby("year")
    }

    result: dict = {
        "current_year": current_year,
        "period": f"01-01 to {current_end.strftime('%m-%d')}",
    }
    for metric in ("frost_days", "hot_days", "tropical_nights", "cold_nights"):
        current_val = current_counts.get(metric, 0)
        baseline_mean = (
            float(pd.Series([v[metric] for v in per_year_baseline.values()]).mean())
            if per_year_baseline
            else 0.0
        )
        result[metric] = {
            "current": current_val,
            "baseline_mean_1991_2020": round(baseline_mean, 1),
            "anomaly": round(current_val - baseline_mean, 1),
        }
    return result


def ensure_parent(path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)