"""Fetch historical daily weather data for Lithuania proxy cities.

Fetches ERA5 daily weather (mean/min/max temperature, precipitation) for the
five Lithuanian proxy cities defined in ``weather_common.LITHUANIA_PROXY_CITIES``
and writes the combined result to a CSV file.

Caching
-------
If the output CSV already exists and is newer than ``--cache-minutes`` (default
60), the script exits immediately without making any network requests.  This
prevents redundant fetches when the DAG is re-triggered within the same hour.

Fetch / fallback behaviour
--------------------------
Each city's data is fetched via ``fetch_daily_weather`` (see ``weather_common``).
That function handles its own 429→forecast-fallback logic internally.  If the
fetch loop itself raises (e.g. both archive and forecast quotas exhausted), this
script catches the exception and falls back to the stale on-disk CSV so that
downstream tasks can continue with the last-known data.

Data range
----------
Default start is ``1991-01-01`` (WMO climate-normal baseline).  The archive API
provides data from 1940 onwards; the forecast fallback covers only the last 92
days.
"""

from __future__ import annotations

import argparse
import time
from datetime import date
from pathlib import Path

import pandas as pd

from weather_common import LITHUANIA_PROXY_CITIES, ensure_parent, fetch_daily_weather


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch historical daily weather data")
    parser.add_argument("--start-date", type=str, default="1991-01-01")
    parser.add_argument("--end-date", type=str, default=date.today().isoformat())
    parser.add_argument(
        "--output",
        type=str,
        default="python/output/weather/raw_daily_weather.csv",
        help="CSV output path for raw city-level daily weather data",
    )
    parser.add_argument(
        "--cache-minutes",
        type=int,
        default=60,
        help="Do not fetch if cached data is newer than this many minutes",
    )
    args = parser.parse_args()

    output_path = Path(args.output)
    if output_path.exists():
        age_secs = time.time() - output_path.stat().st_mtime
        age_minutes = age_secs / 60.0
        if age_minutes <= args.cache_minutes:
            print(f"Using cached weather file {output_path} ({age_minutes:.1f} minutes old)")
            return

    cities = list(LITHUANIA_PROXY_CITIES.items())
    frames: list[pd.DataFrame] = []
    try:
        for i, (city, (lat, lon)) in enumerate(cities, 1):
            print(f"[{i}/{len(cities)}] Fetching {city} ({lat}, {lon}) {args.start_date} -> {args.end_date} ...", flush=True)
            city_df = fetch_daily_weather(lat, lon, args.start_date, args.end_date)
            city_df["city"] = city
            frames.append(city_df)
            print(f"[{i}/{len(cities)}] {city}: {len(city_df)} rows OK", flush=True)
    except Exception as exc:
        if output_path.exists():
            age_minutes = (time.time() - output_path.stat().st_mtime) / 60.0
            print(
                f"WARNING: fetch failed ({exc}); "
                f"falling back to existing cache ({age_minutes:.1f} min old)"
            )
            return
        raise

    raw_daily = pd.concat(frames, ignore_index=True)
    ensure_parent(args.output)
    raw_daily.to_csv(args.output, index=False)
    print(f"Saved raw weather data to {args.output} ({len(raw_daily)} rows)")


if __name__ == "__main__":
    main()