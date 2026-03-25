"""Fetch historical daily weather data for Lithuania proxy cities."""

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

    frames: list[pd.DataFrame] = []
    for city, (lat, lon) in LITHUANIA_PROXY_CITIES.items():
        city_df = fetch_daily_weather(lat, lon, args.start_date, args.end_date)
        city_df["city"] = city
        frames.append(city_df)

    raw_daily = pd.concat(frames, ignore_index=True)
    ensure_parent(args.output)
    raw_daily.to_csv(args.output, index=False)
    print(f"Saved raw weather data to {args.output} ({len(raw_daily)} rows)")


if __name__ == "__main__":
    main()