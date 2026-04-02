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
    parser.add_argument(
        "--min-years-required",
        type=int,
        default=30,
        help="Minimum distinct year coverage required in cache before baseline is considered healthy",
    )
    parser.add_argument(
        "--force-full-fetch",
        action="store_true",
        help="Ignore incremental delta logic and fetch full start-date -> end-date window",
    )
    args = parser.parse_args()

    output_path = Path(args.output)
    age_minutes: float | None = None
    if output_path.exists():
        age_secs = time.time() - output_path.stat().st_mtime
        age_minutes = age_secs / 60.0

    if output_path.exists() and not args.force_full_fetch:
        assert age_minutes is not None
        if age_minutes <= args.cache_minutes:
            try:
                cached = pd.read_csv(output_path)
                years_present = pd.to_datetime(cached["time"]).dt.year.nunique()
            except Exception as exc:
                years_present = 0
                print(f"WARNING: failed to validate cache coverage ({exc}); forcing fetch")
            if years_present >= args.min_years_required:
                print(
                    f"Using cached weather file {output_path} "
                    f"({age_minutes:.1f} minutes old, {years_present} years)"
                )
                return
            print(
                f"Cache too short ({years_present} years < {args.min_years_required}); "
                "forcing historical backfill"
            )

    # Load existing data so we can do an incremental merge and protect the baseline.
    existing: pd.DataFrame | None = None
    existing_start: str = args.start_date
    if output_path.exists() and not args.force_full_fetch:
        try:
            existing = pd.read_csv(output_path)
            existing_years = pd.to_datetime(existing["time"]).dt.year.nunique()
            if existing_years < args.min_years_required:
                print(
                    f"Existing cache only has {existing_years} years; "
                    "refetching full history"
                )
                existing = None
                existing_start = args.start_date
            else:
                last_date = pd.to_datetime(existing["time"]).max().date().isoformat()
                # Only fetch data we don't already have.
                from datetime import date as _date, timedelta
                next_day = (_date.fromisoformat(last_date) + timedelta(days=1)).isoformat()
                existing_start = next_day
                print(f"Existing data through {last_date}; fetching delta {next_day} -> {args.end_date}")
        except Exception as exc:
            print(f"WARNING: could not read existing CSV ({exc}); will do full fetch")
            existing = None
            existing_start = args.start_date
    elif args.force_full_fetch:
        print("Force full fetch requested; rebuilding full historical window")

    if existing_start > args.end_date:
        print(f"Data already up to date through {args.end_date}; nothing to fetch.")
        return

    cities = list(LITHUANIA_PROXY_CITIES.items())
    frames: list[pd.DataFrame] = []
    try:
        for i, (city, (lat, lon)) in enumerate(cities, 1):
            print(f"[{i}/{len(cities)}] Fetching {city} ({lat}, {lon}) {existing_start} -> {args.end_date} ...", flush=True)
            city_df = fetch_daily_weather(lat, lon, existing_start, args.end_date)
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

    new_data = pd.concat(frames, ignore_index=True)

    # Guard: if the new fetch only covers a short window (e.g. 429 fallback returned
    # < 180 days), keep the existing historical baseline and just append the delta.
    if existing is not None:
        new_days = int(new_data["time"].nunique())
        if new_days < 180:
            print(f"WARNING: fetch returned only {new_days} unique days — merging with existing baseline")
        # Merge: concatenate, deduplicate, sort.
        combined = pd.concat([existing, new_data], ignore_index=True)
        combined["time"] = pd.to_datetime(combined["time"]).dt.date.astype(str)
        combined = combined.drop_duplicates(subset=["time", "city"]).sort_values(["city", "time"]).reset_index(drop=True)
        raw_daily = combined
    else:
        raw_daily = new_data

    # Final guard: never write a CSV that loses the historical baseline.
    years_present = pd.to_datetime(raw_daily["time"]).dt.year.nunique()
    if years_present < args.min_years_required:
        if existing is not None:
            existing_years = pd.to_datetime(existing["time"]).dt.year.nunique()
            if existing_years >= args.min_years_required:
                print(
                    f"WARNING: fetched result has only {years_present} years; "
                    f"keeping existing baseline with {existing_years} years"
                )
                return
        raise RuntimeError(
            f"Historical baseline unavailable: fetched data spans only {years_present} years "
            f"(required >= {args.min_years_required}). Archive API likely rate-limited (429)."
        )

    ensure_parent(args.output)
    raw_daily.to_csv(args.output, index=False)
    print(f"Saved raw weather data to {args.output} ({len(raw_daily)} rows, {years_present} years)")


if __name__ == "__main__":
    main()