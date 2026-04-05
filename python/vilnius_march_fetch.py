"""Fetch Vilnius daily temperatures for monthly anomaly analysis.

Incremental fetch strategy
--------------------------
If an existing CSV already covers part of the requested date range (e.g. from
a previous successful archive-API call), only the missing tail is fetched and
appended. This preserves years of historical data across runs and minimises
archive-API quota usage.
"""

from __future__ import annotations

import argparse
import calendar
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from weather_common import ensure_parent, fetch_daily_weather


VILNIUS_COORDS = (54.6872, 25.2797)


def _load_existing(output: str) -> pd.DataFrame | None:
    """Return existing CSV as a DataFrame, or None if it doesn't exist / is empty."""
    p = Path(output)
    if not p.exists() or p.stat().st_size == 0:
        return None
    try:
        df = pd.read_csv(p)
        df["time"] = pd.to_datetime(df["time"])
        return df
    except Exception:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Vilnius daily temperatures for monthly anomaly analysis")
    parser.add_argument("--execution-date", type=str, default=date.today().isoformat())
    parser.add_argument("--window-years", type=int, default=85)
    parser.add_argument("--month", type=int, default=3, help="Calendar month number (1-12, default: 3 for March)")
    parser.add_argument("--output", type=str, default=None, help="CSV output path (default derived from --month)")
    args = parser.parse_args()

    month_slug = calendar.month_name[args.month].lower()
    output = args.output or f"python/output/vilnius_{month_slug}/raw_daily_weather.csv"

    execution_date = date.fromisoformat(args.execution_date)
    start_year = execution_date.year - args.window_years + 1
    full_start = date(start_year, args.month, 1)

    existing = _load_existing(output)
    if existing is not None:
        last_date = existing["time"].max().date()
        fetch_start = last_date + timedelta(days=1)
        if fetch_start > execution_date:
            print(
                f"Cache is up to date ({last_date}); no new data needed. "
                f"({len(existing)} rows total)"
            )
            return
        print(f"Cache covers up to {last_date}; fetching delta {fetch_start} → {execution_date}")
    else:
        fetch_start = full_start
        print(f"No cache found; fetching full history {fetch_start} → {execution_date}")

    new_data = fetch_daily_weather(
        VILNIUS_COORDS[0],
        VILNIUS_COORDS[1],
        fetch_start.isoformat(),
        execution_date.isoformat(),
    )
    new_data["city"] = "Vilnius"

    if existing is not None:
        combined = pd.concat([existing, new_data], ignore_index=True)
        # Drop any duplicate dates (prefer newly fetched values)
        combined["time"] = pd.to_datetime(combined["time"])
        combined = combined.drop_duplicates(subset=["time"], keep="last")
        combined = combined.sort_values("time").reset_index(drop=True)
        # Enforce the full window: drop rows before full_start
        combined = combined[combined["time"].dt.date >= full_start]
    else:
        combined = new_data

    years_in_data = combined["time"].dt.year.nunique()
    ensure_parent(output)
    combined.to_csv(output, index=False)
    print(
        f"Saved Vilnius raw weather data to {output} "
        f"({len(combined)} rows; {full_start} to {execution_date.isoformat()}; "
        f"{years_in_data} calendar years)"
    )
    if years_in_data < 5:
        print(
            f"WARNING: only {years_in_data} year(s) of data — archive API may be rate-limited. "
            "Z-score analysis will be unreliable until more history is accumulated."
        )


if __name__ == "__main__":
    main()