"""Fetch Vilnius daily temperatures for monthly anomaly analysis."""

from __future__ import annotations

import argparse
import calendar
from datetime import date

from weather_common import ensure_parent, fetch_daily_weather


VILNIUS_COORDS = (54.6872, 25.2797)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Vilnius daily temperatures for monthly anomaly analysis")
    parser.add_argument("--execution-date", type=str, default=date.today().isoformat())
    parser.add_argument("--window-years", type=int, default=30)
    parser.add_argument("--month", type=int, default=3, help="Calendar month number (1-12, default: 3 for March)")
    parser.add_argument("--output", type=str, default=None, help="CSV output path (default derived from --month)")
    args = parser.parse_args()

    month_slug = calendar.month_name[args.month].lower()
    output = args.output or f"python/output/vilnius_{month_slug}/raw_daily_weather.csv"

    execution_date = date.fromisoformat(args.execution_date)
    start_year = execution_date.year - args.window_years + 1
    start_date = f"{start_year}-{args.month:02d}-01"

    vilnius_daily = fetch_daily_weather(
        VILNIUS_COORDS[0],
        VILNIUS_COORDS[1],
        start_date,
        execution_date.isoformat(),
    )
    vilnius_daily["city"] = "Vilnius"

    ensure_parent(output)
    vilnius_daily.to_csv(output, index=False)
    print(
        f"Saved Vilnius raw weather data to {output} "
        f"({len(vilnius_daily)} rows; {start_date} to {execution_date.isoformat()})"
    )


if __name__ == "__main__":
    main()