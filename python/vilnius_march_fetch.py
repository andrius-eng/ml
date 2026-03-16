"""Fetch Vilnius daily temperatures for March anomaly analysis."""

from __future__ import annotations

import argparse
from datetime import date

from weather_common import ensure_parent, fetch_daily_weather


VILNIUS_COORDS = (54.6872, 25.2797)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Vilnius daily temperatures for March anomaly analysis")
    parser.add_argument("--execution-date", type=str, default=date.today().isoformat())
    parser.add_argument("--window-years", type=int, default=30)
    parser.add_argument(
        "--output",
        type=str,
        default="python/output/vilnius_march/raw_daily_weather.csv",
        help="CSV output path for raw Vilnius daily weather data",
    )
    args = parser.parse_args()

    execution_date = date.fromisoformat(args.execution_date)
    start_year = execution_date.year - args.window_years + 1
    start_date = f"{start_year}-03-01"

    vilnius_daily = fetch_daily_weather(
        VILNIUS_COORDS[0],
        VILNIUS_COORDS[1],
        start_date,
        execution_date.isoformat(),
    )
    vilnius_daily["city"] = "Vilnius"

    ensure_parent(args.output)
    vilnius_daily.to_csv(args.output, index=False)
    print(
        f"Saved Vilnius raw weather data to {args.output} "
        f"({len(vilnius_daily)} rows; {start_date} to {execution_date.isoformat()})"
    )


if __name__ == "__main__":
    main()