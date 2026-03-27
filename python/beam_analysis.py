"""Apache Beam pipeline for regional monthly temperature anomaly analysis.

Fetches daily weather for an extended set of cities (Lithuanian + neighbouring
capitals), then uses Beam transforms to compute month-by-month temperature
anomalies per city per year, relative to the 1991-2020 baseline climatology.

Output: monthly_anomaly_matrix.csv  (city × year × month anomaly grid)
"""

from __future__ import annotations

import argparse
import calendar
import csv
import json
import statistics
from datetime import date, datetime, timezone
from pathlib import Path

import apache_beam as beam
from apache_beam import coders
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.window import GlobalWindows, IntervalWindow, WindowFn

from weather_common import REGION_CITIES, fetch_daily_weather

BASELINE_START = 1991
BASELINE_END = 2025


# ── DoFns & CombineFns ──────────────────────────────────────────────────────


class FetchCityWeather(beam.DoFn):
    """Fetch daily weather for a single city from Open-Meteo Archive API."""

    def __init__(self, start_date: str, end_date: str):
        self.start_date = start_date
        self.end_date = end_date

    def process(self, element):
        city, (lat, lon) = element
        df = fetch_daily_weather(lat, lon, self.start_date, self.end_date)
        df["city"] = city
        for _, row in df.iterrows():
            yield {
                "city": city,
                "time": str(row["time"]),
                "temperature_2m_mean": row["temperature_2m_mean"],
            }


class MonthlyMeanCombineFn(beam.CombineFn):
    """Compute mean temperature and observation count per group."""

    def create_accumulator(self):
        return (0.0, 0)

    def add_input(self, acc, value):
        if value is not None:
            return (acc[0] + value, acc[1] + 1)
        return acc

    def merge_accumulators(self, accs):
        return (sum(a[0] for a in accs), sum(a[1] for a in accs))

    def extract_output(self, acc):
        if acc[1] == 0:
            return {"mean": None, "count": 0}
        return {"mean": acc[0] / acc[1], "count": acc[1]}


class BaselineStatsCombineFn(beam.CombineFn):
    """Compute mean-of-annual-means and std across baseline years."""

    def create_accumulator(self):
        return []

    def add_input(self, acc, value):
        if value is not None:
            acc.append(value)
        return acc

    def merge_accumulators(self, accs):
        merged = []
        for a in accs:
            merged.extend(a)
        return merged

    def extract_output(self, acc):
        if len(acc) < 2:
            return {"mean": acc[0] if acc else 0.0, "std": 0.0}
        return {"mean": statistics.mean(acc), "std": statistics.stdev(acc)}


# ── Beam windowing primitives ───────────────────────────────────────────────


class CalendarMonthWindowFn(WindowFn):
    """Assign each daily record to its calendar-month IntervalWindow.

    Beam WindowFn primitive: replaces the manual (city, year, month) grouping
    key with proper event-time windowing so downstream transforms are window-
    aware and can use DoFn.WindowParam to recover year/month metadata.
    """

    def assign(self, context):
        dt = datetime.fromtimestamp(context.timestamp, tz=timezone.utc)
        start = datetime(dt.year, dt.month, 1, tzinfo=timezone.utc).timestamp()
        last_day = calendar.monthrange(dt.year, dt.month)[1]
        end = datetime(dt.year, dt.month, last_day, 23, 59, 59, tzinfo=timezone.utc).timestamp()
        return [IntervalWindow(start, end)]

    def get_window_coder(self):
        return coders.IntervalWindowCoder()

    def merge(self, merge_context):
        pass  # non-merging


class TagWindowFn(beam.DoFn):
    """Extract year and month from the CalendarMonth window via DoFn.WindowParam.

    Beam DoFn primitive: uses the window's start timestamp (set by
    CalendarMonthWindowFn) to annotate each element with its year and month
    without any manual date-string parsing.
    """

    def process(self, element, window=beam.DoFn.WindowParam):
        city, stats = element
        if stats["mean"] is None:
            return
        dt = datetime.fromtimestamp(window.start, tz=timezone.utc)
        yield {
            "city": city,
            "year": dt.year,
            "month": dt.month,
            "mean_temp": stats["mean"],
            "days": stats["count"],
        }


def _compute_anomaly(record, baselines):
    """Join a monthly record with the baseline side-input to produce an anomaly row."""
    city, year, month = record["city"], record["year"], record["month"]
    mean_temp = record["mean_temp"]
    bl = baselines.get((city, month))
    anomaly = bl_mean = bl_std = z_score = None
    if bl and mean_temp is not None:
        bl_mean, bl_std = bl["mean"], bl["std"]
        anomaly = mean_temp - bl_mean
        if bl_std and bl_std > 0:
            z_score = anomaly / bl_std
    return {
        "city": city, "year": year, "month": month,
        "mean_temp": round(mean_temp, 2) if mean_temp is not None else None,
        "days": record["days"],
        "baseline_mean": round(bl_mean, 2) if bl_mean is not None else None,
        "baseline_std": round(bl_std, 2) if bl_std is not None else None,
        "anomaly": round(anomaly, 2) if anomaly is not None else None,
        "z_score": round(z_score, 2) if z_score is not None else None,
    }


def _write_csv(rows, output_path):
    """Sort and write anomaly rows to CSV."""
    rows.sort(key=lambda r: (r["city"], r["year"], r["month"]))
    fieldnames = [
        "city", "year", "month", "mean_temp", "days",
        "baseline_mean", "baseline_std", "anomaly", "z_score",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


# ── Main pipeline ────────────────────────────────────────────────────────────


def run(
    start_date: str = "1991-01-01",
    end_date: str | None = None,
    output_dir: str = "python/output/beam",
    cities: dict | None = None,
    input_csv: str | None = None,
    fetch_missing_cities: bool = True,
    runner: str = "DirectRunner",
    streaming: bool = False,
    beam_args: list[str] | None = None,
) -> str:
    """Execute the Beam monthly anomaly pipeline.

    Parameters
    ----------
    start_date : str
        First day of historical window (ISO format).
    end_date : str | None
        Last day (defaults to today).
    output_dir : str
        Directory for output files.
    cities : dict | None
        City name → (lat, lon).  Defaults to REGION_CITIES.
    input_csv : str | None
        If provided, read daily records from this CSV instead of fetching.
    fetch_missing_cities : bool
        When input_csv is provided, fetch records for cities not present in the file.
        Disable to avoid external API calls in constrained or rate-limited environments.
    runner : str
        Beam runner name (for example: DirectRunner, FlinkRunner, SparkRunner).
    streaming : bool
        Enable Beam streaming mode for runners that support it.
    beam_args : list[str] | None
        Additional Beam pipeline args passed through to PipelineOptions.

    Returns
    -------
    str  – path to the output CSV
    """
    import pandas as pd

    if end_date is None:
        end_date = date.today().isoformat()
    if cities is None:
        cities = REGION_CITIES

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    out_csv = out / "monthly_anomaly_matrix.csv"

    end_ts = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc).timestamp()

    # ── Prepare source records ──────────────────────────────────────────

    if input_csv and Path(input_csv).exists():
        raw_df = pd.read_csv(input_csv)
        existing_cities = set(raw_df["city"].unique())

        extra = {c: coord for c, coord in cities.items() if c not in existing_cities}
        if extra and fetch_missing_cities:
            import time as _time
            frames = [raw_df]
            for city_name, (lat, lon) in extra.items():
                print(f"[Beam] Fetching {city_name}...")
                try:
                    df = fetch_daily_weather(lat, lon, start_date, end_date)
                    df["city"] = city_name
                    frames.append(df)
                except Exception as exc:
                    print(f"[Beam] WARNING: failed to fetch {city_name}: {exc}")
                _time.sleep(5)
            raw_df = pd.concat(frames, ignore_index=True)
        elif extra and not fetch_missing_cities:
            print(f"[Beam] Skipping fetch for {len(extra)} missing cities from input CSV")

        raw_df = raw_df[raw_df["city"].isin(cities)].copy()
        raw_df["temperature_2m_mean"] = pd.to_numeric(raw_df["temperature_2m_mean"], errors="coerce")
        raw_df["time"] = raw_df["time"].astype(str)
        raw_df["city"] = raw_df["city"].astype(str)
        raw_df = raw_df.dropna(subset=["temperature_2m_mean"])
        records = raw_df[["city", "time", "temperature_2m_mean"]].to_dict("records")
    else:
        records = None  # will use Beam Create + ParDo

    # ── Run Beam pipeline ───────────────────────────────────────────────

    def _to_timestamped(record):
        """Convert a daily record to a TimestampedValue using its date as event time."""
        ts = datetime.fromisoformat(record["time"][:10]).replace(tzinfo=timezone.utc).timestamp()
        return beam.window.TimestampedValue(
            {"city": record["city"], "temperature": record["temperature_2m_mean"]},
            ts,
        )

    tmp_jsonl = str(out / "_anomalies_tmp")
    pipeline_args = list(beam_args or [])

    if not any(arg == "--runner" or arg.startswith("--runner=") for arg in pipeline_args):
        pipeline_args.extend(["--runner", runner])
    if streaming and "--streaming" not in pipeline_args:
        pipeline_args.append("--streaming")

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    resolved_runner = options.get_all_options().get("runner", runner)
    print(f"[Beam] Runner: {resolved_runner}")

    with beam.Pipeline(options=options) as p:
        if records is not None:
            raw = p | "CreateRecords" >> beam.Create(records)
        else:
            raw = (
                p
                | "CreateCities" >> beam.Create(list(cities.items()))
                | "FetchWeather" >> beam.ParDo(FetchCityWeather(start_date, end_date))
            )

        # Assign event-time timestamps from the record date.
        # Records are historical so no future-date filtering is needed;
        # the end_date bound was already applied when loading/fetching data.
        timestamped = (
            raw
            | "AssignTimestamps" >> beam.Map(_to_timestamped)
        )

        # ── Monthly means: use CalendarMonthWindowFn (Beam WindowFn primitive) ──
        # Each element lands in the window for its calendar month. CombinePerKey
        # computes the monthly mean per city within each window. TagWindowFn then
        # reads the window boundaries via DoFn.WindowParam to recover year/month.
        monthly = (
            timestamped
            | "WindowIntoMonths" >> beam.WindowInto(CalendarMonthWindowFn())
            | "KeyByCity" >> beam.Map(lambda r: (r["city"], r["temperature"]))
            | "MonthlyMean" >> beam.CombinePerKey(MonthlyMeanCombineFn())
            | "TagWithWindow" >> beam.ParDo(TagWindowFn())
        )

        # ── Baseline statistics: re-window into GlobalWindows to aggregate ──
        # Filter to baseline years, then collapse back into a single global window
        # so BaselineStatsCombineFn can see all years at once per (city, month).
        baseline = (
            monthly
            | "FilterBaseline" >> beam.Filter(
                lambda r: BASELINE_START <= r["year"] <= BASELINE_END
            )
            | "RewindowGlobal" >> beam.WindowInto(GlobalWindows())
            | "KeyByCityMonth" >> beam.Map(lambda r: ((r["city"], r["month"]), r["mean_temp"]))
            | "BaselineStats" >> beam.CombinePerKey(BaselineStatsCombineFn())
        )

        baseline_side = beam.pvalue.AsDict(baseline)

        # ── Join monthly means with baseline and compute anomalies ──
        anomalies = (
            monthly
            | "RewindowGlobalAll" >> beam.WindowInto(GlobalWindows())
            | "ComputeAnomalies" >> beam.Map(_compute_anomaly, baselines=baseline_side)
        )

        anomalies | "WriteJsonl" >> beam.Map(json.dumps) | beam.io.WriteToText(
            tmp_jsonl, file_name_suffix=".jsonl", shard_name_template=""
        )

    # ── Read back results ───────────────────────────────────────────────

    jsonl_path = tmp_jsonl + ".jsonl"
    collected = []
    if Path(jsonl_path).exists():
        with open(jsonl_path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    collected.append(json.loads(line))
        Path(jsonl_path).unlink()

    # ── Write output ────────────────────────────────────────────────────

    n = _write_csv(collected, str(out_csv))
    print(f"[Beam] Wrote {n} rows to {out_csv}")
    if n == 0:
        raise RuntimeError(
            "[Beam] Pipeline produced 0 rows — check that fetch_eurostat_hdd ran "
            "successfully and that the Flink cluster is reachable."
        )

    # Also write a summary JSON for the frontend
    _write_summary(collected, out / "beam_summary.json", end_date)
    return str(out_csv)


def _write_summary(rows: list[dict], path: Path, end_date: str) -> None:
    """Write a compact JSON summary grouped by city for frontend consumption."""
    from collections import defaultdict

    by_city: dict[str, list] = defaultdict(list)
    for r in rows:
        by_city[r["city"]].append(r)

    cities_summary = {}
    for city, city_rows in sorted(by_city.items()):
        years = sorted({r["year"] for r in city_rows})
        months = sorted({r["month"] for r in city_rows})
        # Build year×month matrix
        matrix: dict[int, dict[int, dict]] = {}
        for r in city_rows:
            matrix.setdefault(r["year"], {})[r["month"]] = {
                "temp": r["mean_temp"],
                "anomaly": r["anomaly"],
                "z": r["z_score"],
            }
        cities_summary[city] = {
            "years": years,
            "months": months,
            "data": {str(y): m for y, m in sorted(matrix.items())},
        }

    summary = {
        "generated_at": date.today().isoformat(),
        "analysis_end": end_date,
        "baseline": f"{BASELINE_START}-{BASELINE_END}",
        "cities": cities_summary,
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"[Beam] Summary written to {path}")


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Apache Beam pipeline: regional monthly temperature anomaly matrix"
    )
    parser.add_argument(
        "--input", default=None,
        help="Path to existing raw_daily_weather.csv (Lithuanian cities). "
             "Extra cities will be fetched automatically.",
    )
    parser.add_argument("--start-date", default="1991-01-01")
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--output-dir", default="python/output/beam")
    parser.add_argument(
        "--no-fetch-missing-cities",
        action="store_true",
        help="Do not fetch missing cities when --input CSV is provided",
    )
    parser.add_argument(
        "--runner",
        default="DirectRunner",
        help="Beam runner (DirectRunner, FlinkRunner, SparkRunner, etc.)",
    )
    parser.add_argument(
        "--streaming",
        action="store_true",
        help="Enable Beam streaming mode (runner must support streaming)",
    )
    args, beam_args = parser.parse_known_args()

    run(
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        input_csv=args.input,
        fetch_missing_cities=not args.no_fetch_missing_cities,
        runner=args.runner,
        streaming=args.streaming,
        beam_args=beam_args,
    )


if __name__ == "__main__":
    main()
