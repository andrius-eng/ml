"""Apache Beam pipeline for regional monthly temperature anomaly analysis.

Fetches daily weather for an extended set of cities (Lithuanian + neighbouring
capitals), then uses Beam transforms to compute month-by-month temperature
anomalies per city per year, relative to the 1991-2020 baseline climatology.

Output: monthly_anomaly_matrix.csv  (city × year × month anomaly grid)
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from datetime import date
from pathlib import Path

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

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


# ── Utility transforms ──────────────────────────────────────────────────────


def _parse_record(record):
    """Add year/month fields to a raw daily record."""
    t = record["time"]
    return {
        "city": record["city"],
        "time": t,
        "year": int(t[:4]),
        "month": int(t[5:7]),
        "day": int(t[8:10]),
        "temperature": record["temperature_2m_mean"],
    }


def _monthly_key(record):
    return ((record["city"], record["year"], record["month"]), record["temperature"])


def _baseline_rekey(kv):
    """Re-key ((city, year, month), stats) → ((city, month), annual_mean)."""
    (city, _year, month), stats = kv
    return ((city, month), stats["mean"])


def _compute_anomaly(kv, baselines):
    """Join monthly mean with baseline side-input to produce anomaly row."""
    (city, year, month), stats = kv
    mean_temp = stats["mean"]
    days = stats["count"]
    bl = baselines.get((city, month))
    anomaly = None
    z_score = None
    bl_mean = None
    bl_std = None
    if bl and mean_temp is not None:
        bl_mean = bl["mean"]
        bl_std = bl["std"]
        anomaly = mean_temp - bl_mean
        if bl_std and bl_std > 0:
            z_score = anomaly / bl_std
    return {
        "city": city,
        "year": year,
        "month": month,
        "mean_temp": round(mean_temp, 2) if mean_temp is not None else None,
        "days": days,
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

    end_month = int(end_date[5:7])
    end_day = int(end_date[8:10])

    # ── Prepare source records ──────────────────────────────────────────

    if input_csv and Path(input_csv).exists():
        # Read existing fetched data
        raw_df = pd.read_csv(input_csv)
        existing_cities = set(raw_df["city"].unique())

        # Fetch any cities not already in the file
        extra = {c: coord for c, coord in cities.items() if c not in existing_cities}
        if extra:
            import time as _time
            frames = [raw_df]
            for city_name, (lat, lon) in extra.items():
                print(f"[Beam] Fetching {city_name}...")
                df = fetch_daily_weather(lat, lon, start_date, end_date)
                df["city"] = city_name
                frames.append(df)
                _time.sleep(5)  # rate-limit courtesy
            raw_df = pd.concat(frames, ignore_index=True)
        # Filter to only requested cities
        raw_df = raw_df[raw_df["city"].isin(cities)].copy()
        # Ensure consistent types for Beam schema inference
        raw_df["temperature_2m_mean"] = raw_df["temperature_2m_mean"].astype(float)
        raw_df["time"] = raw_df["time"].astype(str)
        raw_df["city"] = raw_df["city"].astype(str)
        raw_df = raw_df.dropna(subset=["temperature_2m_mean"])
        records = raw_df[["city", "time", "temperature_2m_mean"]].to_dict("records")
    else:
        # Fetch everything via Beam DoFn
        records = None  # will use Beam Create + ParDo

    # ── YTD filter: full year for completed years, date cutoff for current year ──

    end_year = int(end_date[:4])

    def _within_ytd(record):
        if record["year"] < end_year:
            return True  # completed years: show all 12 months
        if record["month"] > end_month:
            return False
        if record["month"] == end_month and record["day"] > end_day:
            return False
        return True

    # ── Run Beam pipeline ───────────────────────────────────────────────

    tmp_jsonl = str(out / "_anomalies_tmp")
    options = PipelineOptions(flags=[], runner="DirectRunner")

    with beam.Pipeline(options=options) as p:
        if records is not None:
            # Records already in memory
            daily = (
                p
                | "CreateRecords" >> beam.Create(records)
                | "Parse" >> beam.Map(_parse_record)
                | "FilterYTD" >> beam.Filter(_within_ytd)
            )
        else:
            # Fetch via Beam ParDo
            daily = (
                p
                | "CreateCities" >> beam.Create(list(cities.items()))
                | "FetchWeather" >> beam.ParDo(FetchCityWeather(start_date, end_date))
                | "ParseFetched" >> beam.Map(_parse_record)
                | "FilterYTDFetched" >> beam.Filter(_within_ytd)
            )

        # Monthly mean per (city, year, month)
        monthly = (
            daily
            | "KeyMonthly" >> beam.Map(_monthly_key)
            | "MonthlyMean" >> beam.CombinePerKey(MonthlyMeanCombineFn())
        )

        # Baseline statistics per (city, month) from 1991-2025
        baseline = (
            daily
            | "FilterBaseline" >> beam.Filter(
                lambda r: BASELINE_START <= r["year"] <= BASELINE_END
            )
            | "KeyBaselineMonthly" >> beam.Map(_monthly_key)
            | "BaselineMonthlyMean" >> beam.CombinePerKey(MonthlyMeanCombineFn())
            | "RekeyBaseline" >> beam.Map(_baseline_rekey)
            | "BaselineStats" >> beam.CombinePerKey(BaselineStatsCombineFn())
        )

        baseline_side = beam.pvalue.AsDict(baseline)

        # Compute anomalies by joining with baseline side input
        anomalies = (
            monthly
            | "ComputeAnomalies" >> beam.Map(_compute_anomaly, baselines=baseline_side)
        )

        # Write results as JSON lines (side-effect append doesn't work in Beam 2.71)
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
    args = parser.parse_args()

    run(
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        input_csv=args.input,
    )


if __name__ == "__main__":
    main()
