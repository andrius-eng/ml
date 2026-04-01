"""Fetch monthly Heating Degree Days for Lithuania from Eurostat.

Uses the Eurostat dissemination JSON API — no API key required.
Dataset: nrg_chdd_m (monthly HDD/CDD by country).

Output: python/output/weather/hdd.json
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

_EUROSTAT_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
    "nrg_chdd_m?format=JSON&geo=LT&freq=M&indic_nrg=HDD&lang=EN"
)

# Lithuania heating season: Oct – Apr
_HEATING_MONTHS = {10, 11, 12, 1, 2, 3, 4}


def fetch_raw_hdd() -> dict:
    """Fetch raw Eurostat JSON for Lithuania monthly HDD."""
    with urlopen(_EUROSTAT_URL, timeout=30) as resp:
        return json.load(resp)


def parse_hdd_series(raw: dict) -> list[dict]:
    """Convert Eurostat JSON response to a sorted list of {month, hdd} dicts.

    *month* is an ISO year-month string, e.g. ``"2024-01"``.
    """
    time_index: dict = raw["dimension"]["time"]["category"]["index"]
    values: dict = raw.get("value", {})
    rows = []
    for label, idx in time_index.items():
        val = values.get(str(idx))
        if val is not None:
            # Eurostat labels are "YYYY-MM" already in most versions; some older
            # datasets use "YYYYMM" — normalise both.
            if len(label) == 6 and "-" not in label:
                label = f"{label[:4]}-{label[4:]}"
            rows.append({"month": label, "hdd": float(val)})
    return sorted(rows, key=lambda r: r["month"])


def build_hdd_summary(series: list[dict], today: date | None = None) -> dict:
    """Compute year-to-date HDD total and 1991-2020 baseline comparison.

    Eurostat publishes with a significant lag (often 6–15 months).  When the
    latest available data predates the current calendar year, the function falls
    back to the most-recent year that *has* data so the dashboard always shows
    real numbers rather than zeros.

    Returns a dict with:
    - ``recent_months``: last 12 months of HDD data
    - ``data_through``: ISO year-month of the latest observation
    - ``data_lag_months``: approximate lag vs today (informational)
    - ``ytd``: Jan–latest-available-month total vs baseline mean
    - ``heating_season``: most-recent partial/complete Oct–Apr season
    """
    if today is None:
        today = date.today()

    if not series:
        return {
            "fetched_at": today.isoformat(),
            "country": "LT",
            "unit": "HDD",
            "recent_months": [],
            "data_through": None,
            "data_lag_months": None,
            "ytd": {"label": "n/a", "months": 0, "total_hdd": 0.0,
                    "baseline_mean_1991_2020": 0.0, "anomaly": 0.0},
            "heating_season": {"label": "n/a", "months_included": 0,
                               "total_hdd": 0.0, "baseline_mean_1991_2020": 0.0,
                               "anomaly": 0.0},
        }

    latest_month_str = series[-1]["month"]
    latest_yr = int(latest_month_str.split("-")[0])
    latest_mo = int(latest_month_str.split("-")[1])

    # Approximate publication lag in months
    today_total = today.year * 12 + today.month
    latest_total = latest_yr * 12 + latest_mo
    lag_months = today_total - latest_total

    # ── Year-to-date: use the latest year that has data ───────────────────────
    ref_year = latest_yr
    ref_end = latest_month_str  # don't venture beyond known data

    ytd_months = [
        r for r in series
        if r["month"].startswith(f"{ref_year}-") and r["month"] <= ref_end
    ]
    ytd_total = sum(r["hdd"] for r in ytd_months)
    ytd_month_nums = {int(r["month"].split("-")[1]) for r in ytd_months}

    # Baseline: same calendar-months averaged over 1991-2020
    baseline_by_year: dict[int, float] = {}
    for r in series:
        yr_str, mo_str = r["month"].split("-")
        yr, mo = int(yr_str), int(mo_str)
        if 1991 <= yr <= 2020 and mo in ytd_month_nums:
            baseline_by_year[yr] = baseline_by_year.get(yr, 0.0) + r["hdd"]
    ytd_baseline = (
        float(sum(baseline_by_year.values()) / len(baseline_by_year))
        if baseline_by_year else 0.0
    )

    ytd_label_end = f"{latest_yr}-{latest_mo:02d}"
    if latest_mo == 12:
        ytd_label = f"Full year {latest_yr}"
    else:
        import calendar as _cal
        ytd_label = f"Jan–{_cal.month_abbr[latest_mo]} {latest_yr}"

    # ── Heating season: most-recent season with at least one data point ───────
    # Try the season that straddles latest_month first, then fall back one year.
    for season_start_year in (
        (latest_yr - 1 if latest_mo < 10 else latest_yr),
        (latest_yr - 2 if latest_mo < 10 else latest_yr - 1),
    ):
        season_end_year = season_start_year + 1
        season_months_present = []
        for r in series:
            yr_str, mo_str = r["month"].split("-")
            yr, mo = int(yr_str), int(mo_str)
            if mo not in _HEATING_MONTHS:
                continue
            if (yr == season_start_year and mo >= 10) or (yr == season_end_year and mo <= 4):
                if r["month"] <= ref_end:
                    season_months_present.append(r)
        if season_months_present:
            break
    else:
        season_months_present = []
        season_start_year = latest_yr - 1

    season_end_year = season_start_year + 1
    season_total = sum(r["hdd"] for r in season_months_present)
    season_label = f"{season_start_year}/{str(season_end_year)[-2:]}"

    # Baseline for heating season
    season_month_pairs: set[tuple[int, int]] = set()
    for r in season_months_present:
        yr_str, mo_str = r["month"].split("-")
        yr_offset = int(yr_str) - season_start_year
        season_month_pairs.add((yr_offset, int(mo_str)))

    season_baseline_by_year: dict[int, float] = {}
    for r in series:
        yr_str, mo_str = r["month"].split("-")
        yr, mo = int(yr_str), int(mo_str)
        if not (1991 <= yr <= 2020):
            continue
        for yr_offset, ref_mo in season_month_pairs:
            if mo == ref_mo:
                ref_season_start = yr - yr_offset
                if 1991 <= ref_season_start <= 2020:
                    key = ref_season_start
                    season_baseline_by_year[key] = season_baseline_by_year.get(key, 0.0) + r["hdd"]
    season_baseline = (
        float(sum(season_baseline_by_year.values()) / len(season_baseline_by_year))
        if season_baseline_by_year else 0.0
    )

    return {
        "fetched_at": today.isoformat(),
        "country": "LT",
        "unit": "HDD",
        "data_through": latest_month_str,
        "data_lag_months": lag_months,
        "recent_months": series[-12:],
        "ytd": {
            "label": ytd_label,
            "months": len(ytd_months),
            "total_hdd": round(ytd_total, 1),
            "baseline_mean_1991_2020": round(ytd_baseline, 1),
            "anomaly": round(ytd_total - ytd_baseline, 1),
        },
        "heating_season": {
            "label": season_label,
            "months_included": len(season_months_present),
            "total_hdd": round(season_total, 1),
            "baseline_mean_1991_2020": round(season_baseline, 1),
            "anomaly": round(season_total - season_baseline, 1),
        },
    }

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Lithuania HDD from Eurostat")
    parser.add_argument(
        "--output",
        type=str,
        default="python/output/weather/hdd.json",
        help="JSON output path",
    )
    args = parser.parse_args()

    output_path = Path(args.output)
    try:
        raw = fetch_raw_hdd()
        series = parse_hdd_series(raw)
        summary = build_hdd_summary(series)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2, sort_keys=True)
        print(
            f"Saved HDD summary to {output_path} "
            f"({len(series)} monthly observations)"
        )
    except (URLError, TimeoutError, KeyError, ValueError) as exc:
        print(f"WARNING: Eurostat HDD fetch failed ({exc}); skipping output.")


if __name__ == "__main__":
    main()
