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

    *series* — sorted list of {month, hdd} dicts as returned by
    :func:`parse_hdd_series`.

    Returns a dict with:
    - ``recent_months``: last 12 months of HDD data
    - ``ytd``: current Jan–<today> period total vs baseline mean
    - ``heating_season``: Oct–Apr (partial) total vs baseline mean
    """
    if today is None:
        today = date.today()

    current_year = today.year
    this_month = today.strftime("%Y-%m")
    month_by_key = {r["month"]: r["hdd"] for r in series}

    # ── Year-to-date (Jan–current month) ──────────────────────────────────────
    ytd_months = [
        r for r in series
        if r["month"].startswith(f"{current_year}-") and r["month"] <= this_month
    ]
    ytd_total = sum(r["hdd"] for r in ytd_months)
    ytd_month_nums = {int(r["month"].split("-")[1]) for r in ytd_months}

    # Baseline: same months averaged over 1991-2020
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

    # ── Current heating season (Oct prev-year – Apr this year, partial) ───────
    season_start_year = current_year - 1 if today.month < 10 else current_year
    season_label = f"{season_start_year}/{str(current_year)[-2:]}"
    season_months_present = []
    for r in series:
        yr_str, mo_str = r["month"].split("-")
        yr, mo = int(yr_str), int(mo_str)
        if mo not in _HEATING_MONTHS:
            continue
        # Oct–Dec of start year OR Jan–Apr of end year
        if (yr == season_start_year and mo >= 10) or (yr == current_year and mo <= 4):
            if r["month"] <= this_month:
                season_months_present.append(r)
    season_total = sum(r["hdd"] for r in season_months_present)

    # Baseline for heating season: same set of calender-months, 1991-2020
    season_month_pairs: set[tuple[int, int]] = set()
    for r in season_months_present:
        yr_str, mo_str = r["month"].split("-")
        yr_offset = int(yr_str) - season_start_year  # 0 or 1
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
        "recent_months": series[-12:],
        "ytd": {
            "label": f"Jan\u2013{today.strftime('%b')} {current_year}",
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
