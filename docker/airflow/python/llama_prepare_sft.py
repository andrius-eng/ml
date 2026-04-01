"""Build SFT dataset from DAG outputs for local Llama fine-tuning.

This script converts pipeline artifacts under python/output into instruction/
input/output examples suitable for LoRA SFT. It does not train a model itself.

Data sources used:
  weather/ytd_summary.json            - national YTD anomaly summary
  weather/city_ytd_summary.json       - per-city YTD summary
  weather/city_rankings.json          - city combined anomaly rankings
  weather/annual_summary.csv          - 36 years of national YTD data
  weather/city_annual_summary.csv     - 180 rows (city x year) YTD data
  weather/city_monthly_anomalies.csv  - per-city per-month z-scores
  weather/country_monthly_anomalies.csv - national monthly z-scores
  weather/city_daily_anomalies.csv    - daily rolling anomaly windows
  vilnius_*/summary.json              - Vilnius monthly baseline metadata
  vilnius_march/march_temperature_anomalies.csv - 30 years of March anomalies
  climate/climate_evaluation.json     - model R2/RMSE/MAE
  climate/climate_predictions.csv     - 1180 prediction vs actual rows
  beam/beam_summary.json              - regional monthly anomaly by city
  beam/monthly_anomaly_matrix.csv     - flat matrix of regional anomalies
  rag/rag_demo.json                   - curated Q&A pairs from RAG pipeline
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_json(path: Path) -> dict | list | None:
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def add_example(
    examples: list[dict],
    instruction: str,
    context: str,
    answer: str,
    source: str,
) -> None:
    context = context.strip()
    answer = answer.strip()
    if not context or not answer:
        return
    examples.append(
        {
            "instruction": instruction,
            "input": context,
            "output": answer,
            "source": source,
        }
    )


def _f(val, decimals: int = 2) -> float:
    try:
        return round(float(val), decimals)
    except (TypeError, ValueError):
        return 0.0


# ---------------------------------------------------------------------------
# Example builders — one function per data source
# ---------------------------------------------------------------------------

def _examples_ytd_summary(output_dir: Path, examples: list[dict]) -> None:
    weather = load_json(output_dir / "weather" / "ytd_summary.json")
    if not isinstance(weather, dict):
        return
    period = weather.get("coverage", {}).get("period", "unknown")
    temp = weather.get("temperature", {})
    precip = weather.get("precipitation", {})
    t_dev = _f(temp.get("deviation_vs_1991_2020_mean", 0))
    t_z = _f(temp.get("z_score_vs_baseline", 0))
    p_dev = _f(precip.get("deviation_vs_1991_2020_mean", 0), 1)
    p_z = _f(precip.get("z_score_vs_baseline", 0))
    direction = "warmer" if t_dev > 0 else "colder"
    significance = "significantly " if abs(t_z) > 1.5 else ""
    add_example(
        examples,
        "Summarize Lithuania year-to-date weather anomaly signal.",
        json.dumps(weather, ensure_ascii=False),
        (
            f"For {period}, Lithuania is {significance}{direction} than the "
            f"1991-2020 baseline by {abs(t_dev):.2f} C (z={t_z:+.2f}). "
            f"Precipitation deviation is {p_dev:+.1f} mm (z={p_z:+.2f})."
        ),
        "weather/ytd_summary.json",
    )
    # Second phrasing of the same fact (helps generalisation)
    add_example(
        examples,
        "Is Lithuania currently warmer or colder than normal this year?",
        json.dumps(weather, ensure_ascii=False),
        (
            f"Lithuania is {significance}{direction} than normal in {period}: "
            f"temperature anomaly {t_dev:+.2f} C, z-score {t_z:+.2f}."
        ),
        "weather/ytd_summary.json",
    )


def _examples_city_ytd(output_dir: Path, examples: list[dict]) -> None:
    city_ytd = load_json(output_dir / "weather" / "city_ytd_summary.json")
    if not isinstance(city_ytd, dict):
        return
    for city, data in city_ytd.items():
        if not isinstance(data, dict):
            continue
        temp = data.get("temperature", {})
        t_dev = _f(temp.get("deviation_vs_1991_2020_mean", 0))
        t_z = _f(temp.get("z_score_vs_baseline", 0))
        direction = "warmer" if t_dev > 0 else "colder"
        add_example(
            examples,
            f"Summarize the year-to-date temperature anomaly for {city}.",
            json.dumps({city: data}, ensure_ascii=False),
            (
                f"{city} is {direction} than the 1991-2020 baseline by "
                f"{abs(t_dev):.2f} C (z={t_z:+.2f}) year-to-date."
            ),
            "weather/city_ytd_summary.json",
        )


def _examples_city_rankings(output_dir: Path, examples: list[dict]) -> None:
    city_rankings = load_json(output_dir / "weather" / "city_rankings.json")
    if not isinstance(city_rankings, dict):
        return
    combined = city_rankings.get("combined", [])
    if combined:
        top = combined[0]
        bottom = combined[-1]
        add_example(
            examples,
            "Which city currently has the strongest combined anomaly signal?",
            json.dumps(city_rankings, ensure_ascii=False),
            (
                f"The strongest combined anomaly is in {top.get('city', '?')} "
                f"(score {_f(top.get('combined_score', 0)):+.2f}). "
                f"The weakest is {bottom.get('city', '?')} "
                f"(score {_f(bottom.get('combined_score', 0)):+.2f})."
            ),
            "weather/city_rankings.json",
        )
    temp_rank = city_rankings.get("temperature", [])
    if temp_rank:
        top_t = temp_rank[0]
        add_example(
            examples,
            "Which city has the most extreme temperature anomaly right now?",
            json.dumps({"temperature_rankings": temp_rank}, ensure_ascii=False),
            (
                f"{top_t.get('city', '?')} has the highest temperature z-score "
                f"at {_f(top_t.get('temp_zscore', 0)):+.2f}."
            ),
            "weather/city_rankings.json",
        )


def _examples_annual_trend(output_dir: Path, examples: list[dict]) -> None:
    rows = load_csv(output_dir / "weather" / "annual_summary.csv")
    if len(rows) < 5:
        return
    # Compare current year to previous year
    latest = rows[-1]
    prev = rows[-2]
    cur_year = latest.get("year", "?")
    prev_year = prev.get("year", "?")
    cur_temp = _f(latest.get("ytd_mean_temp", 0))
    prev_temp = _f(prev.get("ytd_mean_temp", 0))
    diff = _f(cur_temp - prev_temp)
    direction = "warmer" if diff > 0 else "colder"
    context = json.dumps(
        [{"year": r["year"], "ytd_mean_temp": r["ytd_mean_temp"],
          "ytd_total_precip": r["ytd_total_precip"]} for r in rows[-10:]],
        ensure_ascii=False,
    )
    add_example(
        examples,
        f"How does the {cur_year} YTD temperature compare to {prev_year}?",
        context,
        (
            f"{cur_year} YTD mean temp is {cur_temp:.2f} C vs "
            f"{prev_temp:.2f} C in {prev_year}, so {cur_year} is "
            f"{abs(diff):.2f} C {direction} year-on-year."
        ),
        "weather/annual_summary.csv",
    )
    # Warmest/coldest year in the last decade
    decade = rows[-10:]
    warmest = max(decade, key=lambda r: _f(r.get("ytd_mean_temp", 0)))
    coldest = min(decade, key=lambda r: _f(r.get("ytd_mean_temp", 0)))
    add_example(
        examples,
        "Which year in the last decade had the warmest and coldest YTD temperature in Lithuania?",
        context,
        (
            f"In the last decade, {warmest['year']} was warmest "
            f"({_f(warmest['ytd_mean_temp']):.2f} C YTD) and "
            f"{coldest['year']} was coldest "
            f"({_f(coldest['ytd_mean_temp']):.2f} C YTD)."
        ),
        "weather/annual_summary.csv",
    )


def _examples_city_monthly(output_dir: Path, examples: list[dict]) -> None:
    rows = load_csv(output_dir / "weather" / "city_monthly_anomalies.csv")
    if not rows:
        return
    # One example per city-month
    for row in rows:
        city = row.get("city", "?")
        year = row.get("year", "?")
        month = row.get("month", "?")
        t_anom = _f(row.get("temp_anomaly", 0))
        t_z = _f(row.get("temp_zscore", 0))
        p_anom = _f(row.get("precip_anomaly", 0), 1)
        p_z = _f(row.get("precip_zscore", 0))
        t_dir = "warmer" if t_anom > 0 else "colder"
        add_example(
            examples,
            f"Describe the weather anomaly for {city} in month {month} of {year}.",
            json.dumps(row, ensure_ascii=False),
            (
                f"{city} in month {month}/{year} was {t_dir} than the climatology "
                f"by {abs(t_anom):.2f} C (z={t_z:+.2f}). "
                f"Precipitation anomaly: {p_anom:+.1f} mm (z={p_z:+.2f})."
            ),
            "weather/city_monthly_anomalies.csv",
        )
    # Cross-city comparison for the most recent month
    most_recent_month = rows[-1].get("month") if rows else None
    most_recent_year = rows[-1].get("year") if rows else None
    if most_recent_month:
        month_rows = [r for r in rows if r.get("month") == most_recent_month
                      and r.get("year") == most_recent_year]
        if len(month_rows) >= 2:
            sorted_by_z = sorted(month_rows, key=lambda r: _f(r.get("temp_zscore", 0)), reverse=True)
            warmest_city = sorted_by_z[0]
            coldest_city = sorted_by_z[-1]
            add_example(
                examples,
                f"Which city was warmest and coldest relative to normal in month {most_recent_month} of {most_recent_year}?",
                json.dumps(month_rows, ensure_ascii=False),
                (
                    f"In month {most_recent_month}/{most_recent_year}, "
                    f"{warmest_city['city']} was most above normal "
                    f"(z={_f(warmest_city['temp_zscore']):+.2f}) and "
                    f"{coldest_city['city']} was most below normal "
                    f"(z={_f(coldest_city['temp_zscore']):+.2f})."
                ),
                "weather/city_monthly_anomalies.csv",
            )


def _examples_country_monthly(output_dir: Path, examples: list[dict]) -> None:
    rows = load_csv(output_dir / "weather" / "country_monthly_anomalies.csv")
    for row in rows:
        year = row.get("year", "?")
        month = row.get("month", "?")
        t_anom = _f(row.get("temp_anomaly", 0))
        t_z = _f(row.get("temp_zscore", 0))
        p_anom = _f(row.get("precip_anomaly", 0), 1)
        direction = "above" if t_anom > 0 else "below"
        add_example(
            examples,
            f"Describe the national weather anomaly for Lithuania in month {month} of {year}.",
            json.dumps(row, ensure_ascii=False),
            (
                f"Lithuania month {month}/{year}: temperature was {direction} "
                f"normal by {abs(t_anom):.2f} C (z={t_z:+.2f}); "
                f"precipitation anomaly {p_anom:+.1f} mm."
            ),
            "weather/country_monthly_anomalies.csv",
        )


def _examples_city_daily_rolling(output_dir: Path, examples: list[dict]) -> None:
    rows = load_csv(output_dir / "weather" / "city_daily_anomalies.csv")
    if not rows:
        return
    # Group the most recent day per city
    by_city: dict[str, dict] = {}
    for row in rows:
        city = row.get("city", "")
        if city:
            by_city[city] = row  # last row per city is most recent date
    for city, row in by_city.items():
        roll7 = _f(row.get("rolling_7d_temp_anomaly", 0))
        roll30 = _f(row.get("rolling_30d_temp_anomaly", 0))
        date = row.get("time", "?")
        add_example(
            examples,
            f"What is the recent temperature trend for {city}?",
            json.dumps(row, ensure_ascii=False),
            (
                f"As of {date}, {city} has a 7-day rolling temperature anomaly "
                f"of {roll7:+.2f} C and a 30-day rolling anomaly of {roll30:+.2f} C."
            ),
            "weather/city_daily_anomalies.csv",
        )


def _examples_vilnius_monthly(output_dir: Path, examples: list[dict]) -> None:
    """Vilnius monthly anomaly from summary.json + the raw CSV for actual values."""
    for summary_path in sorted(output_dir.glob("vilnius_*/summary.json")):
        summary = load_json(summary_path)
        if not isinstance(summary, dict):
            continue
        month_name = summary.get("month_name",
                                  summary_path.parent.name.replace("vilnius_", "").capitalize())
        month_num = summary.get("month", "?")
        baseline = summary.get("baseline", {})
        latest_year = summary.get("latest_year", "unknown")
        cutoff_day = summary.get("window", {}).get("cutoff_day", "?")

        # Try to load the matching CSV for the actual anomaly value
        csv_path = summary_path.parent / f"{summary_path.parent.name.replace('vilnius_', '')}_temperature_anomalies.csv"
        # e.g. vilnius_march/march_temperature_anomalies.csv
        anomaly_val, z_val = None, None
        if csv_path.exists():
            csv_rows = load_csv(csv_path)
            for r in csv_rows:
                if str(r.get("year", "")) == str(latest_year):
                    anomaly_val = _f(r.get("anomaly_c", 0))
                    z_val = _f(r.get("zscore", 0))
                    break

        if anomaly_val is not None:
            direction = "above" if anomaly_val > 0 else "below"
            answer = (
                f"Vilnius {month_name} {latest_year} (through day {cutoff_day}): "
                f"temperature anomaly {anomaly_val:+.2f} C (z={z_val:+.2f}) "
                f"{direction} the {baseline.get('mean_temp_c', 0):.2f} C baseline."
            )
        else:
            answer = (
                f"Vilnius {month_name} latest year is {latest_year}. "
                f"Baseline mean: {_f(baseline.get('mean_temp_c', 0)):.2f} C "
                f"(std {_f(baseline.get('std_temp_c', 0)):.2f} C)."
            )
        add_example(
            examples,
            f"Describe the Vilnius {month_name} anomaly for the latest year.",
            json.dumps(summary, ensure_ascii=False),
            answer,
            f"{summary_path.parent.name}/summary.json",
        )


def _examples_vilnius_march_trend(output_dir: Path, examples: list[dict]) -> None:
    rows = load_csv(output_dir / "vilnius_march" / "march_temperature_anomalies.csv")
    if len(rows) < 5:
        return
    # Is March warming over time?
    recent = rows[-10:]
    warming_years = sum(1 for r in recent if _f(r.get("anomaly_c", 0)) > 0)
    latest = rows[-1]
    latest_year = latest.get("year", "?")
    latest_anom = _f(latest.get("anomaly_c", 0))
    latest_z = _f(latest.get("zscore", 0))
    warmest = max(rows, key=lambda r: _f(r.get("anomaly_c", 0)))
    coldest = min(rows, key=lambda r: _f(r.get("anomaly_c", 0)))
    context = json.dumps(
        [{"year": r["year"], "anomaly_c": r["anomaly_c"], "zscore": r["zscore"]}
         for r in rows],
        ensure_ascii=False,
    )
    add_example(
        examples,
        "How unusual is this March in Vilnius compared to history?",
        context,
        (
            f"Vilnius March {latest_year} anomaly is {latest_anom:+.2f} C "
            f"(z={latest_z:+.2f}). "
            f"Historically, the warmest March was {warmest['year']} "
            f"({_f(warmest['anomaly_c']):+.2f} C) and coldest was "
            f"{coldest['year']} ({_f(coldest['anomaly_c']):+.2f} C). "
            f"In the last 10 years, {warming_years}/10 Marches were above normal."
        ),
        "vilnius_march/march_temperature_anomalies.csv",
    )
    add_example(
        examples,
        "Is Vilnius March showing a long-term warming trend?",
        context,
        (
            f"Over the recorded period, {warming_years} of the last 10 Marches "
            f"in Vilnius were above the baseline. "
            f"The most recent year ({latest_year}) shows an anomaly of "
            f"{latest_anom:+.2f} C (z={latest_z:+.2f})."
        ),
        "vilnius_march/march_temperature_anomalies.csv",
    )


def _examples_march_year_comparisons(output_dir: Path, examples: list[dict]) -> None:
    """Year-vs-year comparison examples: 'Is this March warmer than 2003?'

    The answer is pre-computed so the tiny model never needs to do arithmetic.
    Context includes the full CSV table so the model learns to read it.
    """
    rows = load_csv(output_dir / "vilnius_march" / "march_temperature_anomalies.csv")
    if len(rows) < 2:
        return

    by_year = {r["year"]: r for r in rows}
    latest_row = rows[-1]
    latest_year = latest_row["year"]
    latest_temp = _f(latest_row["mean_temp_c"])
    context = json.dumps(
        [{"year": r["year"], "mean_temp_c": r["mean_temp_c"],
          "anomaly_c": r["anomaly_c"], "zscore": r["zscore"]}
         for r in rows],
        ensure_ascii=False,
    )

    for other_year, other_row in by_year.items():
        if other_year == latest_year:
            continue
        other_temp = _f(other_row["mean_temp_c"])
        diff = round(latest_temp - other_temp, 2)
        warmer = diff > 0
        abs_diff = abs(diff)
        answer = (
            f"Yes, March {latest_year} ({latest_temp:.2f} C) is warmer than "
            f"March {other_year} ({other_temp:.2f} C) by {abs_diff:.2f} C."
            if warmer else
            f"No, March {latest_year} ({latest_temp:.2f} C) is colder than "
            f"March {other_year} ({other_temp:.2f} C) by {abs_diff:.2f} C."
        )
        add_example(
            examples,
            f"Is this March warmer than {other_year}? Give a yes/no answer with the degree difference.",
            context,
            answer,
            "vilnius_march/march_temperature_anomalies.csv",
        )


def _examples_climate_model(output_dir: Path, examples: list[dict]) -> None:
    climate_eval = load_json(output_dir / "climate" / "climate_evaluation.json")
    if isinstance(climate_eval, dict):
        r2 = _f(climate_eval.get("r2", 0), 4)
        rmse = _f(climate_eval.get("rmse", 0), 4)
        mae = _f(climate_eval.get("mae", 0), 4)
        quality = "good" if r2 > 0.7 else "moderate" if r2 > 0.5 else "poor"
        add_example(
            examples,
            "Evaluate model quality from metrics.",
            json.dumps(climate_eval, ensure_ascii=False),
            (
                f"The climate model has {quality} fit: R2={r2:.4f}, "
                f"RMSE={rmse:.4f} C, MAE={mae:.4f} C on held-out data."
            ),
            "climate/climate_evaluation.json",
        )
        add_example(
            examples,
            "Can you trust the climate model predictions for Lithuania?",
            json.dumps(climate_eval, ensure_ascii=False),
            (
                f"The model achieves R2={r2:.4f} ({quality} fit), meaning it "
                f"explains {r2*100:.1f}% of temperature variance. "
                f"Typical error is {mae:.2f} C (MAE)."
            ),
            "climate/climate_evaluation.json",
        )

    # Sample some high-error predictions from the CSV
    pred_rows = load_csv(output_dir / "climate" / "climate_predictions.csv")
    if pred_rows:
        high_err = sorted(pred_rows,
                          key=lambda r: abs(_f(r.get("residual", 0))),
                          reverse=True)[:5]
        context = json.dumps(high_err, ensure_ascii=False)
        avg_err = sum(abs(_f(r.get("residual", 0))) for r in high_err) / len(high_err)
        add_example(
            examples,
            "Where does the climate model make its largest prediction errors?",
            context,
            (
                f"The 5 largest prediction errors average {avg_err:.2f} C. "
                f"The worst case has a residual of "
                f"{_f(high_err[0].get('residual', 0)):+.2f} C "
                f"(predicted {_f(high_err[0].get('y_pred', 0)):.2f} C, "
                f"actual {_f(high_err[0].get('y_true', 0)):.2f} C)."
            ),
            "climate/climate_predictions.csv",
        )


def _examples_beam(output_dir: Path, examples: list[dict]) -> None:
    beam_summary = load_json(output_dir / "beam" / "beam_summary.json")
    if isinstance(beam_summary, dict):
        cities = beam_summary.get("cities", {})
        for city_name, city_data in cities.items():
            years = city_data.get("years", [])
            if not years:
                continue
            latest_year = max(years)
            latest_data = city_data.get("data", {}).get(str(latest_year), {})
            month_points = []
            for m in sorted(latest_data.keys(), key=lambda x: int(x)):
                entry = latest_data[m]
                anomaly = entry.get("anomaly")
                if anomaly is None:
                    continue
                month_points.append(f"month {m}: {float(anomaly):+.2f} C")
            if not month_points:
                continue
            add_example(
                examples,
                f"Summarize {city_name} regional monthly anomalies for {latest_year}.",
                json.dumps(city_data, ensure_ascii=False),
                (
                    f"For {city_name} in {latest_year}, monthly anomaly pattern: "
                    + "; ".join(month_points[:12]) + "."
                ),
                "beam/beam_summary.json",
            )

    beam_matrix_path = output_dir / "beam" / "monthly_anomaly_matrix.csv"
    matrix_rows = load_csv(beam_matrix_path)
    valid = [r for r in matrix_rows if r.get("anomaly") not in (None, "", "nan")]
    if valid:
        top = sorted(valid, key=lambda r: abs(_f(r.get("anomaly", 0))), reverse=True)[:10]
        records_str = "; ".join(
            f"{r['city']} {r['year']}-{r['month']}: {_f(r['anomaly']):+.2f} C (z={_f(r['z_score']):+.2f})"
            for r in top
        )
        add_example(
            examples,
            "Identify strongest regional monthly anomaly signals.",
            json.dumps(top, ensure_ascii=False),
            f"Top anomaly signals: {records_str}.",
            "beam/monthly_anomaly_matrix.csv",
        )


def _examples_rag_qa(output_dir: Path, examples: list[dict]) -> None:
    """Promote curated RAG Q&A pairs directly into SFT examples."""
    rag = load_json(output_dir / "rag" / "rag_demo.json")
    if not isinstance(rag, dict):
        return
    for qa in rag.get("questions", []):
        question = qa.get("question", "").strip()
        answer = qa.get("answer", "").strip()
        interpretation = qa.get("interpretation", "").strip()
        if not question or not answer:
            continue
        # Combine answer + interpretation when available
        full_answer = answer if not interpretation else f"{answer} {interpretation}"
        sources = qa.get("sources", [])
        context = json.dumps(
            {"question": question, "sources": sources},
            ensure_ascii=False,
        )
        add_example(
            examples,
            question,
            context,
            full_answer,
            "rag/rag_demo.json",
        )


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_examples(output_dir: Path) -> list[dict]:
    examples: list[dict] = []

    _examples_ytd_summary(output_dir, examples)
    _examples_city_ytd(output_dir, examples)
    _examples_city_rankings(output_dir, examples)
    _examples_annual_trend(output_dir, examples)
    _examples_city_monthly(output_dir, examples)
    _examples_country_monthly(output_dir, examples)
    _examples_city_daily_rolling(output_dir, examples)
    _examples_vilnius_monthly(output_dir, examples)
    _examples_vilnius_march_trend(output_dir, examples)
    _examples_march_year_comparisons(output_dir, examples)
    _examples_climate_model(output_dir, examples)
    _examples_beam(output_dir, examples)
    _examples_rag_qa(output_dir, examples)

    # Shuffle so train/eval split is not source-ordered
    random.seed(42)
    random.shuffle(examples)

    return examples


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare SFT JSONL from DAG artifacts")
    parser.add_argument("--output-dir", type=str, default="python/output")
    parser.add_argument("--train-jsonl", type=str, default="python/output/llm/sft_train.jsonl")
    parser.add_argument("--eval-jsonl", type=str, default="python/output/llm/sft_eval.jsonl")
    parser.add_argument("--eval-ratio", type=float, default=0.2)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    examples = build_examples(output_dir)

    if len(examples) < 2:
        raise RuntimeError(
            "Not enough examples from DAG outputs. "
            "Run weather/climate DAGs first so artifacts exist."
        )

    split_at = max(1, int(len(examples) * (1.0 - args.eval_ratio)))
    train = examples[:split_at]
    eval_ = examples[split_at:]
    if not eval_:
        eval_ = train[-1:]

    train_path = Path(args.train_jsonl)
    eval_path = Path(args.eval_jsonl)
    train_path.parent.mkdir(parents=True, exist_ok=True)
    eval_path.parent.mkdir(parents=True, exist_ok=True)

    with open(train_path, "w", encoding="utf-8") as f:
        for row in train:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    with open(eval_path, "w", encoding="utf-8") as f:
        for row in eval_:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    by_source: dict[str, int] = {}
    for ex in examples:
        by_source[ex["source"]] = by_source.get(ex["source"], 0) + 1
    print(f"Total SFT examples: {len(examples)} ({len(train)} train, {len(eval_)} eval)")
    print("Examples by source:")
    for src, count in sorted(by_source.items()):
        print(f"  {count:3d}  {src}")
    print(f"Train -> {train_path}")
    print(f"Eval  -> {eval_path}")


if __name__ == "__main__":
    main()
