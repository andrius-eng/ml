"""Build SFT dataset from DAG outputs for local Llama fine-tuning.

This script converts pipeline artifacts under python/output into instruction/input/output
examples suitable for LoRA SFT. It does not train a model by itself.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def load_json(path: Path) -> dict | list | None:
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def add_example(examples: list[dict], instruction: str, context: str, answer: str, source: str) -> None:
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


def build_examples(output_dir: Path) -> list[dict]:
    examples: list[dict] = []

    weather = load_json(output_dir / "weather" / "ytd_summary.json")
    if isinstance(weather, dict):
        period = weather.get("coverage", {}).get("period", "unknown")
        temp = weather.get("temperature", {})
        precip = weather.get("precipitation", {})
        context = json.dumps(weather, ensure_ascii=False)
        add_example(
            examples,
            "Summarize Lithuania year-to-date weather anomaly signal.",
            context,
            (
                f"For {period}, Lithuania temperature anomaly is "
                f"{temp.get('deviation_vs_1991_2020_mean', 0.0):.2f} C "
                f"(z={temp.get('z_score_vs_baseline', 0.0):.2f}) and precipitation anomaly is "
                f"{precip.get('deviation_vs_1991_2020_mean', 0.0):.1f} mm "
                f"(z={precip.get('z_score_vs_baseline', 0.0):.2f})."
            ),
            "weather/ytd_summary.json",
        )

    city_rankings = load_json(output_dir / "weather" / "city_rankings.json")
    if isinstance(city_rankings, dict):
        combined = city_rankings.get("combined", [])
        top = combined[0] if combined else {}
        add_example(
            examples,
            "Which city currently has the strongest combined anomaly signal?",
            json.dumps(city_rankings, ensure_ascii=False),
            (
                f"The top combined anomaly city is {top.get('city', 'unknown')} "
                f"with combined score {float(top.get('combined_score', 0.0)):.2f}."
            ),
            "weather/city_rankings.json",
        )

    for summary_path in sorted(output_dir.glob("vilnius_*/summary.json")):
        summary = load_json(summary_path)
        if not isinstance(summary, dict):
            continue
        month_name = summary.get("month_name", summary_path.parent.name.replace("vilnius_", "").capitalize())
        baseline = summary.get("baseline", {})
        latest_year = summary.get("latest_year", "unknown")
        context = json.dumps(summary, ensure_ascii=False)
        add_example(
            examples,
            f"Describe the Vilnius {month_name} anomaly for the latest year.",
            context,
            (
                f"Vilnius {month_name} latest year is {latest_year}. "
                f"Baseline mean is {float(baseline.get('mean_temp_c', 0.0)):.2f} C "
                f"with std {float(baseline.get('std_temp_c', 0.0)):.2f} C."
            ),
            f"{summary_path.parent.name}/summary.json",
        )

    climate_eval = load_json(output_dir / "climate" / "climate_evaluation.json")
    if isinstance(climate_eval, dict):
        add_example(
            examples,
            "Evaluate model quality from metrics.",
            json.dumps(climate_eval, ensure_ascii=False),
            (
                f"Model quality: R2={float(climate_eval.get('r2', 0.0)):.4f}, "
                f"RMSE={float(climate_eval.get('rmse', 0.0)):.4f}, "
                f"MAE={float(climate_eval.get('mae', 0.0)):.4f}."
            ),
            "climate/climate_evaluation.json",
        )

    beam_summary = load_json(output_dir / "beam" / "beam_summary.json")
    if isinstance(beam_summary, dict):
        cities = beam_summary.get("cities", {})
        for city_name, city_data in list(cities.items())[:5]:
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
                f"Summarize {city_name} regional monthly anomalies for the latest year.",
                json.dumps(city_data, ensure_ascii=False),
                (
                    f"For {city_name} in {latest_year}, monthly anomaly pattern is "
                    + "; ".join(month_points[:12])
                    + "."
                ),
                "beam/beam_summary.json",
            )

    beam_matrix_path = output_dir / "beam" / "monthly_anomaly_matrix.csv"
    if beam_matrix_path.exists():
        try:
            matrix = pd.read_csv(beam_matrix_path)
            if not matrix.empty:
                top = matrix.dropna(subset=["anomaly"]).copy()
                if not top.empty:
                    top_abs = top.reindex(top["anomaly"].abs().sort_values(ascending=False).index).head(10)
                    records = top_abs[["city", "year", "month", "anomaly", "z_score"]].to_dict(orient="records")
                    add_example(
                        examples,
                        "Identify strongest regional monthly anomaly signals.",
                        json.dumps(records, ensure_ascii=False),
                        "Top monthly anomaly signals are the rows with highest absolute anomaly and z-score in this set.",
                        "beam/monthly_anomaly_matrix.csv",
                    )
        except Exception:
            # Keep dataset generation robust even if Beam CSV schema changes.
            pass

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
            "Not enough examples from DAG outputs. Run weather/climate DAGs first so artifacts exist."
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

    print(f"SFT train examples: {len(train)} -> {train_path}")
    print(f"SFT eval examples: {len(eval_)} -> {eval_path}")


if __name__ == "__main__":
    main()
