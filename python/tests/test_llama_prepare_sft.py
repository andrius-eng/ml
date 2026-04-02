"""Tests for building deterministic SFT examples from pipeline artifacts."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from llama_prepare_sft import build_examples


def test_build_examples_supports_list_city_ytd_schema_and_filters_outliers(tmp_path):
    weather_dir = tmp_path / "weather"
    weather_dir.mkdir()
    (weather_dir / "city_ytd_summary.json").write_text(
        json.dumps(
            [
                {
                    "city": "Vilnius",
                    "temperature": {
                        "deviation_vs_1991_2020_mean": 1.8,
                        "z_score_vs_baseline": 1.25,
                    },
                },
                {
                    "city": "Kaunas",
                    "temperature": {
                        "deviation_vs_1991_2020_mean": -37.68,
                        "z_score_vs_baseline": -14.68,
                    },
                },
            ]
        )
    )
    (weather_dir / "city_rankings.json").write_text(
        json.dumps(
            {
                "combined": [
                    {"city": "Vilnius", "combined_score": 1.35},
                    {"city": "Klaipeda", "combined_score": -0.85},
                ],
                "temperature": [
                    {"city": "Vilnius", "anomaly": 1.8, "z_score": 1.35},
                ],
            }
        )
    )

    rag_dir = tmp_path / "rag"
    rag_dir.mkdir()
    (rag_dir / "rag_demo.json").write_text(
        json.dumps(
            {
                "questions": [
                    {
                        "question": "What will tomorrow be?",
                        "answer": "Hallucinated answer",
                    }
                ]
            }
        )
    )

    examples = build_examples(tmp_path)

    city_examples = [
        example for example in examples
        if example["source"] == "weather/city_ytd_summary.json"
    ]
    ranking_examples = [
        example for example in examples
        if example["source"] == "weather/city_rankings.json"
    ]

    assert any("Vilnius is warmer" in example["output"] for example in city_examples)
    assert not any("Kaunas" in example["output"] for example in city_examples)
    assert any("+1.35" in example["output"] for example in ranking_examples)
    assert all(example["source"] != "rag/rag_demo.json" for example in examples)


def test_build_examples_skips_pathological_vilnius_march_rows(tmp_path):
    march_dir = tmp_path / "vilnius_march"
    march_dir.mkdir()
    (march_dir / "march_temperature_anomalies.csv").write_text(
        "year,mean_temp_c,anomaly_c,zscore\n"
        "2024,3.27,3.52,1.33\n"
        "2025,3.67,3.92,1.48\n"
        "2026,-99.89,-99.65,-37.52\n"
    )

    examples = build_examples(tmp_path)

    march_examples = [
        example for example in examples
        if example["source"] == "vilnius_march/march_temperature_anomalies.csv"
    ]

    assert march_examples
    assert not any("-99.89 C" in example["output"] for example in march_examples)
    assert not any("2026" in example["output"] for example in march_examples)