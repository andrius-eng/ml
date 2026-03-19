"""Tests for the Qdrant-backed retrieval layer."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from rag_pipeline import answer_question, build_demo_payload, build_documents


MARCH_CSV = """\
year,mean_temp_c,days_observed,anomaly_c,zscore
2022,1.5,16,1.431,0.449
2023,-0.3,16,-0.369,-0.116
2024,2.1,16,2.031,0.637
2025,3.2,10,3.131,0.982
2026,4.3,16,4.231,1.327
"""


def create_pipeline_fixture(tmp_path: Path) -> Path:
    march_dir = tmp_path / "vilnius_march"
    march_dir.mkdir()
    (march_dir / "march_temperature_anomalies.csv").write_text(MARCH_CSV)
    (march_dir / "summary.json").write_text(
        json.dumps(
            {
                "window": {
                    "start_year": 2022,
                    "end_year": 2026,
                    "years_included": 5,
                    "cutoff_day": 16,
                    "execution_date": "2026-03-16",
                },
                "baseline": {"mean_temp_c": 0.069, "std_temp_c": 3.189},
                "latest_year": 2026,
            }
        )
    )
    (march_dir / "report.md").write_text(
        "Vilnius March is running warmer than the long-run mean. The anomaly is visible across the current slice."
    )

    weather_dir = tmp_path / "weather"
    weather_dir.mkdir()
    (weather_dir / "ytd_summary.json").write_text(
        json.dumps(
            {
                "current_year": 2026,
                "coverage": {"period": "01-01 to 03-16", "proxy_cities": ["Vilnius", "Kaunas"]},
                "temperature": {
                    "deviation_vs_1991_2020_mean": -3.6,
                    "z_score_vs_baseline": -1.53,
                    "latest_7d_anomaly": 6.3,
                },
                "precipitation": {
                    "deviation_vs_1991_2020_mean": -18.4,
                    "z_score_vs_baseline": -0.54,
                },
            }
        )
    )
    (weather_dir / "city_rankings.json").write_text(
        json.dumps(
            {
                "combined": [{"city": "Vilnius", "combined_score": 1.35}],
                "temperature": [{"city": "Vilnius", "anomaly": -3.29, "z_score": -1.35}],
                "precipitation": [{"city": "Kaunas", "anomaly": -41.75, "z_score": -1.35}],
            }
        )
    )
    (weather_dir / "weather_summary.md").write_text(
        "Lithuania remains colder than normal on a year-to-date basis, despite a warmer most recent week."
    )

    climate_dir = tmp_path / "climate"
    climate_dir.mkdir()
    (climate_dir / "climate_evaluation.json").write_text(
        json.dumps({"r2": 0.9889, "rmse": 0.2035, "mae": 0.1604})
    )

    return tmp_path


def test_build_documents_collects_multiple_sources(tmp_path):
    output_dir = create_pipeline_fixture(tmp_path)
    docs = build_documents(output_dir)
    doc_ids = {doc["id"] for doc in docs}
    assert "weather-overview" in doc_ids
    assert "march-overview" in doc_ids
    assert "ml-evaluation" in doc_ids


def test_answer_question_retrieves_weather_context(tmp_path):
    output_dir = create_pipeline_fixture(tmp_path)
    result = answer_question("Is Lithuania warmer or colder than normal?", output_dir)
    assert result["sources"]
    assert "retrieved DAG outputs" in result["answer"]


def test_build_demo_payload_includes_default_questions(tmp_path):
    output_dir = create_pipeline_fixture(tmp_path)
    payload = build_demo_payload(output_dir)
    assert payload["corpus_size"] >= 4
    assert len(payload["questions"]) == 3