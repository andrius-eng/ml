"""Build and query a lightweight vector store from ML pipeline outputs."""

from __future__ import annotations

import argparse
import calendar
import json
import math
import os
import re
from datetime import date
from pathlib import Path
from urllib import error, request

from collections import Counter

import numpy as np
import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams


COLLECTION_NAME = "climate_dashboard_docs"
DEFAULT_QUESTIONS = [
    "Is Lithuania currently warmer or colder than normal?",
    "How unusual is this March in Vilnius?",
    "Can I trust the climate model outputs?",
]
STOP_WORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "how", "in",
    "is", "it", "its", "of", "on", "or", "that", "the", "this", "to", "vs", "what", "with",
}

DEFAULT_LLM_PROVIDER = os.environ.get("RAG_LLM_PROVIDER", "ollama").strip().lower()
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://127.0.0.1:11434").rstrip("/")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.2:3b")
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "")
RAG_PROMPT_NAME = "rag-system-prompt"
RAG_PROMPT_ALIAS = "champion"

try:
    import mlflow as _mlflow
    _mlflow_available = True
except Exception:
    _mlflow = None  # type: ignore[assignment]
    _mlflow_available = False

# Fallback template (matches what is registered in MLflow Prompts)
_RAG_PROMPT_FALLBACK = (
    "You are a climate dashboard assistant. Answer using only the provided context. "
    "If the answer is not in context, say so briefly. Keep answer concise and factual.\n\n"
    "Question: {question}\n\n"
    "Context:\n{context}\n"
)
_rag_prompt_template: str | None = None


def _get_rag_prompt() -> str:
    """Return the RAG system prompt, loading from MLflow Prompts if available."""
    global _rag_prompt_template
    if _rag_prompt_template is not None:
        return _rag_prompt_template
    if _mlflow_available and MLFLOW_TRACKING_URI:
        try:
            _mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
            p = _mlflow.load_prompt(f"prompts:/{RAG_PROMPT_NAME}@{RAG_PROMPT_ALIAS}")
            # MLflow uses {{var}} in templates; normalise to Python {var}
            _rag_prompt_template = p.template.replace("{{question}}", "{question}").replace("{{context}}", "{context}")
            print(f"Loaded RAG prompt from MLflow: {RAG_PROMPT_NAME}@{RAG_PROMPT_ALIAS}")
            return _rag_prompt_template
        except Exception as _e:
            print(f"WARNING: could not load RAG prompt from MLflow ({_e}); using fallback")
    _rag_prompt_template = _RAG_PROMPT_FALLBACK
    return _rag_prompt_template


def load_optional_json(path: Path) -> dict | list | None:
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


def normalize_text(text: str) -> str:
    cleaned = re.sub(r"^#+\s*", "", text, flags=re.MULTILINE)
    return re.sub(r"\s+", " ", cleaned).strip()


def first_sentences(text: str, limit: int = 2) -> str:
    cleaned = normalize_text(text)
    if not cleaned:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", cleaned)
    return " ".join(parts[:limit]).strip()


def add_document(documents: list[dict], doc_id: str, title: str, source: str, text: str) -> None:
    cleaned = normalize_text(text)
    if cleaned:
        documents.append({
            "id": doc_id,
            "title": title,
            "source": source,
            "text": cleaned,
        })


def tokenize(text: str) -> list[str]:
    words = [word for word in re.findall(r"[a-z0-9_]+", text.lower()) if word not in STOP_WORDS]
    bigrams = [f"{words[idx]}_{words[idx + 1]}" for idx in range(len(words) - 1)]
    return words + bigrams


def fit_vectorizer(texts: list[str]) -> dict:
    doc_tokens = [tokenize(text) for text in texts]
    document_frequency: Counter[str] = Counter()
    for tokens in doc_tokens:
        document_frequency.update(set(tokens))

    if not document_frequency:
        document_frequency["__empty__"] = 1

    terms = sorted(document_frequency)
    vocab = {term: idx for idx, term in enumerate(terms)}
    n_docs = max(len(doc_tokens), 1)
    idf = {
        term: math.log((1 + n_docs) / (1 + document_frequency[term])) + 1.0
        for term in terms
    }
    return {"vocab": vocab, "idf": idf}


def vectorize(text: str, vectorizer: dict) -> np.ndarray:
    vocab: dict[str, int] = vectorizer["vocab"]
    idf: dict[str, float] = vectorizer["idf"]
    values = np.zeros(len(vocab), dtype=np.float32)
    tokens = tokenize(text)
    if not tokens:
        return values

    counts = Counter(tokens)
    total = float(sum(counts.values()))
    for token, count in counts.items():
        index = vocab.get(token)
        if index is None:
            continue
        tf = count / total
        values[index] = tf * float(idf[token])

    norm = float(np.linalg.norm(values))
    if norm > 0:
        values /= norm
    return values


def build_documents(output_dir: Path) -> list[dict]:
    documents: list[dict] = []

    weather_summary = load_optional_json(output_dir / "weather" / "ytd_summary.json")
    city_rankings = load_optional_json(output_dir / "weather" / "city_rankings.json")
    heat_stress = load_optional_json(output_dir / "weather" / "heat_stress.json")
    hdd = load_optional_json(output_dir / "weather" / "hdd.json")
    climate_eval = load_optional_json(output_dir / "climate" / "climate_evaluation.json")
    if climate_eval is None:
        climate_eval = load_optional_json(output_dir / "evaluation.json")

    if isinstance(weather_summary, dict):
        coverage = weather_summary.get("coverage", {})
        temp = weather_summary.get("temperature", {})
        precip = weather_summary.get("precipitation", {})
        cities = ", ".join(coverage.get("proxy_cities", [])) or "Lithuanian proxy cities"
        add_document(
            documents,
            "weather-overview",
            "Lithuania YTD weather overview",
            "weather/ytd_summary.json",
            (
                f"Lithuania year-to-date weather for {coverage.get('period', 'the current period')} across {cities} "
                f"shows a temperature anomaly of {temp.get('deviation_vs_1991_2020_mean', 0.0):.2f} C with z-score {temp.get('z_score_vs_baseline', 0.0):.2f}. "
                f"Precipitation anomaly is {precip.get('deviation_vs_1991_2020_mean', 0.0):.1f} mm with z-score {precip.get('z_score_vs_baseline', 0.0):.2f}. "
                f"The latest 7-day temperature anomaly is {temp.get('latest_7d_anomaly', 0.0):.2f} C."
            ),
        )

    if isinstance(city_rankings, dict):
        fragments: list[str] = []
        temp_rank = city_rankings.get("temperature", [])
        precip_rank = city_rankings.get("precipitation", [])
        combined = city_rankings.get("combined", [])
        if temp_rank:
            item = temp_rank[0]
            fragments.append(
                f"The strongest temperature signal is {item.get('city', 'unknown')} with anomaly {item.get('anomaly', 0.0):.2f} and z-score {item.get('z_score', 0.0):.2f}."
            )
        if precip_rank:
            item = precip_rank[0]
            fragments.append(
                f"The strongest precipitation signal is {item.get('city', 'unknown')} with anomaly {item.get('anomaly', 0.0):.2f} and z-score {item.get('z_score', 0.0):.2f}."
            )
        if combined:
            item = combined[0]
            fragments.append(
                f"The highest combined anomaly ranking is {item.get('city', 'unknown')} with combined score {item.get('combined_score', 0.0):.2f}."
            )
        add_document(
            documents,
            "weather-city-rankings",
            "City anomaly rankings",
            "weather/city_rankings.json",
            " ".join(fragments),
        )

    # Discover any vilnius_{month}/ directories and index their anomaly data
    for summary_path in sorted(output_dir.glob("vilnius_*/summary.json")):
        month_dir = summary_path.parent
        month_summary = load_optional_json(summary_path)
        if not isinstance(month_summary, dict):
            continue
        dir_stem = month_dir.name  # e.g. "vilnius_march"
        month_name = month_summary.get("month_name", dir_stem.replace("vilnius_", "").capitalize())
        month_slug = month_name.lower()
        csvs = list(month_dir.glob("*_temperature_anomalies.csv"))
        month_csv = csvs[0] if csvs else month_dir / f"{month_slug}_temperature_anomalies.csv"
        if month_csv.exists():
            annual = pd.read_csv(month_csv)
            if not annual.empty:
                latest = annual.sort_values("year").iloc[-1]
                warmest = annual.sort_values("anomaly_c").iloc[-1]
                coldest = annual.sort_values("anomaly_c").iloc[0]
                baseline = month_summary.get("baseline", {})
                window = month_summary.get("window", {})
                add_document(
                    documents,
                    f"{month_slug}-overview",
                    f"Vilnius {month_name} anomaly overview",
                    f"{dir_stem}/summary.json",
                    (
                        f"Vilnius {month_name} through day {window.get('cutoff_day', '?')} in {int(latest['year'])} has mean temperature {float(latest['mean_temp_c']):.2f} C, "
                        f"an anomaly of {float(latest['anomaly_c']):.2f} C, and z-score {float(latest['zscore']):.2f}. "
                        f"The baseline mean is {baseline.get('mean_temp_c', 0.0):.2f} C with standard deviation {baseline.get('std_temp_c', 0.0):.2f} C."
                    ),
                )
                add_document(
                    documents,
                    f"{month_slug}-extremes",
                    f"Vilnius {month_name} historical extremes",
                    f"{dir_stem}/{month_csv.name}",
                    (
                        f"The warmest {month_name} slice in the window was {int(warmest['year'])} with anomaly {float(warmest['anomaly_c']):.2f} C. "
                        f"The coldest was {int(coldest['year'])} with anomaly {float(coldest['anomaly_c']):.2f} C."
                    ),
                )

    if isinstance(climate_eval, dict):
        r2 = float(climate_eval.get("r2", 0.0))
        rmse = float(climate_eval.get("rmse", 0.0))
        mae = float(climate_eval.get("mae", 0.0))
        quality = "usable" if r2 >= 0.65 else "weak"
        add_document(
            documents,
            "ml-evaluation",
            "Climate model evaluation",
            "climate/climate_evaluation.json",
            (
                f"The climate model reports R2 {r2:.4f}, RMSE {rmse:.4f}, and MAE {mae:.4f} on held-out data. "
                f"That indicates {quality} predictive skill for climate anomaly briefing rather than exact deterministic weather forecasting."
            ),
        )

    if isinstance(heat_stress, dict) and heat_stress.get("frost_days"):
        period = heat_stress.get("period", "YTD")
        year = heat_stress.get("current_year", "")
        fragments: list[str] = [
            f"Lithuania {period} {year} heat and frost day counts versus the 1991-2020 baseline:"
        ]
        for metric, label in (
            ("frost_days", "frost days (Tmin < 0 °C)"),
            ("hot_days", "hot days (Tmax > 25 °C)"),
            ("tropical_nights", "tropical nights (Tmin > 20 °C)"),
            ("cold_nights", "cold nights (Tmin < -15 °C)"),
        ):
            entry = heat_stress.get(metric, {})
            if isinstance(entry, dict):
                fragments.append(
                    f"{label.capitalize()}: {entry.get('current', 0)} "
                    f"(baseline {entry.get('baseline_mean_1991_2020', 0):.1f}, "
                    f"anomaly {entry.get('anomaly', 0):+.1f})."
                )
        add_document(
            documents,
            "heat-stress",
            f"Lithuania {year} heat and frost stress",
            "weather/heat_stress.json",
            " ".join(fragments),
        )

    if isinstance(hdd, dict):
        ytd_hdd = hdd.get("ytd", {})
        season_hdd = hdd.get("heating_season", {})
        parts: list[str] = []
        if ytd_hdd:
            parts.append(
                f"Lithuania Heating Degree Days {ytd_hdd.get('label', '')}: "
                f"{ytd_hdd.get('total_hdd', 0):.1f} HDD total, "
                f"versus a 1991-2020 baseline of {ytd_hdd.get('baseline_mean_1991_2020', 0):.1f} HDD "
                f"(anomaly {ytd_hdd.get('anomaly', 0):+.1f} HDD). "
                "A negative anomaly indicates lower heating demand than the historical average, "
                "consistent with a warmer-than-normal winter."
            )
        if season_hdd and season_hdd.get("months_included", 0) > 0:
            parts.append(
                f"Heating season {season_hdd.get('label', '')}: "
                f"{season_hdd.get('total_hdd', 0):.1f} HDD so far "
                f"(baseline {season_hdd.get('baseline_mean_1991_2020', 0):.1f}, "
                f"anomaly {season_hdd.get('anomaly', 0):+.1f} HDD)."
            )
        if parts:
            add_document(
                documents,
                "heating-degree-days",
                "Lithuania Heating Degree Days (Eurostat)",
                "weather/hdd.json",
                " ".join(parts),
            )

    vilnius_report_paths = [
        Path(month_dir.name) / "report.md"
        for month_dir in sorted(output_dir.glob("vilnius_*/"))
    ]
    for rel_path in [Path("weather") / "weather_summary.md", *vilnius_report_paths]:
        report_path = output_dir / rel_path
        if report_path.exists():
            paragraphs = [segment.strip() for segment in report_path.read_text().split("\n\n") if segment.strip()]
            for idx, paragraph in enumerate(paragraphs[:3], start=1):
                add_document(
                    documents,
                    f"report-{rel_path.stem}-{idx}",
                    f"{rel_path.stem} narrative {idx}",
                    rel_path.as_posix(),
                    paragraph,
                )

    return documents


def open_client(rag_dir: Path) -> QdrantClient:
    db_path = rag_dir / "qdrant"
    db_path.mkdir(parents=True, exist_ok=True)
    return QdrantClient(path=str(db_path))


def sync_vector_store(output_dir: Path) -> dict:
    rag_dir = output_dir / "rag"
    rag_dir.mkdir(parents=True, exist_ok=True)
    documents = build_documents(output_dir)
    vectorizer_path = rag_dir / "tfidf_vectorizer.json"

    client = open_client(rag_dir)
    if not documents:
        if vectorizer_path.exists():
            vectorizer_path.unlink()
        return {"collection": COLLECTION_NAME, "documents": 0}

    vectorizer = fit_vectorizer([doc["text"] for doc in documents])
    with open(vectorizer_path, "w", encoding="utf-8") as handle:
        json.dump(vectorizer, handle)

    matrix = [vectorize(doc["text"], vectorizer) for doc in documents]
    vector_size = len(vectorizer["vocab"])

    if client.collection_exists(COLLECTION_NAME):
        client.delete_collection(COLLECTION_NAME)
    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
    )

    points = [
        PointStruct(
            id=idx,
            vector=matrix[idx].tolist(),
            payload={
                "id": doc["id"],
                "title": doc["title"],
                "source": doc["source"],
                "text": doc["text"],
            },
        )
        for idx, doc in enumerate(documents)
    ]
    client.upsert(collection_name=COLLECTION_NAME, points=points)
    return {"collection": COLLECTION_NAME, "documents": len(documents)}


def retrieve(question: str, output_dir: Path, top_k: int = 3) -> list[dict]:
    rag_dir = output_dir / "rag"
    vectorizer_path = rag_dir / "tfidf_vectorizer.json"
    if not vectorizer_path.exists():
        sync_vector_store(output_dir)
    if not vectorizer_path.exists():
        return []

    with open(vectorizer_path, encoding="utf-8") as handle:
        vectorizer = json.load(handle)
    client = open_client(rag_dir)
    query_vector = vectorize(question, vectorizer).tolist()
    response = client.query_points(collection_name=COLLECTION_NAME, query=query_vector, limit=top_k)
    hits = response.points
    return [
        {
            "title": hit.payload.get("title", "untitled"),
            "source": hit.payload.get("source", "unknown"),
            "text": hit.payload.get("text", ""),
            "score": round(float(hit.score), 4),
        }
        for hit in hits
        if float(hit.score) > 0
    ]


def _interpret_answer(raw_answer: str) -> str:
    """Generate a plain-language interpretation from retrieved metrics."""
    import re

    lines: list[str] = []

    # Look for temperature anomaly
    m = re.search(r"temperature anomaly of\s*([+-]?\d+\.?\d*)\s*[°C]", raw_answer, re.IGNORECASE)
    if not m:
        m = re.search(r"anomaly[:\s]+([+-]?\d+\.?\d*)\s*°?C", raw_answer, re.IGNORECASE)
    if m:
        val = float(m.group(1))
        direction = "warmer" if val > 0 else "colder"
        intensity = "slightly" if abs(val) < 1 else ("notably" if abs(val) < 3 else "significantly")
        lines.append(f"This means it has been {intensity} {direction} than the 30-year average ({val:+.1f} °C).")

    # Look for z-score
    m = re.search(r"z[- ]?score[:\s]+([+-]?\d+\.?\d*)", raw_answer, re.IGNORECASE)
    if m:
        z = float(m.group(1))
        absz = abs(z)
        if absz < 0.5:
            desc = "within the normal range"
        elif absz < 1.0:
            desc = "slightly unusual"
        elif absz < 1.5:
            desc = "anomalous — outside typical variability"
        elif absz < 2.0:
            desc = "very anomalous — in the ~5th percentile of historical years"
        else:
            desc = "extreme — a rare event in the 35-year record"
        lines.append(f"A z-score of {z:+.2f} is {desc}.")

    # Look for R² / model performance — require "R2" or "R²" explicitly
    m = re.search(r"[Rr][²2]\s*[=:]\s*([+-]?\d+\.?\d*)", raw_answer)
    if m:
        r2 = float(m.group(1))
        if 0 <= r2 <= 1:  # valid R² range only
            if r2 >= 0.8:
                lines.append(f"R² = {r2:.2f} indicates the model explains most of the variance — a good fit.")
            elif r2 >= 0.65:
                lines.append(f"R² = {r2:.2f} means the model captures the main seasonal pattern reasonably well.")
            elif r2 >= 0:
                lines.append(f"R² = {r2:.2f} suggests the model has limited predictive power.")
        elif r2 < 0:
            lines.append(f"R² = {r2:.2f} means the model is worse than predicting the mean — it needs improvement.")

    if not lines:
        return ""

    return " ".join(lines)


def _answer_year_month_extreme(question: str, output_dir: Path) -> dict | None:
    """Handle: 'which month in 1995 was the warmest/coldest in Kaunas'."""
    normalized = question.strip().lower()

    # Need a 4-digit year
    year_match = re.search(r"\b(1[89]\d{2}|20\d{2})\b", normalized)
    if not year_match:
        return None
    year = int(year_match.group(0))

    # coldest or warmest
    if any(w in normalized for w in ("coldest", "lowest", "least warm", "most cold")):
        extreme = "coldest"
    elif any(w in normalized for w in ("warmest", "hottest", "highest", "most warm")):
        extreme = "warmest"
    else:
        return None

    # Optional city name
    beam_path = output_dir / "beam" / "beam_summary.json"
    if not beam_path.exists():
        return None
    with open(beam_path, encoding="utf-8") as fh:
        beam = json.load(fh)
    cities_data = beam.get("cities", {})
    city_names = list(cities_data.keys())

    chosen_city = None
    for c in city_names:
        if c.lower() in normalized:
            chosen_city = c
            break
    if chosen_city is None:
        # Default to the first city (usually Kaunas/Vilnius alphabetically)
        chosen_city = city_names[0] if city_names else None
    if chosen_city is None:
        return None

    year_data = cities_data[chosen_city].get("data", {}).get(str(year))
    if not year_data:
        available = sorted(cities_data[chosen_city]["data"].keys())
        return {
            "question": question,
            "answer": f"No data for {chosen_city} in {year}. Available years: {available[0]}–{available[-1]}.",
            "interpretation": "",
            "sources": [],
        }

    MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    months_with_data = {int(m): v for m, v in year_data.items()
                        if v.get("anomaly") is not None}
    if not months_with_data:
        return None

    if extreme == "warmest":
        best_m = max(months_with_data, key=lambda m: months_with_data[m]["anomaly"])
    else:
        best_m = min(months_with_data, key=lambda m: months_with_data[m]["anomaly"])

    entry = months_with_data[best_m]
    month_label = MONTH_NAMES[best_m - 1]
    anomaly = entry["anomaly"]
    temp = entry["temp"]
    z = entry.get("z", 0.0)

    # Runner-up for context
    sorted_months = sorted(months_with_data.items(),
                           key=lambda kv: kv[1]["anomaly"],
                           reverse=(extreme == "warmest"))
    runner = sorted_months[1] if len(sorted_months) > 1 else None
    runner_text = ""
    if runner:
        rm, rv = runner
        runner_text = (f" Runner-up: {MONTH_NAMES[rm - 1]} "
                       f"(anomaly {rv['anomaly']:+.2f} °C).")

    answer = (
        f"The {extreme} month in {chosen_city} {year} was {month_label}, "
        f"with mean temperature {temp:.1f} °C and anomaly {anomaly:+.2f} °C "
        f"(z-score {z:+.2f} vs 1991–2025 baseline).{runner_text}"
    )

    return {
        "question": question,
        "answer": answer,
        "interpretation": f"{month_label} {year} was the {extreme} month in {chosen_city} that year.",
        "sources": [
            {
                "title": f"{chosen_city} regional anomalies (Beam)",
                "source": "beam/beam_summary.json",
                "score": 1.0,
            }
        ],
    }


def _answer_extremes_question(question: str, output_dir: Path) -> dict | None:
    """Handle questions like 'which year was the coldest/warmest March'."""
    normalized = question.strip().lower()

    # Detect coldest / warmest intent
    if "coldest" in normalized or "lowest" in normalized or "least warm" in normalized:
        extreme = "coldest"
    elif "warmest" in normalized or "hottest" in normalized or "highest" in normalized:
        extreme = "warmest"
    else:
        return None

    # Detect month name
    month_names = {name.lower(): name for idx, name in enumerate(calendar.month_name) if name}
    month_name = next((name for name in month_names if name in normalized), None)
    if month_name is None:
        return None

    month_label = month_names[month_name]
    month_dir = output_dir / f"vilnius_{month_name}"
    csv_path = month_dir / f"{month_name}_temperature_anomalies.csv"
    if not csv_path.exists():
        return None

    annual = pd.read_csv(csv_path)
    if annual.empty:
        return None

    annual = annual.copy()
    annual["year"] = annual["year"].astype(int)
    annual["anomaly_c"] = annual["anomaly_c"].astype(float)
    annual["mean_temp_c"] = annual["mean_temp_c"].astype(float)

    if extreme == "coldest":
        row = annual.loc[annual["anomaly_c"].idxmin()]
        superlative = "coldest"
    else:
        row = annual.loc[annual["anomaly_c"].idxmax()]
        superlative = "warmest"

    year = int(row["year"])
    anomaly = float(row["anomaly_c"])
    mean_temp = float(row["mean_temp_c"])
    zscore = float(row.get("zscore", 0.0)) if "zscore" in row.index else 0.0

    # Also include second extreme for context
    other = annual.loc[annual["anomaly_c"].idxmax()] if extreme == "coldest" else annual.loc[annual["anomaly_c"].idxmin()]
    other_year = int(other["year"])
    other_anomaly = float(other["anomaly_c"])

    answer = (
        f"The {superlative} {month_label} in the record was {year}, "
        f"with a mean temperature of {mean_temp:.1f} °C and anomaly {anomaly:+.2f} °C"
        f" (z-score {zscore:+.2f}). "
        f"For comparison, the {'warmest' if extreme == 'coldest' else 'coldest'} was {other_year} "
        f"with anomaly {other_anomaly:+.2f} °C."
    )

    return {
        "question": question,
        "answer": answer,
        "interpretation": f"{year} was the {superlative} {month_label} in the dataset.",
        "sources": [
            {
                "title": f"Vilnius {month_label} anomaly table",
                "source": f"vilnius_{month_name}/{month_name}_temperature_anomalies.csv",
                "score": 1.0,
            }
        ],
    }


def _answer_month_comparison(question: str, output_dir: Path) -> dict | None:
    normalized = question.strip().lower()
    month_names = {name.lower(): idx for idx, name in enumerate(calendar.month_name) if name}
    month_name = next((name for name in month_names if name in normalized), None)
    if month_name is None or ("warmer" not in normalized and "colder" not in normalized):
        return None

    year_match = re.search(r"\b(19|20)\d{2}\b", normalized)
    if not year_match:
        return None
    comparison_year = int(year_match.group(0))

    month_dir = output_dir / f"vilnius_{month_name}"
    csv_path = month_dir / f"{month_name}_temperature_anomalies.csv"
    if not csv_path.exists():
        return None

    annual = pd.read_csv(csv_path)
    if annual.empty or comparison_year not in set(annual["year"].astype(int)):
        return None

    latest = annual.sort_values("year").iloc[-1]
    current_year = int(latest["year"])
    if "this" not in normalized and str(current_year) not in normalized:
        return None

    other = annual[annual["year"] == comparison_year].iloc[0]
    current_mean = float(latest["mean_temp_c"])
    other_mean = float(other["mean_temp_c"])
    current_anomaly = float(latest["anomaly_c"])
    other_anomaly = float(other["anomaly_c"])
    delta = current_mean - other_mean
    relation = "warmer" if delta > 0 else "colder" if delta < 0 else "the same temperature as"
    direction_text = "warmer than" if delta > 0 else "colder than" if delta < 0 else "the same as"
    month_label = month_name.capitalize()

    answer = (
        f"Yes. Vilnius {month_label} {current_year} is {direction_text} {comparison_year}. "
        f"Mean temperature is {current_mean:.2f} C versus {other_mean:.2f} C, "
        f"a difference of {delta:+.2f} C over the same cutoff window. "
        f"Anomalies are {current_anomaly:+.2f} C and {other_anomaly:+.2f} C respectively."
        if delta != 0
        else f"Vilnius {month_label} {current_year} is the same as {comparison_year} at {current_mean:.2f} C over the same cutoff window. "
             f"Anomalies are {current_anomaly:+.2f} C and {other_anomaly:+.2f} C respectively."
    )

    return {
        "question": question,
        "answer": answer,
        "interpretation": f"Vilnius {month_label} {current_year} is {relation} {comparison_year} by {delta:+.2f} C.",
        "sources": [
            {
                "title": f"Vilnius {month_label} anomaly table",
                "source": f"vilnius_{month_name}/{month_name}_temperature_anomalies.csv",
                "score": 1.0,
            }
        ],
    }


def _answer_forecast_question(question: str, output_dir: Path) -> dict | None:
    """Answer questions about tomorrow's or a specific date's forecast temperature.

    Detects keywords like 'tomorrow', 'next week', or a specific date and runs
    the trained ClimateModel directly to produce a prediction.
    """
    normalized = question.strip().lower()
    # Only handle forecast-style questions
    forecast_keywords = ('tomorrow', 'next week', 'forecast', 'predict', 'will it be', 'will the temp')
    if not any(kw in normalized for kw in forecast_keywords):
        return None

    try:
        import math as _math
        import torch as _torch
        from pathlib import Path as _Path
        import sys as _sys
        _sys.path.insert(0, str(_Path(__file__).resolve().parent))
        from model import ClimateModel
    except Exception:
        return None

    # Determine target date
    target_date = date.today() + __import__('datetime').timedelta(days=1)  # default: tomorrow
    if 'next week' in normalized:
        target_date = date.today() + __import__('datetime').timedelta(days=7)
    else:
        date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', question)
        if date_match:
            try:
                target_date = date.fromisoformat(date_match.group(0))
            except ValueError:
                pass

    # Load model from .pth file (no MLflow dependency here)
    model_path = output_dir / 'climate' / 'climate_model.pth'
    if not model_path.exists():
        return None

    try:
        model = ClimateModel()
        model.load_state_dict(_torch.load(str(model_path), weights_only=True))
        model.eval()
        doy = target_date.timetuple().tm_yday
        sin_doy = _math.sin(2 * _math.pi * doy / 365)
        cos_doy = _math.cos(2 * _math.pi * doy / 365)
        year_norm = (target_date.year - 1991) / 30.0
        x = _torch.tensor([[sin_doy, cos_doy, year_norm]], dtype=_torch.float32)
        with _torch.no_grad():
            temp = round(float(model(x).item()), 1)
    except Exception:
        return None

    label = 'tomorrow' if target_date == date.today() + __import__('datetime').timedelta(days=1) else str(target_date)
    return {
        'question': question,
        'answer': (
            f'The climate model predicts {temp}°C for {label} ({target_date.isoformat()}) '
            f'in Lithuania. This is a seasonal-trend estimate based on historical ERA5 data, '
            f'not a live weather forecast — actual temperatures may differ by ±4–5°C.'
        ),
        'interpretation': f'Predicted mean temperature: {temp}°C',
        'sources': [{'title': 'ClimateModel (PyTorch MLP)', 'source': 'climate/climate_model.pth', 'score': 1.0}],
    }


def _answer_with_ollama(question: str, matches: list[dict]) -> str | None:
    if not matches:
        return None

    context_lines = []
    for idx, match in enumerate(matches, start=1):
        context_lines.append(
            f"[{idx}] {match['title']} ({match['source']})\n{match['text']}"
        )
    context = "\n\n".join(context_lines)

    prompt = _get_rag_prompt().format(question=question, context=context)

    payload = {
        "model": OLLAMA_MODEL,
        "stream": False,
        "prompt": prompt,
    }
    req = request.Request(
        f"{OLLAMA_BASE_URL}/api/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=45) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        text = (data.get("response") or "").strip()
        return text or None
    except (error.URLError, TimeoutError, json.JSONDecodeError, OSError):
        return None


def answer_question(question: str, output_dir: Path, top_k: int = 3) -> dict:
    deterministic = _answer_forecast_question(question, output_dir)
    if deterministic is not None:
        return deterministic

    deterministic = _answer_year_month_extreme(question, output_dir)
    if deterministic is not None:
        return deterministic

    deterministic = _answer_extremes_question(question, output_dir)
    if deterministic is not None:
        return deterministic

    deterministic = _answer_month_comparison(question, output_dir)
    if deterministic is not None:
        return deterministic

    matches = retrieve(question, output_dir, top_k=top_k)
    if not matches:
        return {
            "question": question,
            "answer": "No relevant pipeline artifacts were available for this question.",
            "sources": [],
        }

    answer = ""
    if DEFAULT_LLM_PROVIDER == "ollama":
        llm_answer = _answer_with_ollama(question, matches)
        if llm_answer:
            answer = llm_answer

    if not answer:
        snippets = [first_sentences(match["text"], limit=1) for match in matches[:2]]
        snippets = [snippet for snippet in snippets if snippet]
        answer = "Based on retrieved DAG outputs, " + " ".join(snippets)

    interpretation = _interpret_answer(answer)
    return {
        "question": question,
        "answer": answer,
        "interpretation": interpretation,
        "sources": [
            {
                "title": match["title"],
                "source": match["source"],
                "score": match["score"],
            }
            for match in matches
        ],
    }


def build_demo_payload(output_dir: Path, questions: list[str] | None = None) -> dict:
    sync_state = sync_vector_store(output_dir)
    selected_questions = questions or DEFAULT_QUESTIONS
    return {
        "generated_at": date.today().isoformat(),
        "collection": sync_state["collection"],
        "corpus_size": sync_state["documents"],
        "questions": [answer_question(question, output_dir) for question in selected_questions],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync and query the climate dashboard vector store")
    parser.add_argument("--output-dir", type=str, default="python/output")
    parser.add_argument("--demo-output", type=str, default="python/output/rag/rag_demo.json")
    parser.add_argument("--question", type=str, default="")
    parser.add_argument("--top-k", type=int, default=3)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    if args.question:
        print(json.dumps(answer_question(args.question, output_dir, top_k=args.top_k), indent=2))
        return

    payload = build_demo_payload(output_dir)
    demo_output = Path(args.demo_output)
    demo_output.parent.mkdir(parents=True, exist_ok=True)
    with open(demo_output, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"Vector RAG demo written to {demo_output}")


if __name__ == "__main__":
    main()