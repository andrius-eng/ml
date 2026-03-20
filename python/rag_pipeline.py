"""Build and query a lightweight vector store from ML pipeline outputs."""

from __future__ import annotations

import argparse
import json
import math
import re
from datetime import date
from pathlib import Path

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


def answer_question(question: str, output_dir: Path, top_k: int = 3) -> dict:
    matches = retrieve(question, output_dir, top_k=top_k)
    if not matches:
        return {
            "question": question,
            "answer": "No relevant pipeline artifacts were available for this question.",
            "sources": [],
        }

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