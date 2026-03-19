# Python Pipelines

This directory contains all executable pipeline scripts used by Airflow DAGs,
the dashboard export path, and the FastAPI endpoints.

## Environment

```bash
cd ml
uv sync
```

Run scripts without activating a shell:

```bash
uv run python python/<script>.py
```

## Script Groups

### Shared foundations

- model.py: model classes and synthetic data helpers
- metrics.py: lightweight metrics used by evaluators

### Climate model pipeline

- climate_data.py: feature engineering and train test split
- climate_train.py: train PyTorch model and write metrics
- climate_evaluate.py: evaluate held-out set and write summary json
- plot.py: training curve plot
- diagnostics.py: residual and parity plot
- quality_gate.py: threshold validation for climate model outputs

### Lithuania weather pipeline

- weather_common.py: shared fetch and anomaly utilities
- weather_fetch.py: fetch daily weather data
- weather_analyze.py: build summaries and anomaly artifacts
- weather_plot.py: render charts
- weather_quality_gate.py: validate data quality thresholds

### Vilnius March anomaly pipeline

- vilnius_march_fetch.py
- vilnius_march_analyze.py
- vilnius_march_plot.py
- vilnius_march_quality_gate.py

### Retrieval and dashboard bridge

- rag_pipeline.py: builds retrieval corpus and answers questions
- export_frontend_data.py: creates src/data/dashboard.json

### Services and local orchestration

- serve.py: FastAPI app with predict and rag query endpoints
- run_all.py: synthetic local demo workflow
- check_python.py: runtime version check helper

## Airflow Integration

Current DAG IDs and major script paths:

- climate_temperature_model: climate_data, climate_train, climate_evaluate, plot, diagnostics, quality_gate, rag_pipeline
- lithuania_weather_analysis: weather_fetch, weather_analyze, weather_plot, weather_quality_gate, rag_pipeline
- vilnius_march_temperature_anomalies: vilnius_march_fetch, vilnius_march_analyze, vilnius_march_plot, vilnius_march_quality_gate, rag_pipeline

## FastAPI (Option B)

Endpoint:

- GET /rag/query?q=...

Working startup command from project root:

```bash
cd ml
uv run uvicorn --app-dir python serve:app --host 127.0.0.1 --port 8000
```

Why this matters: running uvicorn from repo root without app-dir python cannot
import serve.py and fails with module import errors.

Quick test:

```bash
curl "http://127.0.0.1:8000/rag/query?q=Is+Lithuania+currently+warmer+or+colder+than+normal%3F"
```

## Verification

```bash
uv run python -m pytest python/tests -q
```

Current verified status: 30 passed.
