# Lithuania Climate Anomaly Dashboard

End-to-end MLOps workflow for ERA5 climate analytics with Airflow orchestration,
PyTorch training, Qdrant-backed retrieval, and a live dashboard.

## Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.10 + PostgreSQL |
| Data | Open-Meteo ERA5 reanalysis |
| Processing | Python 3.11, pandas, numpy |
| Modeling | PyTorch, MLflow-skinny |
| Retrieval | Qdrant local store + lightweight TF-IDF |
| Frontend | Vite, vanilla JS, Chart.js |
| Live updates | Node WebSocket server + periodic export |

## DAGs

Current DAG IDs:

- climate_temperature_model
- lithuania_weather_analysis
- vilnius_march_temperature_anomalies

Each DAG ends with refresh_rag_context to rebuild retrieval context from latest
pipeline artifacts.

## Quick Start

### 1. Install dependencies

```bash
cd ml
uv sync
```

### 2. Validate tests

```bash
uv run python -m pytest python/tests -q
```

Current verified status: 30 passed.

### 3. Export dashboard data

```bash
uv run python python/export_frontend_data.py
```

### 4. Start dashboard UI

```bash
npm run dev
```

Open http://localhost:5173

## Live RAG Query API (Option B)

The dashboard Ask the Pipeline form calls the FastAPI endpoint:

- GET /rag/query?q=your question

Working local command:

```bash
cd ml
uv run uvicorn --app-dir python serve:app --host 127.0.0.1 --port 8000
```

If you run uvicorn from repo root without --app-dir python, you get:

- Error loading ASGI app. Could not import module serve

Quick endpoint test:

```bash
curl "http://127.0.0.1:8000/rag/query?q=How+unusual+is+this+March+in+Vilnius%3F"
```

If port 8000 is occupied:

```bash
lsof -nP -iTCP:8000 -sTCP:LISTEN
kill -9 <PID>
```

## Docker stack

```bash
docker compose -f airflow/docker-compose.yml -f docker-compose.full.yml up -d --build
docker compose -f airflow/docker-compose.yml -f docker-compose.full.yml ps
```

This stack includes:

- Airflow webserver and scheduler
- ws-server for dashboard refresh messages
- frontend (nginx)
- ml-server for FastAPI prediction and RAG query endpoints

## Airflow (local standalone)

Run in its own terminal:

```bash
cd ml/airflow

export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export ML_PROJECT_ROOT="$PWD/.."
export TRAIN_PYTHON_BIN="$PWD/../.venv/bin/python"

env -u VIRTUAL_ENV uv run airflow standalone
```

Open http://localhost:8080 (admin / check .airflow/standalone_admin_password.txt).

Trigger a DAG manually:

```bash
cd ml/airflow
env -u VIRTUAL_ENV \
  AIRFLOW_HOME="$PWD/.airflow" \
  AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
  AIRFLOW__CORE__LOAD_EXAMPLES=False \
  ML_PROJECT_ROOT="$PWD/.." \
  TRAIN_PYTHON_BIN="$PWD/../.venv/bin/python" \
  ./.venv/bin/airflow dags trigger lithuania_weather_analysis
```

## Project Layout

```text
ml/
  airflow/dags/
    train_dag.py
    weather_lithuania_dag.py
    vilnius_march_temperature_dag.py
  python/
    model.py
    metrics.py
    climate_data.py
    climate_train.py
    climate_evaluate.py
    weather_common.py
    weather_fetch.py
    weather_analyze.py
    weather_plot.py
    weather_quality_gate.py
    vilnius_march_fetch.py
    vilnius_march_analyze.py
    vilnius_march_plot.py
    vilnius_march_quality_gate.py
    rag_pipeline.py
    export_frontend_data.py
    serve.py
    quality_gate.py
    plot.py
    diagnostics.py
    run_all.py
    tests/
  server/
    dashboard-ws.js
  src/
    main.js
    styles.css
    data/dashboard.json
  docker/
    airflow/Dockerfile
    frontend/Dockerfile
    frontend/nginx.conf
    ml-pipeline/Dockerfile
    ws-server/Dockerfile
  docker-compose.full.yml
  docker-stack.yml
  pyproject.toml
  vite.config.js
```

## CI

GitHub Actions workflow in .github/workflows/ci.yml runs on push and PR:

- uv sync --frozen
- uv run python -m pytest python/tests
- npm ci and npm run build

## Notes

- pyproject.toml plus uv.lock are the dependency source of truth.
- python/requirements.txt is exported for compatibility workflows.
- python/requirements-airflow-runtime.txt remains curated for airflow image needs.
- ERA5 is reanalysis data on a 0.25 degree grid. For publication-quality
  climatology, cross-validate against Lithuanian Hydrometeorological Service
  station records.
