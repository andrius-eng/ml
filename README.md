# Lithuania Climate Anomaly Dashboard

End-to-end MLOps workflow for ERA5 climate analytics with Airflow orchestration,
PyTorch training, Qdrant-backed retrieval, and a live dashboard.

## Prerequisites

- Python 3.11+ managed by [uv](https://docs.astral.sh/uv/)
- Node.js 18+ and npm
- Docker and Docker Compose (for the full stack)

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
uv sync          # Python deps
npm install       # JS deps
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

This reads pipeline outputs under python/output/ and writes src/data/dashboard.json.

### 4. Start dashboard UI

```bash
npm run dev
```

Open http://localhost:5173. You will see:

- KPI cards for current Lithuanian temperature and precipitation anomalies
- A 30-year Vilnius March anomaly bar chart
- City-level z-score comparisons
- ML model regression metrics
- Vector RAG Briefings assembled from pipeline artifacts
- An "Ask the Pipeline" form for live retrieval queries

## Docker Stack

The fastest way to run everything (Airflow + dashboard + RAG API):

```bash
docker compose -f airflow/docker-compose.yml -f docker-compose.full.yml up -d --build
```

To use prebuilt GHCR images (no local image build), pull and run:

```bash
export GHCR_OWNER=andrius
echo <github_pat> | docker login ghcr.io -u YOUR_GITHUB_USERNAME --password-stdin
docker compose -f airflow/docker-compose.yml -f docker-compose.full.yml pull
docker compose -f airflow/docker-compose.yml -f docker-compose.full.yml up -d
```

The GHCR images are intended to stay private. Authenticate before pulling them
locally. If you ever change a GHCR package to public in GitHub, GitHub does not
allow changing that package back to private.

Once running, trigger DAGs from Airflow UI at http://localhost:8080 (admin / admin).
The dashboard at http://localhost:5173 updates automatically via WebSocket.

## Airflow (Local Standalone)

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

## Live RAG Query API

The dashboard "Ask the Pipeline" form sends questions to a FastAPI endpoint.
Start the API server in a separate terminal:

```bash
cd ml
uv run uvicorn --app-dir python serve:app --host 127.0.0.1 --port 8000
```

The --app-dir python flag is required so uvicorn can find serve.py.
Without it you get: Could not import module "serve".

Test it:

```bash
curl "http://127.0.0.1:8000/rag/query?q=Is+Lithuania+warmer+than+usual%3F"
```

Example response:

```json
{
  "question": "Is Lithuania warmer than usual?",
  "answer": "Based on retrieved DAG outputs, Lithuania year-to-date weather shows a temperature anomaly of -3.46 C with z-score -1.47.",
  "sources": [{"title": "weather_summary narrative 1", "source": "weather/weather_summary.md", "score": 0.41}]
}
```

Note: the API returns meaningful answers only after DAGs have run and produced
artifacts under python/output/. Before that, you get "No relevant pipeline
artifacts were available."

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

GitHub Actions workflows:

- .github/workflows/ci.yml
  - npm ci, npm run build, format check
  - uv sync, dry-run train check, pytest
  - full Docker stack smoke test (build + airflow-init + endpoint checks)
- .github/workflows/docker-images.yml
  - builds and pushes images to ghcr.io on push to main/master and manual dispatch
  - images: ml-airflow-custom, ml-ws-server, ml-frontend, ml-ml-pipeline
  - images are expected to remain private in GHCR; local pulls require auth

## Notes

- pyproject.toml plus uv.lock are the dependency source of truth.
- python/requirements.txt is exported for compatibility workflows.
- python/requirements-airflow-runtime.txt remains curated for airflow image needs.
- ERA5 is reanalysis data on a 0.25 degree grid. For publication-quality
  climatology, cross-validate against Lithuanian Hydrometeorological Service
  station records.
