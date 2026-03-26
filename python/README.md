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

Note on HDD: Eurostat publishes monthly heating degree day data with a lag, so `eurostat_fetch.py` now reports the latest published full year / heating season instead of returning zeros for the current year.

- weather_common.py: shared fetch and anomaly utilities
- weather_fetch.py: fetch daily weather data
- weather_analyze.py: build summaries and anomaly artifacts
- weather_plot.py: render charts
- weather_quality_gate.py: validate data quality thresholds
- eurostat_fetch.py: fetch monthly heating degree days from Eurostat
- beam_analysis.py: generate regional month-by-month anomaly matrices for the dashboard heatmap

### Vilnius March anomaly pipeline

- vilnius_march_fetch.py: fetch 30-year March daily temperatures for Vilnius
- vilnius_march_analyze.py: compute year-by-year anomaly and z-score vs baseline
- vilnius_march_plot.py: render March anomaly bar chart
- vilnius_march_quality_gate.py: validate year count and z-score bounds

### Retrieval and dashboard bridge

- rag_pipeline.py: builds retrieval corpus and answers questions
- Structured RAG queries (year-vs-year, warmest/coldest year, year-month extremes) are answered directly from source artifacts before falling back to vector retrieval
- export_frontend_data.py: creates src/data/dashboard.json

### Services and local orchestration

- serve.py: FastAPI app with predict and rag query endpoints
- Docker frontend proxies `/api/*` to FastAPI; if you restart `ml-server`, reload or rebuild the frontend only if nginx config changed

### LLM fine-tuning

- llama_prepare_sft.py: builds SFT jsonl files from DAG artifacts
- llama_train_lora.py: trains LoRA adapter on the generated SFT dataset

## Kubernetes Deployment

In the Kubernetes setup, scripts run inside the Airflow containers that are
orchestrated by the `kubernetes/` manifests. Output data lands on the shared
`ml-output` PVC, which is also mounted into `ml-server` and `ws-server` pods.

No script changes are needed — the same DAGs and scripts run identically. The
volume paths (`/opt/airflow/project/python/output`) are preserved in the K8s
ConfigMap (`airflow-config`).


## Airflow Integration

Current DAG IDs and major script paths:

- climate_temperature_model: climate_data, climate_train, climate_evaluate, plot, diagnostics, quality_gate, rag_pipeline
- lithuania_weather_analysis: weather_fetch, weather_analyze, weather_plot, weather_quality_gate, rag_pipeline
- vilnius_march_temperature_anomalies: vilnius_march_fetch, vilnius_march_analyze, vilnius_march_plot, vilnius_march_quality_gate, rag_pipeline
- llama_dag_finetune: llama_prepare_sft, llama_train_lora

## LoRA Troubleshooting (Airflow)

If `llama_dag_finetune.train_lora_adapter` fails with messages like:

- `Disabling PyTorch because PyTorch >= 2.4 is required`
- `NameError: LRScheduler is not defined`

the container has an incompatible transformers stack. Rebuild Airflow services to reinstall pinned compatible versions from `python/requirements-llm-train.txt`:

```bash
cd ml
docker compose -f airflow/docker-compose.yml -f docker-compose.full.yml build airflow-init airflow-webserver airflow-scheduler
docker compose -f airflow/docker-compose.yml -f docker-compose.full.yml up -d airflow-webserver airflow-scheduler
```

Then re-run the failed task from Airflow UI or clear it from CLI.

## FastAPI Server

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

Current verified status: 45 passed.
