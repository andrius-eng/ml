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

- model.py: `ClimateModel` — residual MLP with `input_dim` parameter (default 3,
  expands to up to 8 when weather features are present). Uses `BatchNorm1d`, a
  skip connection, and configurable dropout.
- metrics.py: lightweight metrics used by evaluators

### Climate model pipeline

- climate_data.py: feature engineering and train/test split. Always emits three
  temporal features (`sin_doy`, `cos_doy`, `year_norm`). Adds up to five
  weather-derived features when the source CSV includes the corresponding
  columns: `precip_log1p`, `snow_log1p`, `sunshine_frac_day`, `wind_norm`,
  `et0_norm`. Writes `python/output/climate/feature_columns.json` (ordered list)
  and `python/output/climate/feature_defaults.json` (per-feature training means
  for inference fallback).
- climate_train.py: reads feature columns dynamically from the CSV header;
  instantiates `ClimateModel(input_dim=len(feature_cols))`; logs `features` param
  to MLflow as a comma-separated list.
- climate_evaluate.py: reads feature columns from the test CSV header;
  instantiates `ClimateModel(input_dim=len(feature_cols))` before loading the
  state dict — must match the `input_dim` used at training time.
- plot.py: training curve plot
- diagnostics.py: residual and parity plot
- quality_gate.py: threshold validation for climate model outputs

### Lithuania weather pipeline

Note on HDD: Eurostat publishes monthly heating degree day data with a lag, so
`eurostat_fetch.py` now reports the latest published full year / heating season
instead of returning zeros for the current year.

- weather_common.py: shared fetch and anomaly utilities. Computes country-level
  YTD aggregates for snowfall, sunshine duration, wind speed max, and
  evapotranspiration (ET₀) in addition to temperature and precipitation.
- weather_fetch.py: historical fetch with strict baseline protection.
  Requires at least 30 years of coverage when `--min-years-required` is set
  (default in DAG path), supports `--force-full-fetch`, and uses NASA POWER
  fallback through `weather_common.py` when Open-Meteo archive endpoints return
  429. Writes are blocked when minimum year coverage is not met.
- weather_analyze.py: builds summaries and anomaly artifacts. Logs extended
  MLflow metrics to the `weather-analysis` experiment: `ytd_total_snowfall_cm`,
  `ytd_total_sunshine_h`, `ytd_mean_wind_kmh`, `ytd_total_et0_mm`,
  `trend_direction`, plus existing temperature/precipitation metrics.
- weather_plot.py: render charts
- weather_quality_gate.py: validates data quality thresholds. NaN z-scores (when
  the historical baseline is absent) are treated as warnings rather than
  failures. Logs a separate MLflow run (type `quality_gate`) in the
  `weather-analysis` experiment with `n_extreme_temp_months` and
  `n_extreme_precip_months` metrics.
- eurostat_fetch.py: fetch monthly heating degree days from Eurostat
- beam_analysis.py: Beam pipeline for regional month-by-month anomaly matrices
  (dashboard heatmap). The Flink-compatible path uses composite key grouping
  (`(city, year, month)` + `GroupByKey`/`CombinePerKey`) rather than custom
  Python `WindowFn` objects, which are not portable to Flink Java runtime.
  `CalendarMonthWindowFn` and `TagWindowFn` remain in the file as reference-only
  implementations for non-Flink experimentation.

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

### Admin / one-off scripts (`scripts/`)

These are not invoked by Airflow. Run manually from the project root.

- scripts/register_mlflow_prompts.py: registers the RAG system prompt in the MLflow Prompt Registry and sets the `@champion` alias. Re-run whenever the prompt template changes.

```bash
MLFLOW_TRACKING_URI=http://localhost:5000 uv run python python/scripts/register_mlflow_prompts.py
```

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
