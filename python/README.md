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
- climate_train.py: resolves the ordered feature contract from
  `feature_columns.json` when present and falls back to the CSV header only when
  the manifest is absent. Logs `features` and `feature_count` to MLflow, writes
  the feature manifest into the run artifacts, and explicitly ensures a model
  version exists for the training run.
- climate_evaluate.py: validates that the held-out test set satisfies the saved
  feature manifest before loading the checkpoint, then reorders inputs using the
  same contract.
- plot.py: training curve plot
- diagnostics.py: residual and parity plot
- quality_gate.py: threshold validation for climate model outputs plus explicit
  `@champion` alias promotion by run ID using the shared MLflow registry helper

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

- vilnius_march_fetch.py: fetch up to 85 years (ERA5 back to 1940) of daily temperatures for Vilnius
- vilnius_march_analyze.py: compute year-by-year anomaly and z-score vs baseline
- vilnius_march_plot.py: render March anomaly bar chart
- vilnius_march_quality_gate.py: validate year count and z-score bounds

### Retrieval and dashboard bridge

- rag_pipeline.py: builds retrieval corpus and answers questions
- Structured RAG queries (year-vs-year, warmest/coldest year, year-month extremes) are answered directly from source artifacts before falling back to vector retrieval
- If `vilnius_<month>/<month>_temperature_anomalies.csv` is missing, month-extremes queries fall back to `beam/beam_summary.json` (defaults to Vilnius when city is not specified).
- export_frontend_data.py: creates src/data/dashboard.json and sanitizes
  non-JSON float tokens (NaN/Infinity) to null so Vite/CI strict JSON parsing
  cannot fail.

### Services and local orchestration

- serve.py: FastAPI app with predict and rag query endpoints. It uses the same
  `feature_columns.json` / `feature_defaults.json` manifest as training so
  `/predict`, `/forecast`, and forecast-style RAG answers keep the saved model's
  input dimension and column order.
- Docker frontend proxies `/api/*` to FastAPI; if you restart `ml-server`, reload or rebuild the frontend only if nginx config changed

### LLM fine-tuning

- llama_prepare_sft.py: builds SFT jsonl files from DAG artifacts
- llama_train_lora.py: trains LoRA adapter on the generated SFT dataset

### Admin / one-off scripts (`scripts/`)

These are not invoked by Airflow. Run manually from the project root.

- scripts/register_mlflow_prompts.py: registers the RAG system prompt in the MLflow Prompt Registry and sets the `@champion` alias. Re-run whenever the prompt template changes.
- scripts/register_climate_model.py: backfills or repairs `ClimateTemperatureModel`
  registration and `@champion` promotion for an existing `mlflow_run_id.txt`.

```bash
MLFLOW_TRACKING_URI=http://localhost:5000 uv run python python/scripts/register_mlflow_prompts.py
MLFLOW_TRACKING_URI=http://localhost:5000 uv run python python/scripts/register_climate_model.py
```

## Model Features & Metrics Reference

### Feature columns

The climate model is trained on an ordered feature vector. The exact set is
recorded in `python/output/climate/feature_columns.json` after each
`prepare_climate_data` run. All scripts load this manifest so training,
evaluation, and inference always use the same column order.

| Column | How it is computed | Why it is needed |
|---|---|---|
| `sin_doy` | $\sin\!\left(\frac{2\pi \cdot \text{DOY}}{365}\right)$ | Encodes the seasonal cycle (see *DOY encoding* below) |
| `cos_doy` | $\cos\!\left(\frac{2\pi \cdot \text{DOY}}{365}\right)$ | Paired with `sin_doy` to give the model a complete seasonal phase |
| `year_norm` | $\frac{\text{year} - 1991}{30}$ | Linear warming trend; 0 = 1991, 1 = 2021 |
| `precip_log1p` | $\ln(1 + \text{daily precipitation mm})$ | Log-transform compresses the heavy right tail of rainfall |
| `snow_log1p` | $\ln(1 + \text{daily snowfall cm})$ | Same reason as precipitation |
| `sunshine_frac_day` | $\frac{\text{sunshine seconds}}{86400}$ | Normalises to 0–1 regardless of day length |
| `wind_norm` | $\frac{\text{wind km/h}}{30}$ | Divides by a typical strong-wind reference so values sit near 0–1 |
| `et0_norm` | $\frac{\text{ET}_0 \text{ mm/day}}{10}$ | Reference evapotranspiration; divided by 10 to match other feature scales |

#### DOY encoding

**DOY** (Day of Year) is an integer in 1–365 (or 366) representing which day of
the year a row belongs to. Day 1 = January 1st, day 91 ≈ April 1st, day 182 ≈
July 1st.

A raw integer DOY cannot be fed directly to a neural network because it is
*discontinuous*: day 365 and day 1 are adjacent in the calendar but 364 apart
numerically. Sinusoidal encoding wraps DOY onto a circle:

$$\text{sin\_doy} = \sin\!\left(\frac{2\pi \cdot \text{DOY}}{365}\right), \quad
\text{cos\_doy} = \cos\!\left(\frac{2\pi \cdot \text{DOY}}{365}\right)$$

Together, `(sin_doy, cos_doy)` form a point on the unit circle that moves
smoothly through summer (maximum) and winter (minimum) without any jump between
December 31 and January 1. The **two columns together** encode one full phase —
neither alone is sufficient because $\sin$ alone cannot distinguish spring from
autumn.

### Evaluation metrics

| Metric | Formula | Units | Interpretation |
|---|---|---|---|
| **MSE** (Mean Squared Error) | $\frac{1}{n}\sum (y_i - \hat{y}_i)^2$ | °C² | Average squared prediction error. Quality gate threshold: ≤ 50 °C². Lower is better. Sensitive to large outliers. |
| **RMSE** (Root MSE) | $\sqrt{\text{MSE}}$ | °C | Same as MSE but in the same units as temperature; easier to interpret. RMSE = 7 means the model is off by ≈ 7 °C on average. |
| **MAE** (Mean Absolute Error) | $\frac{1}{n}\sum |y_i - \hat{y}_i|$ | °C | Average absolute error; less sensitive to outliers than RMSE. |
| **R²** (Coefficient of Determination) | $1 - \frac{\sum(y_i-\hat{y}_i)^2}{\sum(y_i-\bar{y})^2}$ | — | Fraction of variance explained by the model. 1.0 = perfect, 0 = no better than predicting the mean, negative = worse than the mean. Quality gate threshold: ≥ 0.65. |

#### Why MSE ≠ MAE²

A gap between MSE and MAE (e.g. RMSE = 35 while MAE = 3.7) means a small
number of rows have very large errors that dominate the squared metric. This was
the symptom of the -999 fill-value bug in the raw weather CSV: three rows with
`temperature = -597 °C` inflated RMSE to 35 while leaving MAE near 3.7.
After filtering those rows, both metrics agree.

---

## Kubernetes Deployment

In the Kubernetes setup, scripts run inside the Airflow containers that are
orchestrated by the `kubernetes/` manifests. Output data lands on the shared
`ml-output` PVC, which is also mounted into `ml-server` and `ws-server` pods.

No script changes are needed — the same DAGs and scripts run identically. The
volume paths (`/opt/airflow/project/python/output`) are preserved in the K8s
ConfigMap (`airflow-config`).


## Airflow Integration

Current DAG IDs and major script paths:

- era5_temperature_forecast_retrain: climate_data, climate_train, climate_evaluate, plot, diagnostics, quality_gate, rag_pipeline
- lithuania_weather_anomaly: weather_fetch, weather_analyze, weather_plot, weather_quality_gate, rag_pipeline
- vilnius_march_anomaly: vilnius_march_fetch, vilnius_march_analyze, vilnius_march_plot, vilnius_march_quality_gate, rag_pipeline
- llama_lora_finetune: llama_prepare_sft, llama_train_lora

## LoRA Troubleshooting (Airflow)

If `llama_lora_finetune.train_lora_adapter` fails with messages like:

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
