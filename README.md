# Lithuania Climate Anomaly Dashboard

An end-to-end MLOps pipeline that fetches ERA5 reanalysis data, computes
temperature and precipitation anomalies, and serves a live Chart.js dashboard.
Designed as a credible early-warning signal for agri, energy, and logistics
clients exposed to temperature risk — the kind of system you'd hand to a paying
client on Monday.

---

## What this demonstrates

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.10 (LocalExecutor + PostgreSQL via Docker) |
| Data source | Open-Meteo ERA5 reanalysis API (1940–present, 9 km resolution) |
| Processing | Python 3.11 · pandas · numpy |
| ML training | PyTorch feed-forward network · MLflow experiment tracking |
| Quality gates | Per-DAG validation tasks that fail the pipeline on bad data |
| Testing | pytest 9 · 17 unit + integration smoke tests · `uv run pytest` |
| Frontend | Vite + vanilla JS + Chart.js — live anomaly charts from pipeline JSON |
| Deployment | `docker compose up` for Airflow; `npm run build` for the dashboard |

---

## The three Airflow DAGs

### `mlflow_torch_training`
Generates synthetic weather-like data, trains a PyTorch regression model, and
logs all parameters, metrics, and the model artifact to MLflow. Run on demand
to reproduce or compare experiments.

### `lithuania_weather_analysis`
Fetches ERA5 daily temperature and precipitation for Vilnius, Kaunas, and
Klaipeda from Jan 1 to the DAG execution date. Computes YTD anomalies,
city rankings, monthly z-scores, and per-city charts. A quality gate rejects
runs with sparse coverage or extreme z-scores.

### `vilnius_march_temperature_anomalies`
Extracts the March 1–N slice from 30 years of Vilnius ERA5 data and computes a
year-by-year temperature anomaly with z-scores against the full-window baseline.
Produces a longitudinal trend chart showing whether each March was warmer or
cooler than average — useful for seasonal risk assessments.

---

## Quick start

### 1 — Python environment

```bash
cd ml
uv sync                  # creates .venv and installs all deps
uv run pytest            # 17 tests — should all pass
```

---

### 2 — Airflow (local standalone)

Run in its own terminal. Keep it running while you trigger DAGs.

```bash
cd ml/airflow

export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export ML_PROJECT_ROOT="$PWD/.."
export TRAIN_PYTHON_BIN="$PWD/../.venv/bin/python"

env -u VIRTUAL_ENV uv run airflow standalone
```

Open **http://localhost:8080** — username `admin`, password printed in the terminal on first run (check `.airflow/standalone_admin_password.txt`).

To trigger a DAG manually (in a second terminal):

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

Available DAG ids:
- `mlflow_torch_training`
- `lithuania_weather_analysis`
- `vilnius_march_temperature_anomalies`

---

### 3 — Dashboard (Chart.js / Vite)

Run in its own terminal. Re-run the export step any time a DAG completes.

```bash
cd ml

# Pull latest pipeline outputs into the dashboard JSON
env -u VIRTUAL_ENV uv run python python/export_frontend_data.py

# Start the dev server
npm run dev
```

Open **http://localhost:5173**

To build a static bundle for deployment:

```bash
npm run build    # output in dist/
```

---

### Airflow (Docker — production setup)

Requires Docker Desktop running.

```bash
cd ml/airflow

docker compose up airflow-init   # first time only — migrates DB + creates admin
docker compose up -d             # starts webserver + scheduler + postgres
docker compose down              # stop
docker compose down -v           # stop + wipe postgres volume
```

Open **http://localhost:8080** · username `admin` · password `admin`.

---

## Data accuracy note

ERA5 is reanalysis data — it blends station observations, satellite radiances,
and radar via a physical model on a 0.25° grid (~25 km). It is consistent across
decades (unlike station networks that change over time) and appropriate for
anomaly comparison, but is not a substitute for point-station observations.
For publication-quality climatology, cross-validate against the Lithuanian
Hydrometeorological Service station records.

---

## Project layout

```
ml/
├── python/
│   ├── vilnius_march_fetch.py       # fetch 30yr Vilnius March ERA5 data
│   ├── vilnius_march_analyze.py     # compute year-by-year anomalies
│   ├── vilnius_march_plot.py        # render anomaly bar chart
│   ├── vilnius_march_quality_gate.py
│   ├── weather_fetch.py             # Lithuania YTD fetch
│   ├── weather_analyze.py           # city/country anomaly analysis
│   ├── weather_plot.py              # per-city + country charts
│   ├── weather_quality_gate.py
│   ├── weather_common.py            # shared fetch + retry logic
│   ├── export_frontend_data.py      # bridge: pipeline outputs → dashboard JSON
│   ├── train.py / evaluate.py       # PyTorch + MLflow training
│   └── tests/                       # pytest smoke tests (17 tests)
├── airflow/
│   ├── dags/                        # three production DAGs
│   └── docker-compose.yml           # Airflow + PostgreSQL stack
└── src/                             # Vite frontend
    ├── main.js                      # Chart.js dashboard
    ├── styles.css
    └── data/
        └── dashboard.json           # generated by export_frontend_data.py
```


> If you prefer Netlify/Vercel, just point the deploy target to the `dist/` folder.

---

## 🧪 CI / CD

This repo includes a GitHub Actions workflow (`.github/workflows/ci.yml`) that runs a build on every push and pull request.

If you want to auto-deploy on push, you can add an action for `gh-pages` or use your preferred host’s deploy workflow.
