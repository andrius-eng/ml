# Python ML + Airflow (torch + mlflow)

This folder contains a minimal example of how to run a PyTorch training job and log results to MLflow, plus an Airflow DAG that can trigger the job.

## ✅ What you get

- `python/train.py`: train a tiny linear model on synthetic data
- `python/requirements.txt`: dependencies for training + MLflow
- `python/requirements-airflow.txt`: optional Airflow stack for orchestration
- `airflow/dags/train_dag.py`: Airflow DAG that runs the training script

## Quick run (local)

This repository now uses `uv` project management with two separate Python environments:

- `ml/.venv` for training, evaluation, plotting, and serving
- `ml/airflow/.venv` for the Airflow CLI and providers

### Training environment

```bash
cd ml
uv sync
source .venv/bin/activate
python python/train.py --epochs 3 --lr 0.01
```

### Equivalent one-off run without activating the shell

```bash
cd ml
uv run python python/train.py --epochs 3 --lr 0.01
```

Run a quick health check with:

```bash
python python/train.py --dry-run
```

This verifies the environment can import `torch` and `mlflow` without running a full training loop.

## 🧩 Included example scripts

- `python/check_python.py` — verify you’re running Python 3.11+ (fails fast otherwise)
- `python/data.py` — generate synthetic regression data to `python/data.csv`
- `python/train.py` — train a small PyTorch model (writes `python/output/model.pth` and metrics CSV)
- `python/evaluate.py` — load a saved model and write `mse`, `rmse`, `mae`, `r2`, and residual stats
- `python/plot.py` — render a training-loss plot to `python/output/training_mse.png`
- `python/diagnostics.py` — render prediction-vs-actual and residual distribution plots
- `python/quality_gate.py` — fail the pipeline if evaluation thresholds are missed
- `python/weather_fetch.py` — fetch historical daily weather data for Lithuania proxy cities
- `python/weather_analyze.py` — build climatology-based daily, monthly, rolling, and city-level anomalies plus a markdown report
- `python/weather_plot.py` — render overview charts and one separate plot per city
- `python/weather_quality_gate.py` — validate output coverage and monthly anomaly thresholds
- `python/vilnius_march_fetch.py` — fetch Vilnius daily weather for the last 30 March comparison window
- `python/vilnius_march_analyze.py` — compute a March temperature anomaly for each year in the window
- `python/vilnius_march_plot.py` — render a year-by-year Vilnius March anomaly chart
- `python/vilnius_march_quality_gate.py` — validate that the March comparison window is complete enough to trust
- `python/serve.py` — FastAPI app exposing a `/predict` endpoint based on the saved model
- `python/run_all.py` — run the full pipeline end-to-end (generate data, train, evaluate)

### Example workflow (end-to-end)

```bash
cd ml
uv sync
uv run python python/run_all.py
```

This generates data, trains a model, saves the model to `python/output/model.pth`, and prints an evaluation score.

## Optional: Airflow setup

This repository includes a minimal DAG that calls `python/train.py` and writes MLflow artifacts locally.

### Local Airflow CLI with a dedicated uv environment

Use a separate environment for Airflow so its dependency constraints do not fight the training stack.

```bash
cd ml/airflow
uv sync
AIRFLOW_HOME="$PWD/.airflow" \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
../.venv/bin/python -c "import torch, mlflow; print('training env ok')"
```

Run the DAG task locally through the Airflow CLI:

```bash
cd ml/airflow
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export ML_PROJECT_ROOT="$PWD/.."
export TRAIN_PYTHON_BIN="$PWD/../.venv/bin/python"

uv run airflow db migrate
uv run airflow tasks test mlflow_torch_training run_training 2025-01-01
```

The full DAG now exposes these analysis steps in Airflow:

- generate synthetic data
- train the model and log MLflow metrics
- evaluate the model and save `evaluation.json`
- render the training curve and diagnostic plots
- apply a quality gate on `mse` and `r2`

There is also a separate Airflow DAG for Lithuania weather analysis:

- fetch historical daily weather data for Vilnius, Kaunas, and Klaipeda
- build a country-level proxy series
- compare current year-to-date temperature and precipitation with the 1991-2020 baseline
- save rolling daily anomalies, monthly anomaly tables, city rankings, and a markdown report
- render anomaly and seasonality charts plus separate per-city plots
- validate data coverage and monthly anomaly thresholds

The `vilnius_march_temperature_anomalies` DAG will:

- fetch Vilnius daily temperatures for the last 30-year March window
- align every year to the same March day cutoff as the Airflow execution date
- compute a March mean-temperature anomaly for each year in the window
- save a CSV table, JSON summary, markdown report, and anomaly chart

### Run with the official Airflow Docker image

The DAG expects the full project to be available inside the Airflow container so it can access `python/train.py` and write outputs under the repository root.

```bash
cd ml
docker run --rm \
  -e ML_PROJECT_ROOT=/opt/airflow/project \
  -e TRAIN_PYTHON_BIN=/opt/airflow/project/.venv/bin/python \
  -v "$PWD":/opt/airflow/project \
  -v "$PWD/airflow/dags":/opt/airflow/dags \
  -p 8080:8080 \
  apache/airflow:2.10.3 standalone
```

Then open http://localhost:8080 and trigger the `mlflow_torch_training` DAG.

> The official Airflow image does not include `torch` or `mlflow`. Build a custom image that installs `python/requirements-airflow.txt`, and keep training dependencies in a separate environment or image layer.

### Legacy requirements files

`python/requirements.txt` and `python/requirements-airflow.txt` are retained as compatibility exports. The `pyproject.toml` files are now the source of truth.

## Notes

- MLflow artifacts are stored locally in `mlruns/` by default.
- This is meant as a minimal example; adapt it to your own dataset, model, and tracking setup.
