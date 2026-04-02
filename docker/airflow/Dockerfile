FROM apache/airflow:3.1.8

USER root

# Runtime deps used by scripts/healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    libgomp1 \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV AIRFLOW_PYTHON=/home/airflow/.local/bin/python

# Install uv for fast, reliable dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install Python dependencies needed by DAG-invoked scripts.
# Use CPU-only torch wheels in Linux containers to avoid pulling CUDA stacks.
COPY python/requirements-airflow-runtime.txt /tmp/requirements-airflow-runtime.txt
COPY python/requirements-llm-train.txt /tmp/requirements-llm-train.txt
RUN uv pip install --python "${AIRFLOW_PYTHON}" --no-cache \
    --index-strategy unsafe-best-match \
    --extra-index-url https://download.pytorch.org/whl/cpu \
    -r /tmp/requirements-airflow-runtime.txt \
    -r /tmp/requirements-llm-train.txt

# Bake DAGs and project code into the image.
COPY airflow/dags /opt/airflow/dags
COPY python /opt/airflow/project/python

RUN mkdir -p /opt/airflow/logs && \
    chmod -R 775 /opt/airflow /opt/airflow/dags /opt/airflow/project /opt/airflow/logs && \
    chown -R airflow: /opt/airflow /home/airflow/.local
USER airflow

RUN "${AIRFLOW_PYTHON}" - <<'PY'
required = [
    "apache_beam",
    "mlflow",
    "torch",
    "datasets",
    "transformers",
    "peft",
    "accelerate",
]
missing = []
for name in required:
    try:
        __import__(name)
    except Exception as exc:
        missing.append(f"{name}: {exc}")

if missing:
    raise SystemExit("Missing required Airflow image modules:\n" + "\n".join(missing))

print("Validated Airflow image modules:", ", ".join(required))
PY

# Project code and DAGs are mounted at runtime via docker-compose volumes.
# Keeping the image lean avoids long COPY phases on macOS file sharing.
