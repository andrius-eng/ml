FROM apache/airflow:3.1.8

USER root

# Runtime deps used by scripts/healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    libgomp1 \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies needed by DAG-invoked scripts.
# Use CPU-only torch wheels in Linux containers to avoid pulling CUDA stacks.
COPY python/requirements-airflow-runtime.txt /tmp/requirements-airflow-runtime.txt
COPY python/requirements-llm-train.txt /tmp/requirements-llm-train.txt
RUN pip install \
    --no-cache-dir \
    --retries 10 \
    --timeout 120 \
    --extra-index-url https://download.pytorch.org/whl/cpu \
    -r /tmp/requirements-airflow-runtime.txt \
    -r /tmp/requirements-llm-train.txt

# Bake DAGs and project code into the image.
COPY airflow/dags /opt/airflow/dags
COPY python /opt/airflow/project/python

USER root
RUN mkdir -p /opt/airflow/logs && \
    chmod -R 775 /opt/airflow /opt/airflow/dags /opt/airflow/project /opt/airflow/logs && \
    chown -R airflow: /opt/airflow
USER airflow

# Project code and DAGs are mounted at runtime via docker-compose volumes.
# Keeping the image lean avoids long COPY phases on macOS file sharing.
