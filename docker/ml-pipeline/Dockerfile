# ML pipeline image — runs weather fetch, analysis, and export scripts.
# Also used by Airflow tasks (mounted as /opt/airflow/project).
FROM python:3.11-slim

WORKDIR /app

# System deps for matplotlib and pandas
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    libgomp1 \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install uv for fast, reliable dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install CPU-oriented runtime deps (no CUDA/NVIDIA stack) for faster builds
COPY python/requirements-airflow-runtime.txt /app/requirements-runtime.txt
RUN uv pip install --system --no-cache \
    --extra-index-url https://download.pytorch.org/whl/cpu \
    -r requirements-runtime.txt

# Copy project source
COPY python/ /app/python/
COPY src/data/ /app/src/data/

# Default: run export script to refresh dashboard.json from pipeline outputs
CMD ["python", "python/export_frontend_data.py"]
