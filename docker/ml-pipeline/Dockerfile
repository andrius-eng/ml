# ML pipeline image — runs weather fetch, analysis, and export scripts.
# Also used by Airflow tasks (mounted as /opt/airflow/project).
FROM python:3.11-slim

WORKDIR /app

# System deps for matplotlib and pandas
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps separately for better layer caching
COPY python/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy project source
COPY python/ /app/python/
COPY src/data/ /app/src/data/

# Default: run export script to refresh dashboard.json from pipeline outputs
CMD ["python", "python/export_frontend_data.py"]
