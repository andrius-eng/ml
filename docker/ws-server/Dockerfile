# WebSocket live-update server
FROM node:22-alpine

WORKDIR /app

# Runtime dependencies for export_frontend_data.py when host venv is unavailable.
# uv bypasses Alpine's PEP 668 externally-managed restriction cleanly.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
RUN apk add --no-cache python3 py3-pandas \
	&& uv pip install --system --break-system-packages --no-cache 'qdrant-client>=1.9.0'

# Install dependencies first for layer caching
COPY server/package.json server/package-lock.json ./
RUN npm ci --omit=dev

# Copy server source
COPY server/dashboard-ws.js ./

# Bake the export script and data path into the image.
COPY python/ /project/python/
COPY src/data/ /project/src/data/

# The server re-exports dashboard.json by running export_frontend_data.py.
# Project scripts are baked into /project; only python/output and src/data are
# expected to be mounted as named volumes in Swarm.
ENV PROJECT_ROOT=/project

EXPOSE 3000

CMD ["node", "dashboard-ws.js"]
