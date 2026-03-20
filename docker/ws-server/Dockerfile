# WebSocket live-update server
FROM node:22-alpine

WORKDIR /app

# Runtime dependencies for export_frontend_data.py when host venv is unavailable.
# Alpine marks system Python as externally managed (PEP 668), so pip needs
# --break-system-packages in this container image layer.
RUN apk add --no-cache python3 py3-pip py3-pandas \
	&& python3 -m pip install --no-cache-dir --break-system-packages 'qdrant-client>=1.9.0'

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
