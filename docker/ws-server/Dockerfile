# WebSocket live-update server
FROM node:22-alpine

WORKDIR /app

# Runtime dependency for export_frontend_data.py when host venv is unavailable.
RUN apk add --no-cache python3 py3-pandas

# Install dependencies first for layer caching
COPY server/package.json server/package-lock.json ./
RUN npm ci --omit=dev

# Copy server source
COPY server/dashboard-ws.js ./

# The server re-exports dashboard.json by running export_frontend_data.py.
# At runtime, mount the project root at /project so the venv + scripts exist.
ENV PROJECT_ROOT=/project

EXPOSE 3000

CMD ["node", "dashboard-ws.js"]
