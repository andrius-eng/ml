# Dashboard WebSocket Server

## Overview

dashboard-ws.js pushes live dashboard refresh events to browsers and periodically
regenerates dashboard JSON from pipeline outputs.

It does not answer RAG questions directly. Live RAG question answering is served
by FastAPI at /rag/query.

## Responsibilities

- Keep src/data/dashboard.json fresh by running export_frontend_data.py
- Broadcast dashboard_update events to connected clients
- Enforce origin checks and connection rate limits

## Run Locally

From project root:

```bash
cd ml
node server/dashboard-ws.js
```

Or from server directory:

```bash
cd ml/server
npm start
```

## Related Services

- Frontend UI: http://localhost:5173
- WebSocket endpoint: ws://localhost:3000
- FastAPI RAG endpoint: http://127.0.0.1:8000/rag/query

FastAPI startup command used by docs and local verification:

```bash
cd ml
uv run uvicorn --app-dir python serve:app --host 127.0.0.1 --port 8000
```

## Kubernetes Deployment

In Kubernetes the `ws-server` runs as a `Deployment` (`kubernetes/base/dashboard.yaml`).
It mounts the `airflow-data` and `dashboard-data` PVCs in place of the Docker
Compose volume mounts. The WebSocket endpoint is exposed via the nginx Ingress at
`ws://ml-stack.local/ws`.

No code changes are required — the same `dashboard-ws.js` runs in the container.


## Docker Compose Full Stack

The full compose setup wires:

- ws-server for websocket updates
- frontend nginx serving dashboard
- ml-server for FastAPI predict and rag query endpoints


The frontend container proxies `/api/*` to `ml-server`. That proxy now uses
Docker DNS re-resolution so `ml-server` can restart without leaving nginx pinned
to a stale upstream container IP.

Start stack:

```bash
cd ml
docker compose -f airflow/docker-compose.yml -f docker-compose.full.yml up -d --build
```

## Troubleshooting

Connection refused on ws://localhost:3000:

```bash
lsof -nP -iTCP:3000 -sTCP:LISTEN
```

RAG endpoint import error:

- If you see Could not import module serve, start uvicorn with app-dir python.


RAG query returns 503 or the UI says the API is not running:

- Check `docker ps` for `ml-ml-server-1` and `ml-frontend-1`.
- The frontend proxy route is `/api/rag/query`, which forwards to FastAPI
  `/rag/query` after stripping the `/api` prefix.
- If you changed nginx config but did not rebuild the frontend image yet, reload
  nginx inside the running container.

Structured RAG queries still bypass vector search when appropriate:

- year-vs-year month comparisons are answered directly from anomaly CSVs
- warmest/coldest year questions are answered directly from Vilnius month CSVs
- year-month extreme questions are answered directly from `beam_summary.json`

Port already in use on 8000:

```bash
lsof -nP -iTCP:8000 -sTCP:LISTEN
kill -9 <PID>
```
