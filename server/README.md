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
cd /Users/andrius/Development/ml
node server/dashboard-ws.js
```

Or from server directory:

```bash
cd /Users/andrius/Development/ml/server
npm start
```

## Related Services

- Frontend UI: http://localhost:5173
- WebSocket endpoint: ws://localhost:3000
- FastAPI RAG endpoint: http://127.0.0.1:8000/rag/query

FastAPI startup command used by docs and local verification:

```bash
cd /Users/andrius/Development/ml
uv run uvicorn --app-dir python serve:app --host 127.0.0.1 --port 8000
```

## Docker Compose Full Stack

The full compose setup wires:

- ws-server for websocket updates
- frontend nginx serving dashboard
- ml-server for FastAPI predict and rag query endpoints

Start stack:

```bash
cd /Users/andrius/Development/ml
docker compose -f airflow/docker-compose.yml -f docker-compose.full.yml up -d --build
```

## Troubleshooting

Connection refused on ws://localhost:3000:

```bash
lsof -nP -iTCP:3000 -sTCP:LISTEN
```

RAG endpoint import error:

- If you see Could not import module serve, start uvicorn with app-dir python.

Port already in use on 8000:

```bash
lsof -nP -iTCP:8000 -sTCP:LISTEN
kill -9 <PID>
```
