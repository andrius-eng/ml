# Dashboard WebSocket Server

## Overview

The WebSocket server (`dashboard-ws.js`) enables **live dashboard updates**. It periodically refreshes dashboard data from your Airflow workflows and pushes updates to connected clients in real-time.

## Features

- **Hourly polling**: Refreshes dashboard data every 60 minutes using `export_frontend_data.py`
- **Real-time push**: Broadcasts updates to all connected browser clients via WebSocket
- **Auto-reconnect**: Browsers automatically reconnect if the server goes down
- **Visual indicator**: Frontend displays "🔄 live" badge when connected

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ WebSocket Server (ws://localhost:3000)                          │
├─────────────────────────────────────────────────────────────────┤
│ • Listens for client connections (Vite frontend)               │
│ • Polls every 60 minutes                                        │
│ • Runs: python/export_frontend_data.py                          │
│ • Broadcasts update message to all connected clients            │
└─────────────────────────────────────────────────────────────────┘
         ▲                                      │
         │ (JSON update)                       │
         │                                      ▼
    ┌─────────────────────────────────────────────────────────────┐
    │ Frontend (http://localhost:5173)                             │
    ├─────────────────────────────────────────────────────────────┤
    │ • Connects to WS on page load                               │
    │ • Listens for 'dashboard_update' messages                   │
    │ • Re-fetches /data/dashboard.json on update                 │
    │ • Re-renders charts with fresh data                         │
    └─────────────────────────────────────────────────────────────┘
```

## Starting the Server

The server runs as a standalone Node.js process:

```bash
cd /Users/andrius/Development/ml
node server/dashboard-ws.js &
```

Or via npm:

```bash
cd /Users/andrius/Development/ml/server
npm start &
```

## Configuration

Edit `dashboard-ws.js` to adjust:

- **WS_PORT**: WebSocket server port (default: `3000`)
- **POLL_INTERVAL_MS**: Refresh interval (default: `3600000` = 60 minutes)

## Environment Requirements

- Node.js 18+
- Python environment with dependencies installed
- Airflow + ML pipeline running

## Integration with Startup Scripts

To start all components together (Airflow, Vite, WebSocket):

```bash
# In ml/ directory
./airflow/.venv/bin/airflow standalone &
npm run dev &
node server/dashboard-ws.js &
```

All three services will now run in parallel and the frontend will auto-update as data changes.

## Troubleshooting

- **Connection refused (port 3000)**: Check if another process is using port 3000
  ```bash
  lsof -i :3000
  ```

- **Dashboard not updating**: Check WebSocket server console output and ensure `export_frontend_data.py` completes successfully

- **"Dashboard WS unavailable" warning in browser**: This is OK for development; the dashboard will still work statically

## Logs

Check the WebSocket server output:

```bash
tail -f <terminal_output> # Check the running process logs
```

Example output on startup:
```
[Dashboard WS] Server listening on ws://localhost:3000
[Dashboard WS] Starting hourly polling (interval: 60 min)
[Dashboard WS] Refreshing dashboard data...
[Dashboard WS] Export successful
```

## Memory Usage

The server is lightweight (~50MB on startup). Each client connection uses minimal memory (~1MB).
