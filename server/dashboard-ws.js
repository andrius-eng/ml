/**
 * WebSocket server for live dashboard updates.
 * Polls Airflow DB and re-exports dashboard data every hour,
 * then broadcasts updates to connected clients.
 */

import { WebSocketServer } from 'ws';
import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
// In Docker, PROJECT_ROOT is set to the mounted project path.
// In local dev it falls back to the parent of the server/ directory.
const projectRoot = process.env.PROJECT_ROOT || path.resolve(__dirname, '..');

const WS_HOST = '127.0.0.1';
const WS_PORT = 3000;
const POLL_INTERVAL_MS = 60 * 60 * 1000; // 1 hour
const RATE_WINDOW_MS = 10_000;
const MAX_CONNECTIONS_PER_WINDOW = 5;
const WS_TOKEN = process.env.DASHBOARD_WS_TOKEN || '';

const allowedOrigins = new Set([
  'http://localhost:5173',
  'http://127.0.0.1:5173',
]);

const wss = new WebSocketServer({ host: WS_HOST, port: WS_PORT });

console.log(`[Dashboard WS] Server listening on ws://${WS_HOST}:${WS_PORT}`);
if (WS_TOKEN) {
  console.log('[Dashboard WS] Token auth enabled');
}

// Track connected clients
const clients = new Set();
const connectionAttemptsByIp = new Map();

wss.on('connection', (ws, req) => {
  const now = Date.now();
  const ip = req.socket.remoteAddress || 'unknown';
  const origin = req.headers.origin || 'unknown';

  if (origin !== 'unknown' && !allowedOrigins.has(origin)) {
    console.warn(`[Dashboard WS] Rejected client from origin=${origin} ip=${ip}`);
    ws.close(1008, 'Origin not allowed');
    return;
  }

  const recentAttempts = (connectionAttemptsByIp.get(ip) || []).filter(
    (ts) => now - ts <= RATE_WINDOW_MS
  );
  recentAttempts.push(now);
  connectionAttemptsByIp.set(ip, recentAttempts);

  if (recentAttempts.length > MAX_CONNECTIONS_PER_WINDOW) {
    console.warn(`[Dashboard WS] Rate limit exceeded for ip=${ip}`);
    ws.close(1008, 'Rate limit exceeded');
    return;
  }

  if (WS_TOKEN) {
    const host = req.headers.host || `${WS_HOST}:${WS_PORT}`;
    const reqUrl = req.url || '/';
    const parsedUrl = new URL(reqUrl, `http://${host}`);
    const token = parsedUrl.searchParams.get('token') || '';
    if (token !== WS_TOKEN) {
      console.warn(`[Dashboard WS] Unauthorized client ip=${ip}`);
      ws.close(1008, 'Unauthorized');
      return;
    }
  }

  clients.add(ws);
  console.log(`[Dashboard WS] Client connected ip=${ip} origin=${origin}. Total: ${clients.size}`);

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`[Dashboard WS] Client disconnected ip=${ip}. Total: ${clients.size}`);
  });

  ws.on('error', (err) => {
    console.error(`[Dashboard WS] Client error:`, err.message);
  });
});

/**
 * Re-export dashboard data by running export_frontend_data.py
 */
async function refreshDashboard() {
  console.log(`[Dashboard WS] Refreshing dashboard data...`);

  return new Promise((resolve, reject) => {
    const pythonEnv = path.join(projectRoot, '.venv', 'bin', 'activate');
    const scriptPath = path.join(projectRoot, 'python', 'export_frontend_data.py');

    // Shell script that activates venv and runs export
    const shell = `source "${pythonEnv}" && python "${scriptPath}"`;

    const proc = spawn('/bin/bash', ['-c', shell], {
      cwd: projectRoot,
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';

    proc.stdout.on('data', (data) => {
      stdout += data.toString();
    });

    proc.stderr.on('data', (data) => {
      stderr += data.toString();
    });

    proc.on('close', (code) => {
      if (code === 0) {
        console.log(`[Dashboard WS] Export successful`);
        console.log(stdout.trim());
        resolve(true);
      } else {
        console.error(`[Dashboard WS] Export failed (code ${code})`);
        console.error(stderr);
        reject(new Error(`Export failed: ${stderr}`));
      }
    });
  });
}

/**
 * Broadcast update to all connected clients
 */
function broadcastUpdate(data) {
  const message = JSON.stringify({
    type: 'dashboard_update',
    timestamp: new Date().toISOString(),
    ...data,
  });

  clients.forEach((client) => {
    if (client.readyState === 1) { // WebSocket.OPEN = 1
      client.send(message);
    }
  });
}

/**
 * Main polling loop
 */
async function startPolling() {
  console.log(`[Dashboard WS] Starting hourly polling (interval: ${POLL_INTERVAL_MS / 1000 / 60} min)`);

  // Run once immediately on startup
  try {
    await refreshDashboard();
    broadcastUpdate({ status: 'ready', message: 'Dashboard updated on startup' });
  } catch (err) {
    console.error(`[Dashboard WS] Initial export failed:`, err.message);
  }

  // Then poll on interval
  setInterval(async () => {
    try {
      await refreshDashboard();
      broadcastUpdate({ status: 'updated', message: 'Dashboard refreshed' });
    } catch (err) {
      console.error(`[Dashboard WS] Polling failed:`, err.message);
      broadcastUpdate({ status: 'error', message: `Refresh failed: ${err.message}` });
    }
  }, POLL_INTERVAL_MS);
}

startPolling().catch(console.error);

// Graceful shutdown
process.on('SIGINT', () => {
  console.log(`[Dashboard WS] Shutting down...`);
  wss.close(() => {
    console.log(`[Dashboard WS] Closed`);
    process.exit(0);
  });
});
