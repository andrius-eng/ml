import { useState, useEffect, useRef, useCallback } from "react";

const WS_MAX_RECONNECT_DELAY = 60000;

async function fetchDashboardData() {
  for (const url of ["/data/dashboard.json", "/api/dashboard"]) {
    try {
      const res = await fetch(url);
      if (res.ok) return await res.json();
    } catch (_) {
      // try next
    }
  }
  try {
    const mod = await import("../data/dashboard.json");
    return mod.default;
  } catch (_) {
    return null;
  }
}

export function useDashboardData() {
  const [data, setData] = useState(null);
  const [wsConnected, setWsConnected] = useState(false);
  const wsReconnectDelay = useRef(1000);
  const wsRef = useRef(null);
  const reconnectTimer = useRef(null);

  const loadData = useCallback(async () => {
    const d = await fetchDashboardData();
    if (d) setData(d);
  }, []);

  const connect = useCallback(() => {
    const wsProtocol = window.location.protocol === "https:" ? "wss" : "ws";
    const token = import.meta.env.VITE_DASHBOARD_WS_TOKEN;
    const qs = token ? `?token=${encodeURIComponent(token)}` : "";
    const wsUrl = `${wsProtocol}://${window.location.hostname}:3000${qs}`;

    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;

    ws.addEventListener("open", () => {
      setWsConnected(true);
      wsReconnectDelay.current = 1000;
    });

    ws.addEventListener("message", async () => {
      await loadData();
    });

    ws.addEventListener("close", () => {
      setWsConnected(false);
      reconnectTimer.current = setTimeout(
        () => connect(),
        wsReconnectDelay.current,
      );
      wsReconnectDelay.current = Math.min(
        wsReconnectDelay.current * 2,
        WS_MAX_RECONNECT_DELAY,
      );
    });

    ws.addEventListener("error", (err) => {
      console.error("[Dashboard] WebSocket error:", err);
    });
  }, [loadData]);

  useEffect(() => {
    loadData();
    try {
      connect();
    } catch (err) {
      console.warn("[Dashboard] WebSocket unavailable:", err.message);
    }
    return () => {
      if (wsRef.current) wsRef.current.close();
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
    };
  }, [loadData, connect]);

  return { data, wsConnected };
}
