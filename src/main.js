import './styles.css';
import { Chart, registerables } from 'chart.js';
import data from './data/dashboard.json';

Chart.register(...registerables);

// ── helpers ──────────────────────────────────────────────────────────────────

function sign(n) {
  return n >= 0 ? '+' : '';
}

function zLabel(z) {
  const abs = Math.abs(z);
  if (abs < 0.5) return 'near normal';
  if (abs < 1.0) return 'slightly anomalous';
  if (abs < 1.5) return 'anomalous';
  if (abs < 2.0) return 'very anomalous';
  return 'extreme';
}

function kpiCard({ label, value, sub, highlight }) {
  const el = document.createElement('div');
  el.className = 'kpi-card' + (highlight ? ' kpi-card--highlight' : '');
  el.innerHTML = `
    <div class="kpi-label">${label}</div>
    <div class="kpi-value">${value}</div>
    ${sub ? `<div class="kpi-sub">${sub}</div>` : ''}
  `;
  return el;
}

// ── sections ─────────────────────────────────────────────────────────────────

function renderHeader(d) {
  document.getElementById('generated-badge').textContent =
    'Updated ' + d.generated_at;
}

function renderKPIs(d) {
  const row = document.getElementById('kpi-row');
  const m = d.vilnius_march;
  const w = d.lithuania_weather;

  const items = [
    {
      label: 'Vilnius March 2026 anomaly',
      value: sign(m.latest_year.anomaly_c) + m.latest_year.anomaly_c.toFixed(1) + ' °C',
      sub: `z = ${sign(m.latest_year.zscore)}${m.latest_year.zscore.toFixed(2)} · ${zLabel(m.latest_year.zscore)}`,
      highlight: true,
    },
    {
      label: '30-yr March baseline (1997–2026)',
      value: sign(m.baseline.mean_temp_c) + m.baseline.mean_temp_c.toFixed(2) + ' °C',
      sub: `σ = ${m.baseline.std_temp_c.toFixed(2)} °C`,
    },
    {
      label: 'Lithuania YTD temp anomaly',
      value: sign(w.temp_anomaly_c) + w.temp_anomaly_c.toFixed(1) + ' °C',
      sub: `z = ${w.temp_zscore.toFixed(2)} · vs 1991–2020`,
    },
    {
      label: 'Lithuania last-7d temp signal',
      value: sign(w.latest_7d_temp_anomaly) + w.latest_7d_temp_anomaly.toFixed(1) + ' °C',
      sub: `YTD precip anomaly ${sign(w.precip_anomaly_mm)}${w.precip_anomaly_mm.toFixed(0)} mm`,
    },
  ];

  items.forEach((item) => row.appendChild(kpiCard(item)));
}

function renderMarchChart(d) {
  const m = d.vilnius_march;
  const currentYear = m.latest_year.year;

  document.getElementById('cutoff-day').textContent = m.window.cutoff_day;

  const labels = m.annual.map((r) => r.year);
  const values = m.annual.map((r) => r.anomaly_c);

  const bgColors = values.map((v, i) => {
    if (m.annual[i].year === currentYear) return v >= 0 ? '#ff6b35' : '#4895ef';
    return v >= 0 ? 'rgba(239,68,68,0.65)' : 'rgba(59,130,246,0.65)';
  });

  const borderColors = values.map((_v, i) =>
    m.annual[i].year === currentYear ? '#ffffff' : 'transparent'
  );
  const borderWidths = values.map((_v, i) =>
    m.annual[i].year === currentYear ? 2 : 0
  );

  new Chart(document.getElementById('marchChart'), {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label: 'March anomaly (°C)',
          data: values,
          backgroundColor: bgColors,
          borderColor: borderColors,
          borderWidth: borderWidths,
          borderRadius: 3,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const row = m.annual[ctx.dataIndex];
              return [
                `Anomaly: ${sign(row.anomaly_c)}${row.anomaly_c.toFixed(2)} °C`,
                `Mean: ${sign(row.mean_temp_c)}${row.mean_temp_c.toFixed(2)} °C`,
                `z-score: ${sign(row.zscore)}${row.zscore.toFixed(2)}`,
              ];
            },
          },
        },
      },
      scales: {
        x: {
          grid: { color: 'rgba(255,255,255,0.06)' },
          ticks: { color: 'rgba(255,255,255,0.6)', maxRotation: 45 },
        },
        y: {
          grid: { color: 'rgba(255,255,255,0.08)' },
          ticks: {
            color: 'rgba(255,255,255,0.6)',
            callback: (v) => sign(v) + v + ' °C',
          },
          title: {
            display: true,
            text: 'Temperature anomaly (°C)',
            color: 'rgba(255,255,255,0.45)',
          },
        },
      },
    },
  });
}

function renderCityCharts(d) {
  const w = d.lithuania_weather;
  document.getElementById('ytd-period').textContent = w.period;

  const cities = w.city_rankings.temperature.map((r) => r.city);
  const tempZ = w.city_rankings.temperature.map((r) => r.z_score);
  const precipZ = w.city_rankings.precipitation.map((r) => {
    const match = w.city_rankings.precipitation.find((p) => p.city === r.city);
    return match ? match.z_score : 0;
  });

  // order precipitation by the same city order as temperature
  const precipZOrdered = cities.map((city) => {
    const match = w.city_rankings.precipitation.find((p) => p.city === city);
    return match ? match.z_score : 0;
  });

  const barColor = (zArr) =>
    zArr.map((z) => (z >= 0 ? 'rgba(239,68,68,0.7)' : 'rgba(59,130,246,0.7)'));

  const baseScaleOpts = (label) => ({
    x: {
      grid: { color: 'rgba(255,255,255,0.06)' },
      ticks: { color: 'rgba(255,255,255,0.6)' },
    },
    y: {
      grid: { color: 'rgba(255,255,255,0.08)' },
      ticks: { color: 'rgba(255,255,255,0.6)' },
      title: { display: true, text: label, color: 'rgba(255,255,255,0.45)' },
    },
  });

  new Chart(document.getElementById('cityTempChart'), {
    type: 'bar',
    data: {
      labels: cities,
      datasets: [{
        label: 'Temperature z-score',
        data: tempZ,
        backgroundColor: barColor(tempZ),
        borderRadius: 4,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false }, title: { display: true, text: 'YTD Temperature z-score', color: '#f7f7f7' } },
      scales: baseScaleOpts('z-score vs 1991–2020'),
    },
  });

  new Chart(document.getElementById('cityPrecipChart'), {
    type: 'bar',
    data: {
      labels: cities,
      datasets: [{
        label: 'Precipitation z-score',
        data: precipZOrdered,
        backgroundColor: barColor(precipZOrdered),
        borderRadius: 4,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false }, title: { display: true, text: 'YTD Precipitation z-score', color: '#f7f7f7' } },
      scales: baseScaleOpts('z-score vs 1991–2020'),
    },
  });
}

function renderMLMetrics(d) {
  const row = document.getElementById('ml-kpi-row');
  const ml = d.ml_model;

  [
    { label: 'R² (test set)', value: ml.r2.toFixed(4), sub: 'Variance explained' },
    { label: 'RMSE', value: ml.rmse.toFixed(4), sub: 'Root mean squared error' },
    { label: 'MAE', value: ml.mae.toFixed(4), sub: 'Mean absolute error' },
  ].forEach((item) => row.appendChild(kpiCard(item)));
}

function renderPipeline() {
  const grid = document.getElementById('pipeline-grid');
  const dags = [
    {
      name: 'mlflow_torch_training',
      desc: 'Trains and evaluates a PyTorch regression model, logging params + artifacts to MLflow.',
      steps: ['generate_data', 'train_model', 'evaluate_model'],
      tags: ['PyTorch', 'MLflow', 'Regression'],
    },
    {
      name: 'lithuania_weather_analysis',
      desc: 'Fetches ERA5 daily weather for 3 Lithuanian cities, computes YTD anomalies, city rankings, per-city charts, and validates output quality.',
      steps: ['fetch_weather', 'analyze_anomalies', 'plot_charts', 'quality_gate'],
      tags: ['ERA5', 'Anomaly detection', 'Vega/Matplotlib'],
    },
    {
      name: 'vilnius_march_temperature_anomalies',
      desc: 'Historical 30-year deep-dive: extracts every March 1–N slice, computes year-by-year temperature anomaly and z-score, generates a longitudinal trend chart.',
      steps: ['fetch_vilnius_march', 'analyze_anomalies', 'plot_anomalies + quality_gate'],
      tags: ['ERA5', 'Climate trend', '30-year baseline'],
    },
  ];

  dags.forEach((dag) => {
    const card = document.createElement('div');
    card.className = 'pipeline-card';
    card.innerHTML = `
      <h3 class="pipeline-name">${dag.name}</h3>
      <p class="pipeline-desc">${dag.desc}</p>
      <div class="pipeline-steps">
        ${dag.steps.map((s, i) => `
          ${i > 0 ? '<span class="step-arrow">&rarr;</span>' : ''}
          <span class="step">${s}</span>
        `).join('')}
      </div>
      <p class="tags">${dag.tags.map((t) => `<span>${t}</span>`).join('')}</p>
    `;
    grid.appendChild(card);
  });
}

// ── live updates via WebSocket ──────────────────────────────────────────────

function connectWebSocket() {
  const wsProtocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
  const token = import.meta.env.VITE_DASHBOARD_WS_TOKEN;
  const qs = token ? `?token=${encodeURIComponent(token)}` : '';
  const wsUrl = `${wsProtocol}://${window.location.hostname}:3000${qs}`;
  console.log(`[Dashboard] Connecting to ${wsUrl}...`);

  const ws = new WebSocket(wsUrl);

  ws.addEventListener('open', () => {
    console.log('[Dashboard] WebSocket connected');
    document.body.classList.add('ws-connected');
  });

  ws.addEventListener('message', async (event) => {
    const msg = JSON.parse(event.data);
    console.log(`[Dashboard] Update received:`, msg);

    try {
      // Re-fetch the latest dashboard data
      const response = await fetch('/data/dashboard.json');
      const newData = await response.json();

      // Re-render with new data
      renderHeader(newData);
      renderKPIs(newData);
      renderMarchChart(newData);
      renderCityCharts(newData);
      renderMLMetrics(newData);

      // Flash update indicator
      const badge = document.getElementById('generated-badge');
      badge.style.transition = 'all 0.3s ease';
      badge.style.backgroundColor = '#4CAF50';
      badge.style.color = '#fff';
      setTimeout(() => {
        badge.style.backgroundColor = '';
        badge.style.color = '';
      }, 2000);

      console.log('[Dashboard] UI refreshed with new data');
    } catch (err) {
      console.error('[Dashboard] Failed to refresh UI:', err);
    }
  });

  ws.addEventListener('close', () => {
    console.log('[Dashboard] WebSocket disconnected, reconnecting in 5s...');
    document.body.classList.remove('ws-connected');
    setTimeout(connectWebSocket, 5000);
  });

  ws.addEventListener('error', (err) => {
    console.error('[Dashboard] WebSocket error:', err);
  });
}

// ── boot ─────────────────────────────────────────────────────────────────────

function init() {
  renderHeader(data);
  renderKPIs(data);
  renderMarchChart(data);
  renderCityCharts(data);
  renderMLMetrics(data);
  renderPipeline();

  // Attempt to connect to live update server
  try {
    connectWebSocket();
  } catch (err) {
    console.warn('[Dashboard] WebSocket unavailable, dashboard will be static:', err.message);
  }
}

init();
