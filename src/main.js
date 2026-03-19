import './styles.css';
import { Chart, registerables } from 'chart.js';
import data from './data/dashboard.json';

Chart.register(...registerables);

let marchChartInstance = null;
let cityTempChartInstance = null;
let cityPrecipChartInstance = null;

function formatSource(source) {
  return source.source || source.title;
}

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
  const labelEl = document.createElement('div');
  labelEl.className = 'kpi-label';
  labelEl.textContent = label;
  el.appendChild(labelEl);
  const valueEl = document.createElement('div');
  valueEl.className = 'kpi-value';
  valueEl.textContent = value;
  el.appendChild(valueEl);
  if (sub) {
    const subEl = document.createElement('div');
    subEl.className = 'kpi-sub';
    subEl.textContent = sub;
    el.appendChild(subEl);
  }
  return el;
}

// ── sections ─────────────────────────────────────────────────────────────────

function renderHeader(d) {
  document.getElementById('generated-badge').textContent =
    'Updated ' + d.generated_at;
}

function renderKPIs(d) {
  const row = document.getElementById('kpi-row');
  row.innerHTML = '';
  const m = d.vilnius_month_anomaly;
  const w = d.lithuania_weather;

  const items = [
    {
      label: `Vilnius ${m.month_name} ${m.latest_year.year} anomaly`,
      value: sign(m.latest_year.anomaly_c) + m.latest_year.anomaly_c.toFixed(1) + ' °C',
      sub: `z = ${sign(m.latest_year.zscore)}${m.latest_year.zscore.toFixed(2)} · ${zLabel(m.latest_year.zscore)}`,
      highlight: true,
    },
    {
      label: `30-yr ${m.month_name} baseline`,
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
  const m = d.vilnius_month_anomaly;
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

  if (marchChartInstance) marchChartInstance.destroy();
  marchChartInstance = new Chart(document.getElementById('marchChart'), {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label: `${m.month_name} anomaly (°C)`,
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

  if (cityTempChartInstance) cityTempChartInstance.destroy();
  cityTempChartInstance = new Chart(document.getElementById('cityTempChart'), {
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

  if (cityPrecipChartInstance) cityPrecipChartInstance.destroy();
  cityPrecipChartInstance = new Chart(document.getElementById('cityPrecipChart'), {
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
  row.innerHTML = '';
  const ml = d.ml_model;

  [
    { label: 'R² (test set)', value: ml.r2.toFixed(4), sub: 'Variance explained' },
    { label: 'RMSE', value: ml.rmse.toFixed(4), sub: 'Root mean squared error' },
    { label: 'MAE', value: ml.mae.toFixed(4), sub: 'Mean absolute error' },
  ].forEach((item) => row.appendChild(kpiCard(item)));
}

function renderRagDemo(d) {
  const meta = document.getElementById('rag-meta');
  const grid = document.getElementById('rag-grid');
  const rag = d.rag_demo;

  if (!rag || !Array.isArray(rag.questions) || rag.questions.length === 0) {
    meta.textContent = 'No retrieval briefings available yet.';
    grid.innerHTML = '';
    return;
  }

  meta.textContent = `Collection ${rag.collection} · ${rag.corpus_size} indexed documents · Updated ${rag.generated_at}`;
  grid.innerHTML = '';

  rag.questions.forEach((item) => {
    const card = document.createElement('article');
    card.className = 'rag-card';

    const question = document.createElement('h3');
    question.className = 'rag-question';
    question.textContent = item.question;
    card.appendChild(question);

    const answer = document.createElement('p');
    answer.className = 'rag-answer';
    answer.textContent = item.answer;
    card.appendChild(answer);

    const sources = document.createElement('div');
    sources.className = 'rag-sources';
    (item.sources || []).forEach((source) => {
      const chip = document.createElement('span');
      chip.textContent = `${formatSource(source)} (${source.score.toFixed(2)})`;
      sources.appendChild(chip);
    });
    card.appendChild(sources);

    grid.appendChild(card);
  });
}

function renderPipeline() {
  const grid = document.getElementById('pipeline-grid');
  grid.innerHTML = '';
  const dags = [
    {
      name: 'climate_temperature_model',
      desc: 'Trains a PyTorch MLP on real ERA5 Lithuania daily weather (1991–2022) to predict daily mean temperature. Evaluates R² and RMSE on held-out 2023+ data, logging to MLflow.',
      steps: ['prepare_data', 'train_model', 'evaluate_model', 'quality_gate', 'refresh_rag_context'],
      tags: ['PyTorch', 'MLflow', 'ERA5', 'Seasonality'],
    },
    {
      name: 'lithuania_weather_analysis',
      desc: 'Fetches ERA5 daily weather for 3 Lithuanian cities, computes YTD anomalies, city rankings, per-city charts, and validates output quality.',
      steps: ['fetch_weather', 'analyze_anomalies', 'plot_charts', 'quality_gate', 'refresh_rag_context'],
      tags: ['ERA5', 'Anomaly detection', 'Vega/Matplotlib'],
    },
    {
      name: 'vilnius_march_temperature_anomalies',
      name: `vilnius_${d.vilnius_month_anomaly ? d.vilnius_month_anomaly.month_name.toLowerCase() : 'march'}_temperature_anomalies`,
      desc: `Historical 30-year deep-dive: extracts every ${d.vilnius_month_anomaly ? d.vilnius_month_anomaly.month_name : 'March'} 1–N slice, computes year-by-year temperature anomaly and z-score, generates a longitudinal trend chart.`,
      steps: ['fetch_vilnius_march', 'analyze_anomalies', 'plot_anomalies', 'quality_gate', 'refresh_rag_context'],
      tags: ['ERA5', 'Climate trend', '30-year baseline'],
    },
  ];

  dags.forEach((dag) => {
    const card = document.createElement('div');
    card.className = 'pipeline-card';

    const h3 = document.createElement('h3');
    h3.className = 'pipeline-name';
    h3.textContent = dag.name;
    card.appendChild(h3);

    const desc = document.createElement('p');
    desc.className = 'pipeline-desc';
    desc.textContent = dag.desc;
    card.appendChild(desc);

    const stepsEl = document.createElement('div');
    stepsEl.className = 'pipeline-steps';
    dag.steps.forEach((s, i) => {
      if (i > 0) {
        const arrow = document.createElement('span');
        arrow.className = 'step-arrow';
        arrow.textContent = '\u2192';
        stepsEl.appendChild(arrow);
      }
      const step = document.createElement('span');
      step.className = 'step';
      step.textContent = s;
      stepsEl.appendChild(step);
    });
    card.appendChild(stepsEl);

    const tagsEl = document.createElement('p');
    tagsEl.className = 'tags';
    dag.tags.forEach((t) => {
      const tagSpan = document.createElement('span');
      tagSpan.textContent = t;
      tagsEl.appendChild(tagSpan);
    });
    card.appendChild(tagsEl);

    grid.appendChild(card);
  });
}

// ── live updates via WebSocket ──────────────────────────────────────────────

let wsReconnectDelay = 1000;
const WS_MAX_RECONNECT_DELAY = 60000;

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
    wsReconnectDelay = 1000;
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
      renderRagDemo(newData);
      renderPipeline();

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
    console.log(`[Dashboard] WebSocket disconnected, reconnecting in ${wsReconnectDelay / 1000}s...`);
    document.body.classList.remove('ws-connected');
    setTimeout(connectWebSocket, wsReconnectDelay);
    wsReconnectDelay = Math.min(wsReconnectDelay * 2, WS_MAX_RECONNECT_DELAY);
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
  renderRagDemo(data);
  renderPipeline();

  // Attempt to connect to live update server
  try {
    connectWebSocket();
  } catch (err) {
    console.warn('[Dashboard] WebSocket unavailable, dashboard will be static:', err.message);
  }
}

init();
