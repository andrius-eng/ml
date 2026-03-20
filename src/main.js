import './styles.css';
import { Chart, registerables } from 'chart.js';

Chart.register(...registerables);

// Dashboard data is fetched dynamically so we always get the latest
// pipeline output (written by Airflow → export_frontend_data.py).
let data = null;

let marchChartInstance = null;
let cityTempChartInstance = null;
let cityPrecipChartInstance = null;
let mlParityChartInstance = null;
let mlResidualChartInstance = null;

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

// ── Regional Beam heatmap ────────────────────────────────────────────────────

function anomalyColor(val) {
  if (val === null || val === undefined) return 'rgba(255,255,255,0.04)';
  // Blue for cold, red for warm, white at zero
  const clamped = Math.max(-6, Math.min(6, val));
  const t = clamped / 6; // -1 to 1
  if (t < 0) {
    const a = -t;
    return `rgba(59,130,246,${(0.2 + a * 0.7).toFixed(2)})`;
  }
  const a = t;
  return `rgba(239,68,68,${(0.2 + a * 0.7).toFixed(2)})`;
}

const MONTH_LABELS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

function renderBeamHeatmap(d) {
  const beam = d.beam_regional;
  const section = document.getElementById('beam-section');
  if (!beam || !beam.cities) { section.style.display = 'none'; return; }

  const select = document.getElementById('beam-city-select');
  const wrap = document.getElementById('heatmap-wrap');
  const cityNames = Object.keys(beam.cities).sort();
  if (cityNames.length === 0) { section.style.display = 'none'; return; }

  // Populate select
  select.innerHTML = '';
  cityNames.forEach((c) => {
    const opt = document.createElement('option');
    opt.value = c;
    opt.textContent = c;
    select.appendChild(opt);
  });

  function draw(city) {
    const info = beam.cities[city];
    if (!info) { wrap.innerHTML = ''; return; }

    const years = info.years;
    const months = Array.from({ length: 12 }, (_v, i) => i + 1);

    // Build table
    const table = document.createElement('table');
    table.className = 'heatmap-table';

    // Header row with month labels
    const thead = document.createElement('thead');
    const hRow = document.createElement('tr');
    const corner = document.createElement('th');
    corner.textContent = 'Year';
    hRow.appendChild(corner);
    months.forEach((m) => {
      const th = document.createElement('th');
      th.textContent = MONTH_LABELS[m - 1] || m;
      hRow.appendChild(th);
    });
    thead.appendChild(hRow);
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    years.forEach((yr) => {
      const row = document.createElement('tr');
      const yearCell = document.createElement('td');
      yearCell.className = 'heatmap-year';
      yearCell.textContent = yr;
      row.appendChild(yearCell);

      const yearData = info.data[String(yr)] || {};
      months.forEach((m) => {
        const cell = document.createElement('td');
        const entry = yearData[String(m)];
        const anomaly = entry ? entry.anomaly : null;
        cell.className = 'heatmap-cell';
        cell.style.backgroundColor = anomalyColor(anomaly);
        if (anomaly !== null && anomaly !== undefined) {
          cell.textContent = (anomaly >= 0 ? '+' : '') + anomaly.toFixed(1);
          const zPart = (entry && entry.z !== null && entry.z !== undefined)
            ? ` (z=${entry.z.toFixed(2)})`
            : '';
          cell.title = `${city} ${MONTH_LABELS[m - 1]} ${yr}: ${anomaly >= 0 ? '+' : ''}${anomaly.toFixed(2)} °C` +
            zPart;
        } else {
          cell.textContent = '–';
        }
        row.appendChild(cell);
      });
      tbody.appendChild(row);
    });
    table.appendChild(tbody);

    wrap.innerHTML = '';
    wrap.appendChild(table);
  }

  select.addEventListener('change', () => draw(select.value));
  draw(cityNames[0]);
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
      sub: `z = ${w.temp_zscore.toFixed(2)} · vs 1991–2025`,
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
      scales: baseScaleOpts('z-score vs 1991–2025'),
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
      scales: baseScaleOpts('z-score vs 1991–2025'),
    },
  });
}

function renderMLMetrics(d) {
  const row = document.getElementById('ml-kpi-row');
  row.innerHTML = '';
  const ml = d.ml_model;

  [
    { label: 'R² (test set)', value: ml.r2.toFixed(4), sub: 'Variance explained', highlight: ml.r2 >= 0.65 },
    { label: 'RMSE', value: ml.rmse.toFixed(2) + ' °C', sub: 'Root mean squared error' },
    { label: 'MAE', value: ml.mae.toFixed(2) + ' °C', sub: 'Mean absolute error' },
  ].forEach((item) => row.appendChild(kpiCard(item)));
}

function renderMLCharts(d) {
  const preds = d.ml_model.predictions;
  if (!preds || preds.length === 0) return;

  // ── Parity chart: predicted vs actual ──
  const parityData = preds.map((p) => ({ x: p.actual, y: p.predicted }));
  const allVals = preds.flatMap((p) => [p.actual, p.predicted]);
  const lo = Math.floor(Math.min(...allVals)) - 2;
  const hi = Math.ceil(Math.max(...allVals)) + 2;

  if (mlParityChartInstance) mlParityChartInstance.destroy();
  mlParityChartInstance = new Chart(document.getElementById('mlParityChart'), {
    type: 'scatter',
    data: {
      datasets: [
        {
          label: 'Predictions',
          data: parityData,
          backgroundColor: 'rgba(99,202,183,0.5)',
          borderColor: 'rgba(99,202,183,0.8)',
          pointRadius: 2.5,
        },
        {
          label: 'Perfect fit',
          data: [{ x: lo, y: lo }, { x: hi, y: hi }],
          type: 'line',
          borderColor: 'rgba(255,255,255,0.3)',
          borderDash: [6, 4],
          borderWidth: 1,
          pointRadius: 0,
          fill: false,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        title: { display: true, text: 'Predicted vs Actual (°C)', color: '#f7f7f7' },
        legend: { display: false },
      },
      scales: {
        x: {
          min: lo, max: hi,
          title: { display: true, text: 'Actual °C', color: 'rgba(255,255,255,0.45)' },
          grid: { color: 'rgba(255,255,255,0.06)' },
          ticks: { color: 'rgba(255,255,255,0.6)' },
        },
        y: {
          min: lo, max: hi,
          title: { display: true, text: 'Predicted °C', color: 'rgba(255,255,255,0.45)' },
          grid: { color: 'rgba(255,255,255,0.08)' },
          ticks: { color: 'rgba(255,255,255,0.6)' },
        },
      },
    },
  });

  // ── Residual histogram ──
  const residuals = preds.map((p) => +(p.actual - p.predicted).toFixed(2));
  const bucketSize = 1;
  const rMin = Math.floor(Math.min(...residuals));
  const rMax = Math.ceil(Math.max(...residuals));
  const bucketLabels = [];
  const bucketCounts = [];
  for (let b = rMin; b <= rMax; b += bucketSize) {
    bucketLabels.push(b);
    bucketCounts.push(residuals.filter((r) => r >= b && r < b + bucketSize).length);
  }

  if (mlResidualChartInstance) mlResidualChartInstance.destroy();
  mlResidualChartInstance = new Chart(document.getElementById('mlResidualChart'), {
    type: 'bar',
    data: {
      labels: bucketLabels,
      datasets: [{
        label: 'Count',
        data: bucketCounts,
        backgroundColor: 'rgba(99,202,183,0.6)',
        borderRadius: 2,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        title: { display: true, text: 'Residual Distribution (°C)', color: '#f7f7f7' },
        legend: { display: false },
      },
      scales: {
        x: {
          title: { display: true, text: 'Actual − Predicted (°C)', color: 'rgba(255,255,255,0.45)' },
          grid: { color: 'rgba(255,255,255,0.06)' },
          ticks: { color: 'rgba(255,255,255,0.6)' },
        },
        y: {
          title: { display: true, text: 'Frequency', color: 'rgba(255,255,255,0.45)' },
          grid: { color: 'rgba(255,255,255,0.08)' },
          ticks: { color: 'rgba(255,255,255,0.6)' },
        },
      },
    },
  });
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
      desc: 'Trains a PyTorch MLP on full-year ERA5 Lithuania daily weather (1991–2022) to predict daily mean temperature from seasonal + trend features. Evaluates R² and RMSE on held-out 2023+ data, logging to MLflow.',
      steps: ['prepare_data', 'train_model', 'evaluate_model', 'quality_gate', 'refresh_rag_context'],
      tags: ['PyTorch', 'MLflow', 'ERA5', 'Seasonality'],
    },
    {
      name: 'lithuania_weather_analysis',
      desc: 'Fetches ERA5 daily weather for Lithuanian cities, computes YTD anomalies, city rankings, per-city charts, runs an Apache Beam pipeline for regional monthly anomaly analysis, and validates output quality.',
      steps: ['fetch_weather', 'analyze_anomalies', 'beam_regional', 'plot_charts', 'quality_gate', 'refresh_rag_context'],
      tags: ['ERA5', 'Anomaly detection', 'Apache Beam', 'Vega/Matplotlib'],
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
      const newData = await fetchDashboardData();
      if (!newData) return;
      data = newData;

      // Re-render with new data
      renderHeader(data);
      renderKPIs(data);
      renderMarchChart(data);
      renderCityCharts(data);
      renderBeamHeatmap(data);
      renderMLMetrics(data);
      renderMLCharts(data);
      renderRagDemo(data);
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

// ── RAG query form handler ──────────────────────────────────────────────────

document.getElementById('rag-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const input = document.getElementById('rag-input');
  const question = input.value.trim();
  if (!question) return;

  const resultDiv = document.getElementById('rag-query-result');
  resultDiv.textContent = 'Loading...';
  resultDiv.removeAttribute('hidden');

  try {
    const response = await fetch(`/api/rag/query?q=${encodeURIComponent(question)}`);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();

    const interpHtml = data.interpretation
      ? `<div class="rag-interpretation">${data.interpretation}</div>`
      : '';

    resultDiv.innerHTML = `
      <h3>${data.question}</h3>
      <p>${data.answer}</p>
      ${interpHtml}
      <div class="rag-sources">
        ${data.sources.map(s => `<span>${s.title} (${s.score.toFixed(2)})</span>`).join('')}
      </div>
    `;
  } catch (err) {
    if (err.message.includes('502') || err.message.includes('503')) {
      resultDiv.innerHTML = '<p style="color:var(--muted)">RAG API is not running. Start it with:<br><code>uv run uvicorn --app-dir python serve:app --host 127.0.0.1 --port 8000</code></p>';
    } else {
      resultDiv.textContent = `Error: ${err.message}`;
    }
  }
});

// ── boot ─────────────────────────────────────────────────────────────────────

async function fetchDashboardData() {
  // Try the nginx-served static file first, fall back to the API
  for (const url of ['/data/dashboard.json', '/api/dashboard']) {
    try {
      const res = await fetch(url);
      if (res.ok) return await res.json();
    } catch (_) { /* try next */ }
  }
  // Last resort: bundled fallback (may be stale)
  try {
    const mod = await import('./data/dashboard.json');
    return mod.default;
  } catch (_) { return null; }
}

async function init() {
  data = await fetchDashboardData();
  if (!data) {
    document.getElementById('kpi-row').innerHTML =
      '<p style="color:var(--muted)">Waiting for first pipeline run…</p>';
    return;
  }
  renderHeader(data);
  renderKPIs(data);
  renderMarchChart(data);
  renderCityCharts(data);
  renderBeamHeatmap(data);
  renderMLMetrics(data);
  renderMLCharts(data);
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
