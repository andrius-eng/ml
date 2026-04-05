import { useEffect, useRef } from "react";
import { Chart } from "chart.js";
import { KpiCard } from "./KpiCard";
import { sign } from "../utils";

function ScatterChart({ canvasRef, data: chartData, lo, hi }) {
  const chartRef = useRef(null);
  useEffect(() => {
    if (!canvasRef.current || !chartData) return;
    if (chartRef.current) chartRef.current.destroy();
    chartRef.current = new Chart(canvasRef.current, {
      type: "scatter",
      data: {
        datasets: [
          {
            label: "Predictions",
            data: chartData,
            backgroundColor: "rgba(99,202,183,0.5)",
            borderColor: "rgba(99,202,183,0.8)",
            pointRadius: 2.5,
          },
          {
            label: "Perfect fit",
            data: [
              { x: lo, y: lo },
              { x: hi, y: hi },
            ],
            type: "line",
            borderColor: "rgba(255,255,255,0.3)",
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
          title: {
            display: true,
            text: "Predicted vs Actual (\u00b0C)",
            color: "#f7f7f7",
          },
          legend: { display: false },
        },
        scales: {
          x: {
            min: lo,
            max: hi,
            title: {
              display: true,
              text: "Actual \u00b0C",
              color: "rgba(255,255,255,0.45)",
            },
            grid: { color: "rgba(255,255,255,0.06)" },
            ticks: { color: "rgba(255,255,255,0.6)" },
          },
          y: {
            min: lo,
            max: hi,
            title: {
              display: true,
              text: "Predicted \u00b0C",
              color: "rgba(255,255,255,0.45)",
            },
            grid: { color: "rgba(255,255,255,0.08)" },
            ticks: { color: "rgba(255,255,255,0.6)" },
          },
        },
      },
    });
    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
  }, [canvasRef, chartData, lo, hi]);
  return null;
}

function ResidualChart({ canvasRef, residuals, bucketLabels, bucketCounts }) {
  const chartRef = useRef(null);
  useEffect(() => {
    if (!canvasRef.current || !bucketCounts) return;
    if (chartRef.current) chartRef.current.destroy();
    chartRef.current = new Chart(canvasRef.current, {
      type: "bar",
      data: {
        labels: bucketLabels,
        datasets: [
          {
            label: "Count",
            data: bucketCounts,
            backgroundColor: "rgba(99,202,183,0.6)",
            borderRadius: 2,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          title: {
            display: true,
            text: "Residual Distribution (\u00b0C)",
            color: "#f7f7f7",
          },
          legend: { display: false },
        },
        scales: {
          x: {
            title: {
              display: true,
              text: "Actual \u2212 Predicted (\u00b0C)",
              color: "rgba(255,255,255,0.45)",
            },
            grid: { color: "rgba(255,255,255,0.06)" },
            ticks: { color: "rgba(255,255,255,0.6)" },
          },
          y: {
            title: {
              display: true,
              text: "Frequency",
              color: "rgba(255,255,255,0.45)",
            },
            grid: { color: "rgba(255,255,255,0.08)" },
            ticks: { color: "rgba(255,255,255,0.6)" },
          },
        },
      },
    });
    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
  }, [canvasRef, bucketLabels, bucketCounts]);
  return null;
}

function HistoryChart({ canvasRef, history }) {
  const chartRef = useRef(null);
  useEffect(() => {
    if (!canvasRef.current || !history || history.length === 0) return;
    if (chartRef.current) chartRef.current.destroy();
    const labels = history.map((r) => r.date);
    const r2Data = history.map((r) => r.r2);
    const rmseData = history.map((r) => r.rmse);
    chartRef.current = new Chart(canvasRef.current, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "R\u00b2 (test)",
            data: r2Data,
            borderColor: "rgba(99,202,183,0.9)",
            backgroundColor: "rgba(99,202,183,0.15)",
            tension: 0.3,
            pointRadius: 4,
            pointHoverRadius: 6,
            yAxisID: "yR2",
            fill: false,
          },
          {
            label: "RMSE (test, \u00b0C)",
            data: rmseData,
            borderColor: "rgba(255,160,80,0.9)",
            backgroundColor: "rgba(255,160,80,0.15)",
            tension: 0.3,
            pointRadius: 4,
            pointHoverRadius: 6,
            yAxisID: "yRmse",
            fill: false,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          title: {
            display: true,
            text: "Model Performance Across Training Runs (MLflow)",
            color: "#f7f7f7",
          },
          tooltip: {
            callbacks: {
              label: (ctx) => {
                const r = history[ctx.dataIndex];
                if (ctx.datasetIndex === 0) {
                  const bias =
                    r.residual_mean != null
                      ? `  bias ${(r.residual_mean >= 0 ? "+" : "") + r.residual_mean.toFixed(3)}`
                      : "";
                  return `R\u00b2: ${r.r2.toFixed(4)}  (run ${r.run_id})${bias}`;
                }
                const spread =
                  r.residual_std != null
                    ? `  \u03c3 ${r.residual_std.toFixed(3)}`
                    : "";
                return `RMSE: ${r.rmse.toFixed(4)} \u00b0C  MAE: ${r.mae.toFixed(4)} \u00b0C${spread}`;
              },
            },
          },
          legend: { labels: { color: "rgba(255,255,255,0.75)" } },
        },
        scales: {
          x: {
            ticks: { color: "rgba(255,255,255,0.6)", maxRotation: 45 },
            grid: { color: "rgba(255,255,255,0.06)" },
          },
          yR2: {
            type: "linear",
            position: "left",
            min: Math.min(-0.5, Math.min(...r2Data) - 0.05),
            max: 1.0,
            title: {
              display: true,
              text: "R\u00b2",
              color: "rgba(99,202,183,0.8)",
            },
            ticks: { color: "rgba(99,202,183,0.8)" },
            grid: { color: "rgba(255,255,255,0.06)" },
          },
          yRmse: {
            type: "linear",
            position: "right",
            min: 0,
            title: {
              display: true,
              text: "RMSE (\u00b0C)",
              color: "rgba(255,160,80,0.8)",
            },
            ticks: { color: "rgba(255,160,80,0.8)" },
            grid: { drawOnChartArea: false },
          },
        },
      },
    });
    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
  }, [canvasRef, history]);
  return null;
}

export function MLSection({ data }) {
  const parityCanvasRef = useRef(null);
  const residualCanvasRef = useRef(null);
  const historyCanvasRef = useRef(null);

  const ml = data.ml_model;

  const kpiItems = [
    {
      label: "R\u00b2 (test set)",
      value: ml.r2.toFixed(4),
      sub: "Variance explained",
      highlight: ml.r2 >= 0.65,
    },
    {
      label: "RMSE",
      value: ml.rmse.toFixed(2) + " \u00b0C",
      sub: "Root mean squared error",
    },
    {
      label: "MAE",
      value: ml.mae.toFixed(2) + " \u00b0C",
      sub: "Mean absolute error",
    },
  ];
  if (ml.residual_mean != null)
    kpiItems.push({
      label: "Residual Bias",
      value:
        (ml.residual_mean >= 0 ? "+" : "") +
        ml.residual_mean.toFixed(3) +
        " \u00b0C",
      sub: "Mean of (actual \u2212 predicted)",
      highlight: Math.abs(ml.residual_mean) > 0.5,
    });
  if (ml.residual_std != null)
    kpiItems.push({
      label: "Residual Std",
      value: ml.residual_std.toFixed(3) + " \u00b0C",
      sub: "Spread of residuals",
    });

  const preds = ml.predictions || [];
  const allVals = preds.flatMap((p) => [p.actual, p.predicted]);
  const lo = allVals.length ? Math.floor(Math.min(...allVals)) - 2 : -30;
  const hi = allVals.length ? Math.ceil(Math.max(...allVals)) + 2 : 30;
  const parityData = preds.map((p) => ({ x: p.actual, y: p.predicted }));

  const residuals = preds.map((p) => +(p.actual - p.predicted).toFixed(2));
  const rMin = residuals.length ? Math.floor(Math.min(...residuals)) : -5;
  const rMax = residuals.length ? Math.ceil(Math.max(...residuals)) : 5;
  const bucketLabels = [];
  const bucketCounts = [];
  for (let b = rMin; b <= rMax; b++) {
    bucketLabels.push(b);
    bucketCounts.push(residuals.filter((r) => r >= b && r < b + 1).length);
  }

  const history = ml.history || [];
  const hasHistory = history.length > 0;
  const p = ml.params;

  const trainFrom = p?.train_from_year ?? null;
  const testFrom = p?.test_from_year ?? null;
  const splitDesc =
    trainFrom && testFrom
      ? `${trainFrom}\u2013${testFrom - 1} train / ${testFrom}+ test`
      : testFrom
        ? `test set from ${testFrom}`
        : null;

  return (
    <>
      <section className="card" id="ml-section">
        <h2>ML Model &mdash; Regression Performance</h2>
        <p className="section-desc">
          PyTorch MLP trained on full-year ERA5 daily temperatures
          {splitDesc ? ` (${splitDesc})` : ""} using seasonal &amp; trend
          features. Metrics from held-out test set.
        </p>
        <div className="kpi-row">
          {kpiItems.map((item) => (
            <KpiCard key={item.label} {...item} />
          ))}
        </div>
        {p && (
          <div className="ml-config-strip">
            <span className="ml-config-label">Training config</span>
            <span>epochs&thinsp;{p.epochs}</span>
            <span>batch&thinsp;{p.batch_size}</span>
            <span>lr&thinsp;{p.lr}</span>
            <span>{p.train_rows?.toLocaleString()} train rows</span>
            <span>{p.feature_count} features</span>
            {p.features && (
              <span className="ml-config-features">
                {p.features.split(",").join(" \u00b7 ")}
              </span>
            )}
          </div>
        )}
        {preds.length > 0 && (
          <div className="chart-row">
            <div className="chart-wrap chart-wrap--half">
              <canvas ref={parityCanvasRef} />
              <ScatterChart
                canvasRef={parityCanvasRef}
                data={parityData}
                lo={lo}
                hi={hi}
              />
            </div>
            <div className="chart-wrap chart-wrap--half">
              <canvas ref={residualCanvasRef} />
              <ResidualChart
                canvasRef={residualCanvasRef}
                residuals={residuals}
                bucketLabels={bucketLabels}
                bucketCounts={bucketCounts}
              />
            </div>
          </div>
        )}
      </section>

      {hasHistory && (
        <section className="card" id="ml-history-section">
          <h2>ML Model &mdash; Training History</h2>
          <p className="section-desc">
            R&sup2; and RMSE across all training runs tracked in MLflow. Each
            point is one experiment run; the highlighted region shows the
            architectural fix that lifted R&sup2; from &lt;0.2 to &gt;0.91.
          </p>
          <div className="chart-wrap">
            <canvas ref={historyCanvasRef} />
            <HistoryChart canvasRef={historyCanvasRef} history={history} />
          </div>
        </section>
      )}
    </>
  );
}
