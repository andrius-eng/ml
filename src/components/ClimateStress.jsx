import { useEffect, useRef } from "react";
import { Chart } from "chart.js";
import { KpiCard } from "./KpiCard";
import { sign } from "../utils";

export function ClimateStress({ data }) {
  const canvasRef = useRef(null);
  const chartRef = useRef(null);

  const hs = data.heat_stress;
  const hdd = data.heating_degree_days;
  const wm = data.weather_mlflow;

  useEffect(() => {
    const hddData = hdd;
    if (
      !hddData ||
      !Array.isArray(hddData.recent_months) ||
      hddData.recent_months.length === 0
    ) {
      return;
    }
    if (!canvasRef.current) return;

    const labels = hddData.recent_months.map((r) => r.month);
    const values = hddData.recent_months.map((r) => r.hdd);

    if (chartRef.current) chartRef.current.destroy();
    chartRef.current = new Chart(canvasRef.current, {
      type: "bar",
      data: {
        labels,
        datasets: [
          {
            label: "Monthly HDD (Eurostat)",
            data: values,
            backgroundColor: values.map((v) =>
              v > 300
                ? "rgba(59,130,246,0.7)"
                : v > 100
                  ? "rgba(99,179,237,0.6)"
                  : "rgba(186,230,253,0.5)",
            ),
            borderRadius: 3,
          },
        ],
      },
      options: {
        responsive: true,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (ctx) => `${ctx.parsed.y.toFixed(1)} HDD`,
            },
          },
        },
        scales: {
          x: {
            ticks: { color: "#94a3b8", font: { size: 11 } },
            grid: { color: "rgba(255,255,255,0.05)" },
          },
          y: {
            ticks: { color: "#94a3b8" },
            grid: { color: "rgba(255,255,255,0.05)" },
            title: {
              display: true,
              text: "Heating Degree Days",
              color: "#94a3b8",
            },
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
  }, [hdd]);

  const stressItems = hs
    ? [
        {
          label: `Frost days ${hs.current_year} YTD`,
          value: hs.frost_days.current,
          sub: `baseline ${hs.frost_days.baseline_mean_1991_2020.toFixed(1)} \u00b7 anomaly ${sign(hs.frost_days.anomaly)}${hs.frost_days.anomaly.toFixed(1)}`,
          highlight: Math.abs(hs.frost_days.anomaly) > 5,
        },
        {
          label: "Cold nights (<\u221215 \u00b0C)",
          value: hs.cold_nights.current,
          sub: `baseline ${hs.cold_nights.baseline_mean_1991_2020.toFixed(1)} \u00b7 anomaly ${sign(hs.cold_nights.anomaly)}${hs.cold_nights.anomaly.toFixed(1)}`,
        },
        {
          label: "Hot days (>25 \u00b0C)",
          value: hs.hot_days.current,
          sub: `baseline ${hs.hot_days.baseline_mean_1991_2020.toFixed(1)} \u00b7 anomaly ${sign(hs.hot_days.anomaly)}${hs.hot_days.anomaly.toFixed(1)}`,
        },
        {
          label: "Tropical nights (>20 \u00b0C)",
          value: hs.tropical_nights.current,
          sub: `baseline ${hs.tropical_nights.baseline_mean_1991_2020.toFixed(1)} \u00b7 anomaly ${sign(hs.tropical_nights.anomaly)}${hs.tropical_nights.anomaly.toFixed(1)}`,
        },
      ]
    : null;

  const wmItems = [];
  if (wm && Object.keys(wm).length > 0) {
    if (wm.sunshine_h != null)
      wmItems.push({
        label: "YTD Sunshine",
        value: wm.sunshine_h.toFixed(1) + " h",
        sub:
          wm.trend_direction === 1
            ? "warming trend"
            : wm.trend_direction === -1
              ? "cooling trend"
              : "",
      });
    if (wm.snowfall_cm != null)
      wmItems.push({
        label: "YTD Snowfall",
        value: wm.snowfall_cm.toFixed(1) + " cm",
        sub:
          wm.snowfall_deviation_cm != null
            ? `${sign(wm.snowfall_deviation_cm)}${wm.snowfall_deviation_cm.toFixed(1)} cm vs baseline`
            : "",
      });
    if (wm.wind_kmh != null)
      wmItems.push({
        label: "Mean Wind Speed",
        value: wm.wind_kmh.toFixed(1) + " km/h",
        sub: "YTD average",
      });
    if (wm.et0_mm != null)
      wmItems.push({
        label: "Reference ET\u2080",
        value: wm.et0_mm.toFixed(1) + " mm",
        sub: "YTD evapotranspiration",
      });
    if (wm.quality_gate != null) {
      const qg = wm.quality_gate;
      wmItems.push({
        label: "Weather QA",
        value: qg.passed ? "PASS" : "FAIL",
        sub: `${qg.n_extreme_temp_months} extreme-temp \u00b7 ${qg.n_extreme_precip_months} extreme-precip \u00b7 ${qg.n_weak_months} weak months`,
        highlight: !qg.passed,
      });
    }
  }

  const hddItems = [];
  if (hdd && hdd.ytd && hdd.ytd.months > 0) {
    const lagNote =
      hdd.data_lag_months > 3
        ? ` \u00b7 Eurostat data through ${hdd.data_through} (${hdd.data_lag_months}mo lag)`
        : "";
    hddItems.push({
      label: `HDD ${hdd.ytd.label}`,
      value: hdd.ytd.total_hdd.toFixed(0),
      sub: `baseline ${hdd.ytd.baseline_mean_1991_2020.toFixed(0)} \u00b7 anomaly ${sign(hdd.ytd.anomaly)}${hdd.ytd.anomaly.toFixed(0)}${lagNote}`,
      highlight: Math.abs(hdd.ytd.anomaly) > 150,
    });
    hddItems.push({
      label: `Heating season ${hdd.heating_season.label}`,
      value: hdd.heating_season.total_hdd.toFixed(0),
      sub: `${hdd.heating_season.months_included} months \u00b7 baseline ${hdd.heating_season.baseline_mean_1991_2020.toFixed(0)} \u00b7 anomaly ${sign(hdd.heating_season.anomaly)}${hdd.heating_season.anomaly.toFixed(0)}`,
      highlight: Math.abs(hdd.heating_season.anomaly) > 200,
    });
  }

  const allKpiItems = [...(stressItems || []), ...wmItems, ...hddItems];
  const hddChartVisible =
    hdd && Array.isArray(hdd.recent_months) && hdd.recent_months.length > 0;

  return (
    <section className="card" id="climate-stress-section">
      <div className="section-header">
        <div>
          <h2>Climate Stress Indicators</h2>
          <p className="section-desc">
            Frost days, cold nights, and heating degree days (HDD) for Lithuania
            YTD vs. the 1991&ndash;2020 baseline. HDD source: Eurostat{" "}
            <code>nrg_chdd_m</code>.
          </p>
        </div>
      </div>
      <div className="kpi-row">
        {stressItems ? (
          allKpiItems.map((item) => <KpiCard key={item.label} {...item} />)
        ) : (
          <p style={{ opacity: 0.5 }}>Heat stress data not yet available.</p>
        )}
      </div>
      {hddChartVisible && (
        <div className="chart-wrap">
          <canvas ref={canvasRef} />
        </div>
      )}
    </section>
  );
}
