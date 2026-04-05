import { useState, useEffect, useRef, useMemo } from "react";
import { Chart } from "chart.js";
import { sign } from "../utils";

function resolveMonthData(data, citySlug, monthSlug) {
  if (data.city_months && Object.keys(data.city_months).length > 0) {
    const cities = Object.keys(data.city_months);
    const city = citySlug || cities[0];
    const cityData = data.city_months[city] || {};
    const months = Object.keys(cityData);
    const month = monthSlug || months[0];
    return {
      m: cityData[month],
      cityOptions: cities,
      monthOptions: Object.entries(cityData).map(([k, v]) => ({
        slug: k,
        name: v.month_name,
      })),
      resolvedCity: city,
      resolvedMonth: month,
    };
  }

  const months = data.vilnius_months;
  if (months && Object.keys(months).length > 0) {
    const slugs = Object.keys(months);
    const defaultSlug = data.vilnius_month_anomaly
      ? data.vilnius_month_anomaly.month_name.toLowerCase()
      : slugs[0];
    const month = monthSlug || defaultSlug;
    return {
      m: months[month],
      cityOptions: [],
      monthOptions: slugs.map((k) => ({ slug: k, name: months[k].month_name })),
      resolvedCity: null,
      resolvedMonth: month,
    };
  }

  return {
    m: data.vilnius_month_anomaly,
    cityOptions: [],
    monthOptions: [],
    resolvedCity: null,
    resolvedMonth: null,
  };
}

export function MonthlyAnomalyChart({ data }) {
  const [citySlug, setCitySlug] = useState(null);
  const [monthSlug, setMonthSlug] = useState(null);
  const canvasRef = useRef(null);
  const chartRef = useRef(null);

  const { m, cityOptions, monthOptions, resolvedCity, resolvedMonth } = useMemo(
    () => resolveMonthData(data, citySlug, monthSlug),
    [data, citySlug, monthSlug],
  );

  // Sync slug state when data initialises (so selects show correct default)
  useEffect(() => {
    if (resolvedCity && !citySlug) setCitySlug(resolvedCity);
    if (resolvedMonth && !monthSlug) setMonthSlug(resolvedMonth);
  }, [resolvedCity, resolvedMonth, citySlug, monthSlug]);

  // Update RAG chips when city/month changes
  useEffect(() => {
    if (!m) return;
    const cityName = m.city || "Vilnius";
    const monthName = m.month_name;

    const chipMonthCity = document.getElementById("rag-chip-month-city");
    if (chipMonthCity)
      chipMonthCity.textContent = `How unusual is this ${monthName} in ${cityName}?`;
    const chipWarmest = document.getElementById("rag-chip-warmest");
    if (chipWarmest)
      chipWarmest.textContent = `What was the warmest ${monthName} on record in ${cityName}?`;
  }, [m]);

  useEffect(() => {
    if (!m || !canvasRef.current) return;

    const currentYear = m.latest_year.year;
    const labels = m.annual.map((r) => r.year);
    const values = m.annual.map((r) => r.anomaly_c);

    const bgColors = values.map((v, i) => {
      if (m.annual[i].year === currentYear)
        return v >= 0 ? "#ff6b35" : "#4895ef";
      return v >= 0 ? "rgba(239,68,68,0.65)" : "rgba(59,130,246,0.65)";
    });

    const borderColors = values.map((_v, i) =>
      m.annual[i].year === currentYear ? "#ffffff" : "transparent",
    );
    const borderWidths = values.map((_v, i) =>
      m.annual[i].year === currentYear ? 2 : 0,
    );

    if (chartRef.current) chartRef.current.destroy();
    chartRef.current = new Chart(canvasRef.current, {
      type: "bar",
      data: {
        labels,
        datasets: [
          {
            label: `${m.month_name} anomaly (\u00b0C)`,
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
                  `Anomaly: ${sign(row.anomaly_c)}${row.anomaly_c.toFixed(2)} \u00b0C`,
                  `Mean: ${sign(row.mean_temp_c)}${row.mean_temp_c.toFixed(2)} \u00b0C`,
                  `z-score: ${sign(row.zscore)}${row.zscore.toFixed(2)}`,
                ];
              },
            },
          },
        },
        scales: {
          x: {
            grid: { color: "rgba(255,255,255,0.06)" },
            ticks: { color: "rgba(255,255,255,0.6)", maxRotation: 45 },
          },
          y: {
            grid: { color: "rgba(255,255,255,0.08)" },
            ticks: {
              color: "rgba(255,255,255,0.6)",
              callback: (v) => sign(v) + v + " \u00b0C",
            },
            title: {
              display: true,
              text: "Temperature anomaly (\u00b0C)",
              color: "rgba(255,255,255,0.45)",
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
  }, [m]);

  if (!m) return null;

  const cityName = m.city || "Vilnius";
  const monthName = m.month_name;
  const yearsCount = m.window?.years_included ?? m.annual?.length ?? 30;

  return (
    <section className="card" id="march-section">
      <div
        className="section-header"
        style={{
          display: "flex",
          alignItems: "flex-start",
          justifyContent: "space-between",
          gap: "1rem",
        }}
      >
        <div>
          <h2>
            {cityName} &mdash; {monthName} Temperature Anomaly ({yearsCount}{" "}
            years)
          </h2>
          <p className="section-desc">
            Daily mean temperature vs. {yearsCount}-year baseline, {monthName}{" "}
            1&ndash;{m.window.cutoff_day} each year. Red&nbsp;= warmer than
            baseline &middot; Blue&nbsp;= cooler &middot;{" "}
            <strong>Outlined bar</strong>&nbsp;= current year.
          </p>
        </div>
        <div
          style={{
            display: "flex",
            gap: "0.5rem",
            flexShrink: 0,
            marginTop: "0.25rem",
            alignItems: "center",
          }}
        >
          {cityOptions.length > 1 && (
            <select
              className="beam-select"
              aria-label="Select city"
              value={citySlug || ""}
              onChange={(e) => setCitySlug(e.target.value)}
            >
              {cityOptions.map((slug) => (
                <option key={slug} value={slug}>
                  {slug.charAt(0).toUpperCase() + slug.slice(1)}
                </option>
              ))}
            </select>
          )}
          {monthOptions.length > 1 && (
            <select
              className="beam-select"
              aria-label="Select month"
              value={monthSlug || ""}
              onChange={(e) => setMonthSlug(e.target.value)}
            >
              {monthOptions.map(({ slug, name }) => (
                <option key={slug} value={slug}>
                  {name}
                </option>
              ))}
            </select>
          )}
        </div>
      </div>
      <div className="chart-wrap">
        <canvas ref={canvasRef} />
      </div>
    </section>
  );
}
