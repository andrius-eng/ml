import { KpiCard } from "./KpiCard";
import { sign, zLabel } from "../utils";

function finiteNumber(v) {
  return typeof v === "number" && Number.isFinite(v);
}

function inRange(v, min, max) {
  return finiteNumber(v) && v >= min && v <= max;
}

function safeMetric(value, unit, min, max, digits = 1) {
  if (!inRange(value, min, max)) {
    return { value: "N/A", valid: false };
  }
  return { value: `${sign(value)}${value.toFixed(digits)} ${unit}`, valid: true };
}

export function KpiStrip({ data }) {
  if (!data) return null;

  // Prefer city_months (all cities); fall back to vilnius_month_anomaly
  let m = data.vilnius_month_anomaly;
  if (data.city_months) {
    const citySlug = "vilnius";
    const cityData =
      data.city_months[citySlug] ||
      data.city_months[Object.keys(data.city_months)[0]];
    if (cityData) {
      const marchData = cityData["march"] || cityData[Object.keys(cityData)[0]];
      if (marchData) m = marchData;
    }
  }
  const w = data.lithuania_weather;

  if (!m || !m.latest_year || !m.baseline || !w) return null;

  // Climate sanity bounds to avoid rendering corrupted pipeline values.
  const latestAnomaly = safeMetric(m.latest_year.anomaly_c, "\u00b0C", -20, 20, 1);
  const latestZ = inRange(m.latest_year.zscore, -8, 8)
    ? `${sign(m.latest_year.zscore)}${m.latest_year.zscore.toFixed(2)}`
    : "N/A";
  const baselineMean = safeMetric(m.baseline.mean_temp_c, "\u00b0C", -30, 40, 2);
  const baselineStd = inRange(m.baseline.std_temp_c, 0, 15)
    ? `${m.baseline.std_temp_c.toFixed(2)} \u00b0C`
    : "N/A";
  const ytdTemp = safeMetric(w.temp_anomaly_c, "\u00b0C", -20, 20, 1);
  const ytdTempZ = inRange(w.temp_zscore, -8, 8)
    ? w.temp_zscore.toFixed(2)
    : "N/A";
  const last7d = safeMetric(w.latest_7d_temp_anomaly, "\u00b0C", -20, 20, 1);
  const precip = inRange(w.precip_anomaly_mm, -1000, 1500)
    ? `${sign(w.precip_anomaly_mm)}${w.precip_anomaly_mm.toFixed(0)} mm`
    : "N/A";

  const items = [
    {
      label: `${m.city || "Vilnius"} ${m.month_name} ${m.latest_year.year} anomaly`,
      value: latestAnomaly.value,
      sub:
        latestAnomaly.valid && latestZ !== "N/A"
          ? `z = ${latestZ} \u00b7 ${zLabel(m.latest_year.zscore)}`
          : "data quality gate tripped",
      highlight: true,
    },
    {
      label: `${m.window?.years_included ?? 30}-yr ${m.month_name} baseline (${m.city || "Vilnius"})`,
      value: baselineMean.value,
      sub:
        baselineMean.valid && baselineStd !== "N/A"
          ? `\u03c3 = ${baselineStd}`
          : "data quality gate tripped",
    },
    {
      label: "Lithuania YTD temp anomaly",
      value: ytdTemp.value,
      sub:
        ytdTemp.valid && ytdTempZ !== "N/A"
          ? `z = ${ytdTempZ} \u00b7 vs 1991\u20132020`
          : "data quality gate tripped",
    },
    {
      label: "Lithuania last-7d temp signal",
      value: last7d.value,
      sub:
        last7d.valid && precip !== "N/A"
          ? `YTD precip anomaly ${precip}`
          : "data quality gate tripped",
    },
  ];

  return (
    <div className="kpi-row">
      {items.map((item) => (
        <KpiCard key={item.label} {...item} />
      ))}
    </div>
  );
}
