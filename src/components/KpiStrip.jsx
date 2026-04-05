import { KpiCard } from "./KpiCard";
import { sign, zLabel } from "../utils";

export function KpiStrip({ data }) {
  if (!data) return null;

  // Prefer city_months (all cities); fall back to vilnius_month_anomaly
  let m = data.vilnius_month_anomaly;
  if (data.city_months) {
    const citySlug = "vilnius";
    const cityData = data.city_months[citySlug] || data.city_months[Object.keys(data.city_months)[0]];
    if (cityData) {
      const marchData = cityData["march"] || cityData[Object.keys(cityData)[0]];
      if (marchData) m = marchData;
    }
  }
  const w = data.lithuania_weather;

  const items = [
    {
      label: `${m.city || "Vilnius"} ${m.month_name} ${m.latest_year.year} anomaly`,
      value:
        sign(m.latest_year.anomaly_c) +
        m.latest_year.anomaly_c.toFixed(1) +
        " \u00b0C",
      sub: `z = ${sign(m.latest_year.zscore)}${m.latest_year.zscore.toFixed(2)} \u00b7 ${zLabel(m.latest_year.zscore)}`,
      highlight: true,
    },
    {
      label: `${m.window?.years_included ?? 30}-yr ${m.month_name} baseline (${m.city || "Vilnius"})`,
      value:
        sign(m.baseline.mean_temp_c) +
        m.baseline.mean_temp_c.toFixed(2) +
        " \u00b0C",
      sub: `\u03c3 = ${m.baseline.std_temp_c.toFixed(2)} \u00b0C`,
    },
    {
      label: "Lithuania YTD temp anomaly",
      value: sign(w.temp_anomaly_c) + w.temp_anomaly_c.toFixed(1) + " \u00b0C",
      sub: `z = ${w.temp_zscore.toFixed(2)} \u00b7 vs 1991\u20132020`,
    },
    {
      label: "Lithuania last-7d temp signal",
      value:
        sign(w.latest_7d_temp_anomaly) +
        w.latest_7d_temp_anomaly.toFixed(1) +
        " \u00b0C",
      sub: `YTD precip anomaly ${sign(w.precip_anomaly_mm)}${w.precip_anomaly_mm.toFixed(0)} mm`,
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
