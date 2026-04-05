import { useState, useMemo } from "react";
import { anomalyColor, MONTH_LABELS } from "../utils";

function HeatmapTable({ city, info }) {
  const months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];

  return (
    <table className="heatmap-table">
      <thead>
        <tr>
          <th>Year</th>
          {months.map((m) => (
            <th key={m}>{MONTH_LABELS[m - 1]}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {info.years.map((yr) => {
          const yearData = info.data[String(yr)] || {};
          return (
            <tr key={yr}>
              <td className="heatmap-year">{yr}</td>
              {months.map((m) => {
                const entry = yearData[String(m)];
                const anomaly = entry ? entry.anomaly : null;
                const zPart =
                  entry && entry.z != null ? ` (z=${entry.z.toFixed(2)})` : "";
                const label =
                  anomaly != null
                    ? `${city} ${MONTH_LABELS[m - 1]} ${yr}: ${anomaly >= 0 ? "+" : ""}${anomaly.toFixed(2)} \u00b0C${zPart}`
                    : "";
                return (
                  <td
                    key={m}
                    className="heatmap-cell"
                    style={{ backgroundColor: anomalyColor(anomaly) }}
                    title={label}
                  >
                    {anomaly != null
                      ? (anomaly >= 0 ? "+" : "") + anomaly.toFixed(1)
                      : "\u2013"}
                  </td>
                );
              })}
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

export function BeamHeatmap({ data }) {
  const beam = data.beam_regional;
  const cityNames = useMemo(
    () => (beam && beam.cities ? Object.keys(beam.cities).sort() : []),
    [beam],
  );
  const [selectedCity, setSelectedCity] = useState(null);

  if (!beam || cityNames.length === 0) return null;

  const city = selectedCity || cityNames[0];
  const info = beam.cities[city];

  return (
    <section className="card" id="beam-section">
      <div className="section-header">
        <div>
          <h2>Regional Monthly Anomalies &mdash; Apache Beam</h2>
          <p className="section-desc">
            Month-by-month temperature anomaly (&deg;C vs 1991&ndash;2020 WMO
            baseline) for Lithuanian and neighbouring cities. Computed by an
            Apache Beam pipeline. Select a city to view its year &times; month
            heatmap.
          </p>
        </div>
      </div>
      <div className="beam-controls">
        <select
          className="beam-select"
          value={city}
          onChange={(e) => setSelectedCity(e.target.value)}
        >
          {cityNames.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>
      </div>
      <div className="heatmap-wrap">
        {info && <HeatmapTable city={city} info={info} />}
      </div>
    </section>
  );
}
