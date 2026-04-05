import "./styles.css";
import { Chart, registerables } from "chart.js";
import { Header } from "./components/Header";
import { KpiStrip } from "./components/KpiStrip";
import { MonthlyAnomalyChart } from "./components/MonthlyAnomalyChart";
import { CityComparisonCharts } from "./components/CityComparisonCharts";
import { ClimateStress } from "./components/ClimateStress";
import { BeamHeatmap } from "./components/BeamHeatmap";
import { MLSection } from "./components/MLSection";
import { RagSection } from "./components/RagSection";
import { PipelineArchitecture } from "./components/PipelineArchitecture";
import { useDashboardData } from "./hooks/useDashboardData";

Chart.register(...registerables);

export default function App() {
  const { data, wsConnected } = useDashboardData();

  return (
    <>
      <Header data={data} wsConnected={wsConnected} />
      <main className="container">
        {!data ? (
          <div className="kpi-row">
            <p style={{ color: "var(--muted)" }}>
              Waiting for first pipeline run&hellip;
            </p>
          </div>
        ) : (
          <>
            <KpiStrip data={data} />
            <MonthlyAnomalyChart data={data} />
            <CityComparisonCharts data={data} />
            <ClimateStress data={data} />
            <BeamHeatmap data={data} />
            <MLSection data={data} />
            <RagSection data={data} />
            <PipelineArchitecture data={data} />
          </>
        )}
      </main>
      <footer className="footer">
        <div className="container">
          <p>
            Data:&nbsp;
            <a href="https://open-meteo.com" target="_blank" rel="noreferrer">
              Open-Meteo ERA5
            </a>
            &ensp;&middot;&ensp;Orchestration:&nbsp;Apache Airflow
            &ensp;&middot;&ensp;Experiment tracking:&nbsp;MLflow
            &ensp;&middot;&ensp;Frontend:&nbsp;React + Vite + Chart.js
          </p>
        </div>
      </footer>
    </>
  );
}
