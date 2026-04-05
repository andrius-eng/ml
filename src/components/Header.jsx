export function Header({ data, wsConnected }) {
  const label = data ? `Updated ${data.generated_at}` : "Loading\u2026";
  return (
    <header className="site-header">
      <div className="container header-inner">
        <div>
          <h1>Lithuania Climate Anomaly Dashboard</h1>
          <p className="tagline">
            End-to-end MLOps pipeline: ERA5 reanalysis&nbsp;&rarr;&nbsp;Apache
            Airflow orchestration&nbsp;&rarr;&nbsp;live anomaly charts.
            Early-warning signal for agri, energy, and logistics clients exposed
            to temperature risk.
          </p>
        </div>
        <div className="header-meta">
          <span className="badge">
            {label}
            {wsConnected && (
              <span style={{ color: "#4caf50", fontWeight: 600 }}>
                {" "}
                &middot; 🔄 live
              </span>
            )}
          </span>
        </div>
      </div>
    </header>
  );
}
