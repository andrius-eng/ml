export function PipelineArchitecture({ data }) {
  const m = data.vilnius_month_anomaly;
  const months = data.vilnius_months || {};
  const cityName = m?.city || "Vilnius";
  const monthName = m?.month_name || Object.values(months)[0]?.month_name || "";
  const yearsIncluded = m?.window?.years_included ?? 85;

  const dags = [
    {
      name: "climate_temperature_model",
      desc: "Trains a PyTorch MLP on full-year ERA5 Lithuania daily weather to predict daily mean temperature from seasonal + trend features. Evaluates R\u00b2 and RMSE on held-out test data, logging to MLflow.",
      steps: [
        "prepare_data",
        "train_model",
        "evaluate_model",
        "quality_gate",
        "refresh_rag_context",
      ],
      tags: ["PyTorch", "MLflow", "ERA5", "Seasonality"],
    },
    {
      name: "lithuania_weather_analysis",
      desc: "Fetches ERA5 daily weather for Lithuanian cities (back to 1940), computes YTD anomalies, city rankings, per-city charts, runs an Apache Beam pipeline for regional monthly anomaly analysis, and validates output quality.",
      steps: [
        "fetch_weather",
        "analyze_anomalies",
        "beam_regional",
        "plot_charts",
        "quality_gate",
        "refresh_rag_context",
      ],
      tags: ["ERA5", "Anomaly detection", "Apache Beam", "Vega/Matplotlib"],
    },
    {
      name: `${cityName.toLowerCase()}_${monthName.toLowerCase()}_anomaly`,
      desc: `Historical ${yearsIncluded}-year deep-dive: extracts every ${monthName} 1\u2013N daily slice for ${cityName} (ERA5 1940\u2013present), computes year-by-year temperature anomaly and z-score vs the fixed 1991\u20132020 WMO baseline, generates a longitudinal trend chart.`,
      steps: [
        "fetch_vilnius_march",
        "analyze_anomalies",
        "plot_anomalies",
        "quality_gate",
        "refresh_rag_context",
      ],
      tags: [
        "ERA5",
        "Climate trend",
        "85-year window",
        "1991\u20132020 baseline",
      ],
    },
    {
      name: "llama_finetune",
      desc: "Manual LoRA fine-tune of llama3.2 on climate SFT examples generated from pipeline outputs. Trains with Hugging Face PEFT, logs adapter checkpoints and loss curves to MLflow.",
      steps: ["prepare_sft", "train_lora", "log_to_mlflow"],
      tags: ["LoRA", "Llama 3.2", "PEFT", "MLflow"],
    },
  ];

  return (
    <section className="card" id="pipeline-section">
      <h2>Pipeline Architecture</h2>
      <p className="section-desc">
        Four Airflow DAGs: daily weather anomaly, per-city monthly deep-dive,
        weekly model retrain, and a manual LoRA fine-tune. Each follows a
        fetch&nbsp;&rarr;&nbsp;analyze&nbsp;&rarr;&nbsp;plot&nbsp;&rarr;&nbsp;quality-gate
        pattern.
      </p>
      <div className="pipeline-grid">
        {dags.map((dag) => (
          <div key={dag.name} className="pipeline-card">
            <h3 className="pipeline-name">{dag.name}</h3>
            <p className="pipeline-desc">{dag.desc}</p>
            <div className="pipeline-steps">
              {dag.steps.map((step, i) => (
                <span key={step}>
                  {i > 0 && <span className="step-arrow">&rarr;</span>}
                  <span className="step">{step}</span>
                </span>
              ))}
            </div>
            <p className="tags">
              {dag.tags.map((tag) => (
                <span key={tag}>{tag}</span>
              ))}
            </p>
          </div>
        ))}
      </div>
    </section>
  );
}
