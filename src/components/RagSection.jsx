import { useState } from "react";
import { formatSource } from "../utils";

function RagCard({ item }) {
  return (
    <article className="rag-card">
      <h3 className="rag-question">{item.question}</h3>
      <p className="rag-answer">{item.answer}</p>
      {item.interpretation && (
        <p className="rag-interpretation">{item.interpretation}</p>
      )}
      <div className="rag-sources">
        {(item.sources || []).map((source, i) => (
          <span key={i}>
            {formatSource(source)} ({source.score.toFixed(2)})
          </span>
        ))}
      </div>
    </article>
  );
}

async function queryRag(question) {
  const t0 = Date.now();
  const response = await fetch(
    `/api/rag/query?q=${encodeURIComponent(question)}`,
  );
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const result = await response.json();
  return { result, elapsed: ((Date.now() - t0) / 1000).toFixed(1) };
}

function RagQueryResult({ result, elapsed, error }) {
  if (error) {
    if (
      error.includes("502") ||
      error.includes("503") ||
      error.includes("fetch")
    ) {
      return (
        <p style={{ color: "var(--muted)" }}>
          RAG API is not running. Start the serve container or run:
          <br />
          <code>
            uv run uvicorn --app-dir python serve:app --host 127.0.0.1 --port
            8000
          </code>
        </p>
      );
    }
    return <p>{`Error: ${error}`}</p>;
  }
  if (!result) return null;
  return (
    <>
      <div className="rag-result-header">
        <h3 className="rag-question">{result.question}</h3>
        <span className="rag-badge-ollama">Ollama &middot; {elapsed}s</span>
      </div>
      <p className="rag-answer">{result.answer}</p>
      {result.interpretation && (
        <div className="rag-interpretation">{result.interpretation}</div>
      )}
      <div className="rag-sources">
        {(result.sources || []).map((s, i) => (
          <span key={i}>
            {formatSource(s)}
            {s.score != null ? ` (${s.score.toFixed(2)})` : ""}
          </span>
        ))}
      </div>
    </>
  );
}

export function RagSection({ data }) {
  const rag = data.rag_demo;
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [queryResult, setQueryResult] = useState(null);
  const [queryElapsed, setQueryElapsed] = useState(null);
  const [queryError, setQueryError] = useState(null);

  // Dynamic chip labels come from the anomaly data
  const monthAnomaly = data.vilnius_month_anomaly;
  const cityName = monthAnomaly?.city || "Vilnius";
  const monthName = monthAnomaly?.month_name || "March";
  const chips = [
    "What will the temperature be tomorrow?",
    "Is Lithuania currently warmer or colder than normal?",
    `How unusual is this ${monthName} in ${cityName}?`,
    `What was the warmest ${monthName} on record in ${cityName}?`,
  ];

  async function handleSubmit(question) {
    if (!question.trim()) return;
    setLoading(true);
    setQueryResult(null);
    setQueryError(null);
    try {
      const { result, elapsed } = await queryRag(question);
      setQueryResult(result);
      setQueryElapsed(elapsed);
    } catch (err) {
      setQueryError(err.message);
    } finally {
      setLoading(false);
    }
  }

  const noRag =
    !rag || !Array.isArray(rag.questions) || rag.questions.length === 0;

  return (
    <>
      <section className="card" id="rag-section">
        <h2>Vector RAG Briefings</h2>
        <p className="section-desc">
          Airflow tasks publish summaries, rankings, and evaluation artifacts. A
          Qdrant-backed retrieval layer indexes them and assembles the briefings
          below from retrieved evidence.
        </p>
        <div className="rag-meta">
          {noRag
            ? "No retrieval briefings available yet."
            : `Collection ${rag.collection} \u00b7 ${rag.corpus_size} indexed documents \u00b7 Updated ${rag.generated_at}`}
        </div>
        {!noRag && (
          <div className="rag-grid">
            {rag.questions.map((item, i) => (
              <RagCard key={i} item={item} />
            ))}
          </div>
        )}
      </section>

      <section className="card" id="rag-query-section">
        <h2>Ask the Pipeline</h2>
        <p className="section-desc">
          Live query &mdash; the pipeline computes the facts, then Ollama
          (llama3.2) drafts the answer using the prompt template registered in
          MLflow.
        </p>
        <div className="rag-chips">
          {chips.map((chip) => (
            <button
              key={chip}
              className="rag-chip"
              type="button"
              onClick={() => {
                setInput(chip);
                handleSubmit(chip);
              }}
            >
              {chip}
            </button>
          ))}
        </div>
        <form
          className="rag-form"
          onSubmit={(e) => {
            e.preventDefault();
            handleSubmit(input);
          }}
          autoComplete="off"
        >
          <input
            className="rag-input"
            type="text"
            placeholder="Ask anything about Lithuanian climate\u2026"
            aria-label="Ask a question about the pipeline outputs"
            value={input}
            onChange={(e) => setInput(e.target.value)}
          />
          <button className="rag-submit" type="submit" disabled={loading}>
            Ask
          </button>
        </form>
        {(loading || queryResult || queryError) && (
          <div className="rag-query-result">
            {loading ? (
              <p className="rag-loading">
                <span className="rag-spinner" />
                Asking Ollama&hellip;
              </p>
            ) : (
              <RagQueryResult
                result={queryResult}
                elapsed={queryElapsed}
                error={queryError}
              />
            )}
          </div>
        )}
      </section>
    </>
  );
}
