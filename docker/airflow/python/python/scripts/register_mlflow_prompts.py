"""Register RAG system prompt in the MLflow Prompt Registry.

Run once (or whenever the prompt template changes):

    MLFLOW_TRACKING_URI=http://localhost:5000 python python/scripts/register_mlflow_prompts.py

The script is idempotent: it creates a new prompt version and sets the
@champion alias, so re-running it always promotes the latest template.
"""

from __future__ import annotations

import os
import sys

import mlflow
from mlflow import MlflowClient

TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
PROMPT_NAME = "rag-system-prompt"

RAG_PROMPT_TEMPLATE = (
    "You are a climate dashboard assistant for Lithuania. "
    "Answer using only the provided context. "
    "When the context contains climate model forecast facts (baseline temperature, year-to-date bias, adjusted estimate), "
    "explain the reasoning conversationally: what the historical average is, how this year is trending, "
    "and what that implies for the estimate — like a knowledgeable friend, not a weather app.\n"
    "If the answer is not in context, say so briefly. Keep the answer concise.\n\n"
    "Question: {{question}}\n\n"
    "Context:\n{{context}}\n"
)


def main() -> None:
    mlflow.set_tracking_uri(TRACKING_URI)
    client = MlflowClient()

    # Create the prompt entity (no-op if it already exists)
    try:
        client.create_prompt(
            name=PROMPT_NAME,
            description="System prompt for the climate RAG pipeline (Ollama/llama3.2)",
            tags={"task": "rag", "provider": "ollama"},
        )
        print(f"Created prompt '{PROMPT_NAME}'")
    except Exception as exc:
        if "already exists" in str(exc).lower() or "RESOURCE_ALREADY_EXISTS" in str(exc):
            print(f"Prompt '{PROMPT_NAME}' already exists — adding new version")
        else:
            print(f"WARNING: create_prompt raised: {exc}")

    # Register a new version
    try:
        pv = client.create_prompt_version(
            name=PROMPT_NAME,
            template=RAG_PROMPT_TEMPLATE,
            description="Initial RAG system prompt",
            tags={"llm": "llama3.2:3b"},
        )
        version = pv.version
        print(f"Registered '{PROMPT_NAME}' version {version}")
    except Exception as exc:
        print(f"ERROR creating prompt version: {exc}", file=sys.stderr)
        sys.exit(1)

    # Promote to @champion
    try:
        client.set_prompt_alias(name=PROMPT_NAME, alias="champion", version=version)
        print(f"Set @champion -> version {version}")
    except Exception as exc:
        print(f"ERROR setting alias: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"\nDone. Load with: mlflow.load_prompt('prompts:/{PROMPT_NAME}@champion')")


if __name__ == "__main__":
    main()
