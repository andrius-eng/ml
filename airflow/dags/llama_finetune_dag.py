"""Airflow DAG for local Llama LoRA fine-tuning on DAG artifacts.

Manual DAG to:
1) build SFT dataset from existing pipeline outputs
2) run LoRA fine-tuning
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

DAG_DIR = Path(__file__).resolve().parent
DEFAULT_PROJECT_ROOT = DAG_DIR.parents[1] if len(DAG_DIR.parents) >= 2 else Path("/opt/airflow/project")
PROJECT_ROOT = Path(os.environ.get("ML_PROJECT_ROOT", str(DEFAULT_PROJECT_ROOT))).resolve()
PYTHON_BIN = os.environ.get("TRAIN_PYTHON_BIN", "python")
if PYTHON_BIN != "python" and not Path(PYTHON_BIN).exists():
    PYTHON_BIN = "python"

PREP_SCRIPT = PROJECT_ROOT / "python" / "llama_prepare_sft.py"
TRAIN_SCRIPT = PROJECT_ROOT / "python" / "llama_train_lora.py"
LLM_OUTPUT_DIR = PROJECT_ROOT / "python" / "output" / "llm"
# distilgpt2 (82M params) trains in ~1-2 min on CPU with 8 examples.
# Override with LLAMA_BASE_MODEL env var for a larger model.
BASE_MODEL = os.environ.get("LLAMA_BASE_MODEL", "distilgpt2")


def project_python_command(*args: str) -> str:
    quoted_args = " ".join(f'"{arg}"' for arg in args)
    return f'"{PYTHON_BIN}" {quoted_args}'


with DAG(
    dag_id="llama_dag_finetune",
    default_args=DEFAULT_ARGS,
    description="Prepare SFT data from DAG artifacts and fine-tune a local Llama LoRA adapter",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["llm", "lora", "dag-artifacts"],
) as dag:
    prepare_sft = BashOperator(
        task_id="prepare_sft_dataset",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{PREP_SCRIPT}"\n'
            f'{project_python_command(str(PREP_SCRIPT), "--output-dir", str(PROJECT_ROOT / "python" / "output"), "--train-jsonl", str(LLM_OUTPUT_DIR / "sft_train.jsonl"), "--eval-jsonl", str(LLM_OUTPUT_DIR / "sft_eval.jsonl"))}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN},
    )

    train_lora = BashOperator(
        task_id="train_lora_adapter",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            "set -euo pipefail\n"
            f'test -f "{TRAIN_SCRIPT}"\n'
            f'{project_python_command(str(TRAIN_SCRIPT), "--train-jsonl", str(LLM_OUTPUT_DIR / "sft_train.jsonl"), "--eval-jsonl", str(LLM_OUTPUT_DIR / "sft_eval.jsonl"), "--base-model", BASE_MODEL, "--output-dir", str(LLM_OUTPUT_DIR / "lora-adapter"), "--max-length", "256")}'
        ),
        env={"ML_PROJECT_ROOT": str(PROJECT_ROOT), "TRAIN_PYTHON_BIN": PYTHON_BIN, "LLAMA_BASE_MODEL": BASE_MODEL},
    )

    prepare_sft >> train_lora
