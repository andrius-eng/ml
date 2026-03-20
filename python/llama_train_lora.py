"""LoRA fine-tuning entrypoint for local Llama on DAG-derived SFT data.

This script expects JSONL rows with fields: instruction, input, output.
"""

from __future__ import annotations

import argparse
from pathlib import Path


def _parse_major_minor(version: str) -> tuple[int, int]:
    core = version.split("+", 1)[0]
    parts = core.split(".")
    return int(parts[0]), int(parts[1])


def _load_deps():
    try:
        import torch  # type: ignore
        from datasets import load_dataset  # type: ignore
        from peft import LoraConfig, TaskType, get_peft_model  # type: ignore
        from transformers import (  # type: ignore
            AutoModelForCausalLM,
            AutoTokenizer,
            DataCollatorForLanguageModeling,
            Trainer,
            TrainingArguments,
        )
    except Exception as exc:
        raise RuntimeError(
            "LoRA training dependencies are unavailable or incompatible in this runtime. "
            "Rebuild/install with python/requirements-llm-train.txt. "
            f"Original import error: {exc}"
        ) from exc

    return {
        "torch": torch,
        "load_dataset": load_dataset,
        "LoraConfig": LoraConfig,
        "TaskType": TaskType,
        "get_peft_model": get_peft_model,
        "AutoModelForCausalLM": AutoModelForCausalLM,
        "AutoTokenizer": AutoTokenizer,
        "DataCollatorForLanguageModeling": DataCollatorForLanguageModeling,
        "Trainer": Trainer,
        "TrainingArguments": TrainingArguments,
    }


def format_example(row: dict) -> str:
    instruction = row.get("instruction", "")
    inp = row.get("input", "")
    out = row.get("output", "")
    return (
        "### Instruction:\n"
        f"{instruction}\n\n"
        "### Input:\n"
        f"{inp}\n\n"
        "### Response:\n"
        f"{out}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="LoRA fine-tune Llama on DAG SFT data")
    parser.add_argument("--train-jsonl", type=str, default="python/output/llm/sft_train.jsonl")
    parser.add_argument("--eval-jsonl", type=str, default="python/output/llm/sft_eval.jsonl")
    parser.add_argument("--base-model", type=str, default="distilgpt2")
    parser.add_argument("--output-dir", type=str, default="python/output/llm/lora-adapter")
    parser.add_argument("--max-length", type=int, default=256)
    parser.add_argument("--epochs", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--learning-rate", type=float, default=2e-4)
    args = parser.parse_args()

    deps = _load_deps()
    load_dataset = deps["load_dataset"]
    LoraConfig = deps["LoraConfig"]
    TaskType = deps["TaskType"]
    get_peft_model = deps["get_peft_model"]
    AutoModelForCausalLM = deps["AutoModelForCausalLM"]
    AutoTokenizer = deps["AutoTokenizer"]
    DataCollatorForLanguageModeling = deps["DataCollatorForLanguageModeling"]
    Trainer = deps["Trainer"]
    TrainingArguments = deps["TrainingArguments"]
    torch = deps["torch"]

    if _parse_major_minor(torch.__version__) < (2, 2):
        raise RuntimeError(
            f"PyTorch {torch.__version__} is too old for LoRA training in this project. "
            "Expected torch 2.2.x or newer from python/requirements-airflow-runtime.txt."
        )

    train_path = Path(args.train_jsonl)
    eval_path = Path(args.eval_jsonl)
    if not train_path.exists() or not eval_path.exists():
        raise FileNotFoundError(
            "SFT JSONL files not found. Run python/llama_prepare_sft.py first."
        )

    dataset = load_dataset(
        "json",
        data_files={"train": str(train_path), "eval": str(eval_path)},
    )

    tokenizer = AutoTokenizer.from_pretrained(args.base_model, use_fast=True)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    def tok(row):
        # row is a single example dict when batched=False (the default)
        text = format_example(row)
        encoded = tokenizer(
            text,
            truncation=True,
            max_length=args.max_length,
            padding="max_length",
        )
        encoded["labels"] = encoded["input_ids"].copy()
        return encoded

    train_ds = dataset["train"].map(tok, remove_columns=dataset["train"].column_names)
    eval_ds = dataset["eval"].map(tok, remove_columns=dataset["eval"].column_names)

    model = AutoModelForCausalLM.from_pretrained(args.base_model)
    lora_cfg = LoraConfig(
        task_type=TaskType.CAUSAL_LM,
        r=16,
        lora_alpha=32,
        lora_dropout=0.05,
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj"],
    )
    model = get_peft_model(model, lora_cfg)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    train_args = TrainingArguments(
        output_dir=str(output_dir),
        num_train_epochs=args.epochs,
        per_device_train_batch_size=args.batch_size,
        per_device_eval_batch_size=args.batch_size,
        learning_rate=args.learning_rate,
        evaluation_strategy="epoch",
        save_strategy="epoch",
        logging_steps=10,
        fp16=False,
        report_to=[],
    )

    trainer = Trainer(
        model=model,
        args=train_args,
        train_dataset=train_ds,
        eval_dataset=eval_ds,
        tokenizer=tokenizer,
        data_collator=DataCollatorForLanguageModeling(tokenizer=tokenizer, mlm=False),
    )
    trainer.train()

    model.save_pretrained(str(output_dir))
    tokenizer.save_pretrained(str(output_dir))
    print(f"LoRA adapter saved to {output_dir}")


if __name__ == "__main__":
    main()
