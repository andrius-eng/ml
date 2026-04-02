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
            Trainer,
            TrainingArguments,
            default_data_collator,
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
        "Trainer": Trainer,
        "TrainingArguments": TrainingArguments,
        "default_data_collator": default_data_collator,
    }


def format_prompt(row: dict) -> str:
    instruction = row.get("instruction", "")
    inp = row.get("input", "")
    return (
        "### Instruction:\n"
        f"{instruction}\n\n"
        "### Input:\n"
        f"{inp}\n\n"
        "### Response:\n"
    )


def format_response(row: dict) -> str:
    return row.get("output", "")


def format_example(row: dict) -> str:
    return f"{format_prompt(row)}{format_response(row)}"


def tokenize_supervised_example(row: dict, tokenizer, max_length: int) -> dict:
    prompt_text = format_prompt(row)
    response_text = format_response(row)

    prompt_ids_full = tokenizer(prompt_text, add_special_tokens=False)["input_ids"]
    response_ids = tokenizer(response_text, add_special_tokens=False)["input_ids"]
    if tokenizer.eos_token_id is not None:
        response_ids = response_ids + [tokenizer.eos_token_id]

    token_budget = max_length
    input_ids: list[int] = []
    labels: list[int] = []

    if tokenizer.bos_token_id is not None:
        input_ids.append(tokenizer.bos_token_id)
        labels.append(-100)
        token_budget -= 1

    if token_budget <= 0:
        raise ValueError("max_length must be at least 2 for supervised fine-tuning")

    if len(response_ids) >= token_budget:
        response_ids = response_ids[:token_budget]
        if tokenizer.eos_token_id is not None:
            response_ids[-1] = tokenizer.eos_token_id
        prompt_ids = []
    else:
        prompt_budget = token_budget - len(response_ids)
        prompt_ids = prompt_ids_full[:prompt_budget]

    input_ids.extend(prompt_ids)
    labels.extend([-100] * len(prompt_ids))
    input_ids.extend(response_ids)
    labels.extend(response_ids.copy())

    attention_mask = [1] * len(input_ids)
    pad_length = max_length - len(input_ids)
    if pad_length > 0:
        input_ids.extend([tokenizer.pad_token_id] * pad_length)
        attention_mask.extend([0] * pad_length)
        labels.extend([-100] * pad_length)

    if not any(label != -100 for label in labels):
        raise ValueError("Example lost all response tokens; increase max_length or shorten inputs")

    return {
        "input_ids": input_ids,
        "attention_mask": attention_mask,
        "labels": labels,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="LoRA fine-tune Llama on DAG SFT data")
    parser.add_argument("--train-jsonl", type=str, default="python/output/llm/sft_train.jsonl")
    parser.add_argument("--eval-jsonl", type=str, default="python/output/llm/sft_eval.jsonl")
    parser.add_argument("--base-model", type=str, default="distilgpt2")
    parser.add_argument("--output-dir", type=str, default="python/output/llm/lora-adapter")
    parser.add_argument("--max-length", type=int, default=256)
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--learning-rate", type=float, default=5e-4)
    args = parser.parse_args()

    deps = _load_deps()
    load_dataset = deps["load_dataset"]
    LoraConfig = deps["LoraConfig"]
    TaskType = deps["TaskType"]
    get_peft_model = deps["get_peft_model"]
    AutoModelForCausalLM = deps["AutoModelForCausalLM"]
    AutoTokenizer = deps["AutoTokenizer"]
    Trainer = deps["Trainer"]
    TrainingArguments = deps["TrainingArguments"]
    default_data_collator = deps["default_data_collator"]
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
    if tokenizer.pad_token_id is None:
        raise RuntimeError("Tokenizer must define a pad token for supervised fine-tuning")

    def tok(row):
        return tokenize_supervised_example(row, tokenizer, args.max_length)

    train_ds = dataset["train"].map(tok, remove_columns=dataset["train"].column_names)
    eval_ds = dataset["eval"].map(tok, remove_columns=dataset["eval"].column_names)

    try:
        model = AutoModelForCausalLM.from_pretrained(
            args.base_model,
            low_cpu_mem_usage=True,
        )
    except TypeError:
        model = AutoModelForCausalLM.from_pretrained(args.base_model)
    if hasattr(model.config, "use_cache"):
        model.config.use_cache = False

    # Pick LoRA target modules based on model architecture.
    # GPT-2 family uses c_attn/c_proj; Llama/Mistral/TinyLlama use q/k/v/o_proj.
    _model_type = getattr(model.config, "model_type", "").lower()
    if _model_type in ("gpt2",):
        _target_modules = ["c_attn", "c_proj"]
    elif _model_type in ("llama", "mistral", "tinyllama", "qwen2", "phi"):
        _target_modules = ["q_proj", "k_proj", "v_proj", "o_proj"]
    else:
        # Fall back to all linear layers — works across architectures in PEFT ≥0.7
        _target_modules = "all-linear"

    lora_cfg = LoraConfig(
        task_type=TaskType.CAUSAL_LM,
        r=16,
        lora_alpha=32,
        lora_dropout=0.05,
        target_modules=_target_modules,
    )
    model = get_peft_model(model, lora_cfg)
    if hasattr(model, "gradient_checkpointing_enable"):
        model.gradient_checkpointing_enable()
    if hasattr(model, "enable_input_require_grads"):
        model.enable_input_require_grads()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    train_args = TrainingArguments(
        output_dir=str(output_dir),
        num_train_epochs=args.epochs,
        per_device_train_batch_size=args.batch_size,
        per_device_eval_batch_size=args.batch_size,
        learning_rate=args.learning_rate,
        evaluation_strategy="no",
        save_strategy="no",
        logging_steps=10,
        fp16=False,
        report_to=[],
        seed=42,
        dataloader_pin_memory=False,
        gradient_checkpointing=True,
    )

    trainer = Trainer(
        model=model,
        args=train_args,
        train_dataset=train_ds,
        tokenizer=tokenizer,
        data_collator=default_data_collator,
    )
    trainer.train()

    model.save_pretrained(str(output_dir))
    tokenizer.save_pretrained(str(output_dir))
    print(f"LoRA adapter saved to {output_dir}")


if __name__ == "__main__":
    main()
