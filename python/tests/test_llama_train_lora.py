"""Tests for response-only LoRA supervision helpers."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from llama_train_lora import format_prompt, format_response, tokenize_supervised_example


class DummyTokenizer:
    bos_token_id = 101
    eos_token_id = 102
    pad_token_id = 0

    def __call__(self, text, add_special_tokens=False):
        del add_special_tokens
        input_ids = [ord(char) + 3 for char in text]
        return {"input_ids": input_ids, "attention_mask": [1] * len(input_ids)}


def test_tokenize_supervised_example_masks_prompt_and_padding_tokens():
    row = {
        "instruction": "Summarize the anomaly.",
        "input": '{"city": "Vilnius", "z_score": 1.25}',
        "output": "Vilnius is warmer than normal.",
    }
    tokenizer = DummyTokenizer()

    encoded = tokenize_supervised_example(row, tokenizer, max_length=160)
    prompt_len = 1 + len(tokenizer(format_prompt(row))["input_ids"])

    assert len(encoded["input_ids"]) == 160
    assert encoded["labels"][:prompt_len] == [-100] * prompt_len
    assert any(label != -100 for label in encoded["labels"])
    assert all(
        label == -100
        for label, mask in zip(encoded["labels"], encoded["attention_mask"])
        if mask == 0
    )


def test_tokenize_supervised_example_keeps_response_when_prompt_is_truncated():
    row = {
        "instruction": "A" * 120,
        "input": "B" * 120,
        "output": "OK",
    }
    tokenizer = DummyTokenizer()

    encoded = tokenize_supervised_example(row, tokenizer, max_length=10)
    expected_response = tokenizer(format_response(row))["input_ids"] + [tokenizer.eos_token_id]
    supervised_labels = [label for label in encoded["labels"] if label != -100]

    assert supervised_labels
    assert supervised_labels == expected_response[:len(supervised_labels)]