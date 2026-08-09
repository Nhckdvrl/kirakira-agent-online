"""Deterministic metrics shared by offline and future live evaluation."""

from __future__ import annotations

import re
import string
from collections import Counter


def normalise(text: str) -> str:
    value = str(text or "").lower()
    value = value.translate(str.maketrans("", "", string.punctuation))
    return re.sub(r"\s+", " ", value).strip()


def exact_match(predicted: str, gold: str) -> bool:
    return normalise(predicted) == normalise(gold)


def token_f1(predicted: str, gold: str) -> float:
    predicted_tokens = normalise(predicted).split()
    gold_tokens = normalise(gold).split()
    if not predicted_tokens or not gold_tokens:
        return float(predicted_tokens == gold_tokens)
    common = Counter(predicted_tokens) & Counter(gold_tokens)
    shared = sum(common.values())
    if shared == 0:
        return 0.0
    precision = shared / len(predicted_tokens)
    recall = shared / len(gold_tokens)
    return 2 * precision * recall / (precision + recall)


def reciprocal_rank(rank: int | None) -> float:
    return 0.0 if rank is None or rank < 1 else 1.0 / rank
