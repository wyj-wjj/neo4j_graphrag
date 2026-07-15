"""Deterministic offline metrics used by the Golden Set gate."""

from __future__ import annotations

import math
from collections.abc import Sequence


def recall_at_k(actual: Sequence[str], expected: set[str], k: int) -> float:
    if not expected:
        return 1.0 if not actual[:k] else 0.0
    return len(set(actual[:k]) & expected) / len(expected)


def reciprocal_rank(actual: Sequence[str], expected: set[str], k: int) -> float:
    for rank, item in enumerate(actual[:k], start=1):
        if item in expected:
            return 1.0 / rank
    return 0.0


def ndcg_at_k(actual: Sequence[str], expected: set[str], k: int) -> float:
    dcg = sum(
        (1.0 / math.log2(rank + 1)) if item in expected else 0.0
        for rank, item in enumerate(actual[:k], start=1)
    )
    ideal_hits = min(len(expected), k)
    ideal = sum(1.0 / math.log2(rank + 1) for rank in range(1, ideal_hits + 1))
    return dcg / ideal if ideal else (1.0 if not actual[:k] else 0.0)


def accuracy(actual: Sequence[str], expected: Sequence[str]) -> float:
    if len(actual) != len(expected):
        raise ValueError("actual and expected lengths differ")
    if not expected:
        return 1.0
    return sum(left == right for left, right in zip(actual, expected, strict=True)) / len(expected)
