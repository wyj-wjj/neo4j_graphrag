from __future__ import annotations

import pytest

from graphrag.domain.errors import ValidationError
from graphrag.domain.models import ChatMessage, SearchCandidate
from graphrag.retrieval.evaluation import accuracy, ndcg_at_k, recall_at_k, reciprocal_rank
from graphrag.retrieval.query import normalize_query, rewrite_query
from graphrag.retrieval.rrf import reciprocal_rank_fusion


def candidate(chunk_id: str, source: str, rank: int, score: float) -> SearchCandidate:
    return SearchCandidate(
        chunk_id=chunk_id,
        source=source,  # type: ignore[arg-type]
        rank=rank,
        score=score,
    )


def test_rrf_uses_rank_deduplicates_and_is_deterministic() -> None:
    dense = [candidate("a", "dense", 1, 0.1), candidate("b", "dense", 2, 999)]
    graph = [candidate("b", "graph", 1, -500), candidate("a", "graph", 2, 1)]
    fused = reciprocal_rank_fusion([dense, graph], k=60)
    assert [item[0] for item in fused] == ["a", "b"]
    assert fused[0][2] == ("dense", "graph")
    with pytest.raises(ValueError):
        reciprocal_rank_fusion([dense], k=0)


def test_query_normalization_and_conservative_rewrite() -> None:
    assert normalize_query("  Ａ  policy ") == "A policy"
    with pytest.raises(ValidationError):
        normalize_query("!!!")
    rewritten, confidence = rewrite_query(
        "这个怎么办", [ChatMessage(role="user", content="保修政策")]
    )
    assert "保修政策" in rewritten
    assert confidence == 0.8


def test_offline_metrics_match_hand_calculation() -> None:
    actual = ["x", "a", "b"]
    expected = {"a", "b"}
    assert recall_at_k(actual, expected, 2) == 0.5
    assert reciprocal_rank(actual, expected, 3) == 0.5
    assert 0 < ndcg_at_k(actual, expected, 3) < 1
    assert accuracy(["a", "b"], ["a", "x"]) == 0.5
    with pytest.raises(ValueError):
        accuracy(["a"], ["a", "b"])
