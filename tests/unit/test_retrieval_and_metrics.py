from __future__ import annotations

import pytest

from graphrag.domain.errors import ValidationError
from graphrag.domain.models import ChatMessage, Evidence, SearchCandidate, utc_now
from graphrag.retrieval.evaluation import accuracy, ndcg_at_k, recall_at_k, reciprocal_rank
from graphrag.retrieval.pipeline import GraphRAGPipeline
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


def test_conflicting_effective_evidence_is_detected_before_generation() -> None:
    common = {
        "document_title": "同名政策",
        "document_version": 1,
        "updated_at": utc_now(),
        "source_location": "page:1",
        "score": 1.0,
        "sources": ("keyword",),
        "conflict_group": "title:同名政策",
    }
    first = Evidence(
        chunk_id="chunk-1",
        document_id="document-1",
        content="保修期两年",
        **common,
    )
    second = Evidence(
        chunk_id="chunk-2",
        document_id="document-2",
        content="保修期一年",
        **common,
    )
    assert GraphRAGPipeline._conflict_groups((first, second)) == ("title:同名政策",)
    assert GraphRAGPipeline._conflict_groups((first, first)) == ()
