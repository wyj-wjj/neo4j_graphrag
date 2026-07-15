"""Reciprocal Rank Fusion that never compares raw scores across retrievers."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence

from graphrag.domain.models import SearchCandidate


def reciprocal_rank_fusion(
    rankings: Sequence[Sequence[SearchCandidate]], *, k: int = 60
) -> list[tuple[str, float, tuple[str, ...]]]:
    if k <= 0:
        raise ValueError("RRF k must be positive")
    scores: dict[str, float] = defaultdict(float)
    sources: dict[str, set[str]] = defaultdict(set)
    for ranking in rankings:
        seen: set[str] = set()
        for fallback_rank, candidate in enumerate(ranking, start=1):
            if candidate.chunk_id in seen:
                continue
            seen.add(candidate.chunk_id)
            rank = candidate.rank or fallback_rank
            scores[candidate.chunk_id] += 1.0 / (k + rank)
            sources[candidate.chunk_id].add(candidate.source)
    return sorted(
        ((chunk_id, score, tuple(sorted(sources[chunk_id]))) for chunk_id, score in scores.items()),
        key=lambda item: (-item[1], item[0]),
    )
