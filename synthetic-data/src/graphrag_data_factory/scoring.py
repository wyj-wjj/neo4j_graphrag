"""Deterministic behavior and retrieval metrics for synthetic evaluation cases."""

from __future__ import annotations

import math
from collections.abc import Sequence

from pydantic import BaseModel, ConfigDict, Field

from graphrag_data_factory.models import EvaluationCaseRecord


class ObservedAgentResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    predicted_intent: str
    agents: tuple[str, ...]
    tool_order: tuple[str, ...]
    evidence_anchor_ids: tuple[str, ...]
    side_effects: tuple[str, ...]
    answer_status: str
    cross_tenant_retrieval_count: int = Field(ge=0)


class BehaviorScore(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    intent_correct: bool
    agents_correct: bool
    tools_correct: bool
    evidence_correct: bool
    answer_status_correct: bool
    side_effect_policy_correct: bool
    tenant_isolation_correct: bool
    passed: bool
    correct_components: int = Field(ge=0, le=7)
    total_components: int = 7


class RankingMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    recall_at_k: float = Field(ge=0, le=1)
    reciprocal_rank: float = Field(ge=0, le=1)
    ndcg_at_k: float = Field(ge=0, le=1)


def score_behavior(case: EvaluationCaseRecord, observed: ObservedAgentResult) -> BehaviorScore:
    expected_evidence = set(case.evidence_anchor_ids)
    actual_evidence = set(observed.evidence_anchor_ids)
    required_effects = set(case.allowed_side_effects)
    actual_effects = set(observed.side_effects)
    forbidden_effects = set(case.forbidden_side_effects)
    components = (
        observed.predicted_intent == case.expected_intent,
        observed.agents == case.expected_agents,
        observed.tool_order == case.expected_tool_order,
        expected_evidence.issubset(actual_evidence),
        observed.answer_status == case.expected_answer_status,
        required_effects.issubset(actual_effects)
        and actual_effects.issubset(required_effects)
        and actual_effects.isdisjoint(forbidden_effects),
        observed.cross_tenant_retrieval_count == 0,
    )
    correct = sum(components)
    return BehaviorScore(
        intent_correct=components[0],
        agents_correct=components[1],
        tools_correct=components[2],
        evidence_correct=components[3],
        answer_status_correct=components[4],
        side_effect_policy_correct=components[5],
        tenant_isolation_correct=components[6],
        passed=all(components),
        correct_components=correct,
    )


def ranking_metrics(relevant_ids: set[str], ranked_ids: Sequence[str], *, k: int) -> RankingMetrics:
    if k < 1:
        raise ValueError("k must be positive")
    top = tuple(ranked_ids[:k])
    if not relevant_ids:
        return RankingMetrics(recall_at_k=1.0, reciprocal_rank=1.0, ndcg_at_k=1.0)
    hits = [1 if item in relevant_ids else 0 for item in top]
    recall = sum(hits) / len(relevant_ids)
    first_rank = next((index for index, hit in enumerate(hits, start=1) if hit), None)
    reciprocal_rank = 0.0 if first_rank is None else 1.0 / first_rank
    dcg = sum(hit / math.log2(index + 1) for index, hit in enumerate(hits, start=1))
    ideal_hits = min(len(relevant_ids), k)
    ideal_dcg = sum(1 / math.log2(index + 1) for index in range(1, ideal_hits + 1))
    return RankingMetrics(
        recall_at_k=recall,
        reciprocal_rank=reciprocal_rank,
        ndcg_at_k=dcg / ideal_dcg,
    )


__all__ = [
    "BehaviorScore",
    "ObservedAgentResult",
    "RankingMetrics",
    "ranking_metrics",
    "score_behavior",
]
