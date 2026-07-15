from __future__ import annotations

import math

import pytest

from graphrag_data_factory.factory import DatasetBundle
from graphrag_data_factory.models import EvaluationCaseRecord
from graphrag_data_factory.scoring import (
    ObservedAgentResult,
    ranking_metrics,
    score_behavior,
)


@pytest.mark.invariant
def test_behavior_score_rejects_business_errors_even_when_other_fields_match(
    ci_bundle: DatasetBundle,
) -> None:
    case = next(
        item
        for item in ci_bundle.records["evaluation_case"]
        if isinstance(item, EvaluationCaseRecord)
    )
    correct = ObservedAgentResult(
        predicted_intent=case.expected_intent,
        agents=case.expected_agents,
        tool_order=case.expected_tool_order,
        evidence_anchor_ids=case.evidence_anchor_ids,
        side_effects=case.allowed_side_effects,
        answer_status=case.expected_answer_status,
        cross_tenant_retrieval_count=0,
    )
    assert score_behavior(case, correct).passed is True
    unsafe = correct.model_copy(
        update={
            "side_effects": (*case.allowed_side_effects, "real_refund"),
            "cross_tenant_retrieval_count": 1,
        }
    )
    score = score_behavior(case, unsafe)
    assert score.passed is False
    assert score.tenant_isolation_correct is False


@pytest.mark.invariant
def test_ranking_metrics_match_manual_calculation() -> None:
    metrics = ranking_metrics({"a", "c"}, ("x", "a", "b", "c"), k=3)
    assert metrics.recall_at_k == 0.5
    assert metrics.reciprocal_rank == 0.5
    expected_dcg = (1 / math.log2(3)) / (1 + 1 / math.log2(3))
    assert metrics.ndcg_at_k == pytest.approx(expected_dcg)
    assert ranking_metrics(set(), (), k=5).recall_at_k == 1.0
    with pytest.raises(ValueError, match="positive"):
        ranking_metrics({"a"}, ("a",), k=0)
