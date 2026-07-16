from __future__ import annotations

from datetime import timedelta

import pytest

from graphrag.agents.consolidation import DeterministicResultConsolidator
from graphrag.domain.errors import ValidationError
from graphrag.domain.models import AgentIntent, AgentOutcome, Citation, utc_now


def citation(citation_id: str) -> Citation:
    return Citation(
        citation_id=citation_id,
        chunk_id=f"chunk-{citation_id}",
        document_id="document-1",
        document_title="Policy",
        document_version=1,
        updated_at=utc_now(),
        source_location="page 1",
    )


def outcome(
    intent: AgentIntent,
    answer: str,
    *,
    authority_rank: int = 100,
    updated_offset: int | None = 0,
    citations: tuple[Citation, ...] = (),
) -> AgentOutcome:
    updated_at = None if updated_offset is None else utc_now() + timedelta(seconds=updated_offset)
    return AgentOutcome(
        intent=intent,
        answer=answer,
        authority_rank=authority_rank,
        evidence_updated_at=updated_at,
        citations=citations,
    )


def test_single_outcome_passes_through_with_auditable_basis() -> None:
    source = outcome(AgentIntent.KB, "确定答案", citations=(citation("C1"),))

    result = DeterministicResultConsolidator().consolidate([source])

    assert result.answer == source.answer
    assert result.citations == source.citations
    assert result.arbitration_basis == "single"
    assert result.consolidation_version == "deterministic-consolidator-v1"


def test_higher_authority_wins_without_leaking_losing_citations() -> None:
    low = outcome(
        AgentIntent.FAQ,
        "旧的低权威答案",
        authority_rank=50,
        citations=(citation("LOW"),),
    )
    high = outcome(
        AgentIntent.KB,
        "权威答案",
        authority_rank=200,
        citations=(citation("HIGH"),),
    )

    result = DeterministicResultConsolidator().consolidate([low, high])

    assert result.answer == "权威答案"
    assert result.arbitration_basis == "authority"
    assert tuple(item.citation_id for item in result.citations) == ("HIGH",)


@pytest.mark.parametrize("reverse", [False, True])
def test_newer_evidence_wins_when_authority_is_equal(reverse: bool) -> None:
    old = outcome(AgentIntent.KB, "旧答案", updated_offset=0)
    new = outcome(AgentIntent.FAQ, "新答案", updated_offset=60)
    inputs = [new, old] if reverse else [old, new]

    result = DeterministicResultConsolidator().consolidate(inputs)

    assert result.answer == "新答案"
    assert result.arbitration_basis == "recency"
    assert result.requires_clarification is False


def test_equal_answers_are_combined_with_stable_citation_deduplication() -> None:
    shared = citation("SHARED")
    left = outcome(
        AgentIntent.KB,
        "一致答案",
        updated_offset=None,
        citations=(shared, citation("KB")),
    )
    right = outcome(
        AgentIntent.FAQ,
        "一致答案",
        updated_offset=None,
        citations=(shared, citation("FAQ")),
    )

    result = DeterministicResultConsolidator().consolidate([left, right])

    assert result.arbitration_basis == "combined"
    assert tuple(item.citation_id for item in result.citations) == ("SHARED", "KB", "FAQ")


def test_equal_unresolvable_conflict_requires_clarification() -> None:
    left = outcome(AgentIntent.KB, "答案 A", updated_offset=None)
    right = outcome(AgentIntent.FAQ, "答案 B", updated_offset=None)

    result = DeterministicResultConsolidator().consolidate([left, right])

    assert result.requires_clarification is True
    assert result.arbitration_basis == "conflict"
    assert result.citations == ()


def test_complementary_order_and_logistics_are_combined_in_stable_order() -> None:
    logistics = outcome(AgentIntent.LOGISTICS, "物流状态", updated_offset=None)
    order = outcome(AgentIntent.ORDER, "订单状态", updated_offset=None)

    result = DeterministicResultConsolidator().consolidate_complementary([logistics, order])

    assert result.answer == "订单状态\n物流状态"
    assert result.intents == (AgentIntent.ORDER, AgentIntent.LOGISTICS)
    assert result.arbitration_basis == "combined"


@pytest.mark.parametrize("count", [0, 3])
def test_consolidator_enforces_hard_expert_bound(count: int) -> None:
    outcomes = [outcome(AgentIntent.KB, f"答案 {index}") for index in range(count)]

    with pytest.raises(ValidationError):
        DeterministicResultConsolidator().consolidate(outcomes)
