"""Deterministic bounded result convergence reserved for compound plans."""

from __future__ import annotations

from collections.abc import Sequence

from graphrag.domain.errors import ValidationError
from graphrag.domain.models import AgentIntent, AgentOutcome, Citation, ConsolidatedOutcome


class DeterministicResultConsolidator:
    version = "deterministic-consolidator-v1"
    max_outcomes = 2

    def consolidate(self, outcomes: Sequence[AgentOutcome]) -> ConsolidatedOutcome:
        if not outcomes or len(outcomes) > self.max_outcomes:
            raise ValidationError("结果收敛只接受 1 至 2 个受控专家结果")
        if len(outcomes) == 1:
            item = outcomes[0]
            return ConsolidatedOutcome(
                answer=item.answer,
                intents=(item.intent,),
                citations=item.citations,
                arbitration_basis="single",
            )
        left, right = outcomes
        if left.authority_rank != right.authority_rank:
            selected = max(outcomes, key=lambda item: item.authority_rank)
            basis = "authority"
        elif (
            left.evidence_updated_at is not None
            and right.evidence_updated_at is not None
            and left.evidence_updated_at != right.evidence_updated_at
        ):
            selected = left if left.evidence_updated_at > right.evidence_updated_at else right
            basis = "recency"
        elif left.answer != right.answer:
            return ConsolidatedOutcome(
                answer="多个同级权威结果存在冲突，请补充信息或转人工确认。",
                intents=(left.intent, right.intent),
                requires_clarification=True,
                arbitration_basis="conflict",
            )
        else:
            selected = left
            basis = "combined"
        selected_citations = (
            self._deduplicate_citations(outcomes) if basis == "combined" else selected.citations
        )
        return ConsolidatedOutcome(
            answer=selected.answer,
            intents=tuple(dict.fromkeys(item.intent for item in outcomes)),
            citations=selected_citations,
            arbitration_basis=basis,
        )

    def consolidate_complementary(self, outcomes: Sequence[AgentOutcome]) -> ConsolidatedOutcome:
        if len(outcomes) != 2 or {item.intent for item in outcomes} != {
            AgentIntent.ORDER,
            AgentIntent.LOGISTICS,
        }:
            raise ValidationError("互补收敛仅允许订单查询与物流查询两个只读专家")
        ordered = sorted(
            outcomes,
            key=lambda item: (item.intent is not AgentIntent.ORDER, item.intent.value),
        )
        return ConsolidatedOutcome(
            answer="\n".join(item.answer for item in ordered),
            intents=tuple(item.intent for item in ordered),
            citations=self._deduplicate_citations(ordered),
            arbitration_basis="combined",
        )

    @staticmethod
    def _deduplicate_citations(outcomes: Sequence[AgentOutcome]) -> tuple[Citation, ...]:
        citations: dict[str, Citation] = {}
        for outcome in outcomes:
            for citation in outcome.citations:
                citations.setdefault(citation.citation_id, citation)
        return tuple(citations.values())
