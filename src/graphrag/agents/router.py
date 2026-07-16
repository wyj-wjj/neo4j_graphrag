"""Rule-first versioned Router Port with bounded compound planning."""

from __future__ import annotations

import re
from dataclasses import dataclass

from graphrag.domain.models import (
    AgentExecutionPlan,
    AgentIntent,
    AgentPlanStep,
    RouteCandidate,
    RouteDecision,
)

_RULES: tuple[tuple[AgentIntent, re.Pattern[str]], ...] = (
    (AgentIntent.ESCALATION, re.compile(r"人工|投诉|转接|自杀|自伤|威胁|报警", re.I)),
    (AgentIntent.REFUND, re.compile(r"退款|退钱|退货金额|refund", re.I)),
    (AgentIntent.LOGISTICS, re.compile(r"物流|快递|到哪|催单|配送|tracking", re.I)),
    (AgentIntent.ORDER, re.compile(r"订单|改地址|收货地址|查单|order", re.I)),
    (AgentIntent.FAQ, re.compile(r"开发票|怎么开票|退货流程|营业时间|客服电话", re.I)),
    (AgentIntent.KB, re.compile(r"政策|规则|说明|产品|会员|保修|知识|为什么|如何", re.I)),
)


@dataclass(frozen=True, slots=True)
class DeterministicRouter:
    threshold: float = 0.75
    max_experts: int = 2
    version: str = "rule-router-v1"

    def route(self, query: str) -> RouteDecision:
        matched = self._matched(query)
        candidates = tuple(
            RouteCandidate(intent=intent, score=0.99, source="rule") for intent in matched
        )
        if len(matched) == 1:
            return RouteDecision(
                intent=matched[0],
                confidence=0.99,
                reason="rule",
                candidates=candidates,
            )
        if len(matched) > 1:
            high_risk = [
                item for item in matched if item in {AgentIntent.REFUND, AgentIntent.ESCALATION}
            ]
            if len(high_risk) == 1:
                return RouteDecision(
                    intent=high_risk[0],
                    confidence=0.9,
                    reason="multi_intent_high_risk",
                    candidates=candidates,
                )
            return RouteDecision(
                intent=AgentIntent.CLARIFY,
                confidence=max(0.0, self.threshold - 0.01),
                reason="ambiguous_multi_intent",
                candidates=candidates,
            )
        return RouteDecision(
            intent=AgentIntent.KB,
            confidence=max(self.threshold, 0.76),
            reason="safe_kb_default",
            candidates=(RouteCandidate(intent=AgentIntent.KB, score=0.76, source="rule"),),
        )

    def plan(self, query: str) -> AgentExecutionPlan:
        matched = self._matched(query)[: self.max_experts]
        steps = tuple(
            AgentPlanStep(
                ordinal=index,
                intent=intent,
                read_only=self._is_read_only(query, intent),
            )
            for index, intent in enumerate(matched, start=1)
        )
        return AgentExecutionPlan(
            steps=steps,
            max_steps=self.max_experts,
            requires_arbitration=len(steps) > 1,
        )

    @staticmethod
    def _matched(query: str) -> tuple[AgentIntent, ...]:
        return tuple(intent for intent, pattern in _RULES if pattern.search(query))

    @staticmethod
    def _is_read_only(query: str, intent: AgentIntent) -> bool:
        if intent in {AgentIntent.FAQ, AgentIntent.KB}:
            return True
        if intent is AgentIntent.ORDER:
            return re.search(r"改地址|修改地址|变更地址", query, re.I) is None
        if intent is AgentIntent.LOGISTICS:
            return re.search(r"催单|催促|加急", query, re.I) is None
        return False


def route(query: str, *, threshold: float) -> RouteDecision:
    """Compatibility wrapper for evaluations and callers not using dependency injection."""

    return DeterministicRouter(threshold=threshold).route(query)
