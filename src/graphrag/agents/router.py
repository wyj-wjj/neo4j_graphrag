"""Rule-first deterministic supervisor with safe low-confidence behaviour."""

from __future__ import annotations

import re
from dataclasses import dataclass

from graphrag.domain.models import AgentIntent


@dataclass(frozen=True, slots=True)
class RouteDecision:
    intent: AgentIntent
    confidence: float
    reason: str


_RULES: tuple[tuple[AgentIntent, re.Pattern[str]], ...] = (
    (AgentIntent.ESCALATION, re.compile(r"人工|投诉|转接|自杀|自伤|威胁|报警", re.I)),
    (AgentIntent.REFUND, re.compile(r"退款|退钱|退货金额|refund", re.I)),
    (AgentIntent.LOGISTICS, re.compile(r"物流|快递|到哪|催单|配送|tracking", re.I)),
    (AgentIntent.ORDER, re.compile(r"订单|改地址|收货地址|查单|order", re.I)),
    (AgentIntent.FAQ, re.compile(r"开发票|怎么开票|退货流程|营业时间|客服电话", re.I)),
    (AgentIntent.KB, re.compile(r"政策|规则|说明|产品|会员|保修|知识|为什么|如何", re.I)),
)


def route(query: str, *, threshold: float) -> RouteDecision:
    matched = [intent for intent, pattern in _RULES if pattern.search(query)]
    if len(matched) == 1:
        return RouteDecision(matched[0], 0.99, "rule")
    if len(matched) > 1:
        high_risk = [
            item for item in matched if item in {AgentIntent.REFUND, AgentIntent.ESCALATION}
        ]
        if len(high_risk) == 1:
            return RouteDecision(high_risk[0], 0.9, "multi_intent_high_risk")
        return RouteDecision(AgentIntent.CLARIFY, threshold - 0.01, "ambiguous_multi_intent")
    return RouteDecision(AgentIntent.KB, max(threshold, 0.76), "safe_kb_default")
