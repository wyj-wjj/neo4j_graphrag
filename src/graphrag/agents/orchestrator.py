"""LangGraph-backed supervisor and specialist agents with bounded execution."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from difflib import SequenceMatcher
from typing import Any

from langgraph.graph import END, StateGraph

from graphrag.agents.router import route
from graphrag.config import Settings
from graphrag.domain.errors import ValidationError
from graphrag.domain.ids import new_id
from graphrag.domain.models import (
    ActionDraft,
    AgentIntent,
    AnswerStatus,
    ChatResult,
    FAQItem,
    IdentityContext,
    RiskLevel,
    SourceKind,
    utc_now,
)
from graphrag.domain.ports import AuditPort, CheckpointStorePort, KnowledgeRepositoryPort, TracePort
from graphrag.domain.state import AgentState
from graphrag.infrastructure.fakes import FakeBusinessServices
from graphrag.retrieval.pipeline import GraphRAGPipeline
from graphrag.tools.executor import ToolExecutor


@dataclass(slots=True)
class AgentOrchestrator:
    settings: Settings
    repository: KnowledgeRepositoryPort
    checkpoint: CheckpointStorePort
    retrieval: GraphRAGPipeline
    business: FakeBusinessServices
    faqs: tuple[FAQItem, ...]
    trace: TracePort
    graph_checkpointer: Any
    tool_executor: ToolExecutor
    audit: AuditPort
    graph: Any = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.graph = self._build_graph()

    def _build_graph(self) -> Any:
        graph = StateGraph(AgentState)
        graph.add_node("supervisor", self._instrument("supervisor", self._supervisor))
        graph.add_node("faq", self._instrument("faq", self._faq))
        graph.add_node("kb", self._instrument("kb", self._kb))
        graph.add_node("order", self._instrument("order", self._order))
        graph.add_node("logistics", self._instrument("logistics", self._logistics))
        graph.add_node("refund", self._instrument("refund", self._refund))
        graph.add_node("escalation", self._instrument("escalation", self._escalation))
        graph.add_node("clarify", self._instrument("clarify", self._clarify))
        graph.set_entry_point("supervisor")
        graph.add_conditional_edges(
            "supervisor",
            lambda state: state.next_agent.value if state.next_agent is not None else "clarify",
            {
                "faq": "faq",
                "kb": "kb",
                "order": "order",
                "logistics": "logistics",
                "refund": "refund",
                "escalation": "escalation",
                "clarify": "clarify",
            },
        )
        for node in ("faq", "kb", "order", "logistics", "refund", "escalation", "clarify"):
            graph.add_edge(node, END)
        return graph.compile(checkpointer=self.graph_checkpointer)

    def _instrument(
        self,
        node_name: str,
        handler: Callable[[AgentState], Awaitable[dict[str, Any]]],
    ) -> Any:
        async def invoke(state: AgentState) -> dict[str, Any]:
            started_at = utc_now()
            fields: dict[str, Any] = {
                "step_id": new_id(),
                "run_id": state.run_id,
                "tenant_id": state.tenant_id,
                "node_name": node_name,
                "input_summary": {
                    "state_version": state.state_version,
                    "iteration": state.iteration,
                },
                "started_at": started_at.isoformat(),
            }
            try:
                with self.trace.span(
                    f"agent.{node_name}",
                    {"run_id": state.run_id, "tenant_id": state.tenant_id},
                ):
                    result = await handler(state)
            except Exception as exc:
                await self.audit.record(
                    "agent.step",
                    {
                        **fields,
                        "status": "failed",
                        "output_summary": {"error_type": type(exc).__name__},
                        "finished_at": utc_now().isoformat(),
                    },
                )
                raise
            await self.audit.record(
                "agent.step",
                {
                    **fields,
                    "status": "succeeded",
                    "output_summary": {"updated_fields": sorted(result)},
                    "finished_at": utc_now().isoformat(),
                },
            )
            return result

        return invoke

    async def run(
        self,
        identity: IdentityContext,
        *,
        request_id: str,
        session_id: str,
        query: str,
    ) -> ChatResult:
        state = AgentState(
            request_id=request_id,
            run_id=new_id(),
            session_id=session_id,
            tenant_id=identity.tenant_id,
            user_id=identity.user_id,
            roles=identity.roles,
            original_query=query,
            query=query,
        )
        with self.trace.span(
            "agent.run",
            {
                "request_id": request_id,
                "run_id": state.run_id,
                "tenant_id": identity.tenant_id,
                "state_version": state.state_version,
            },
        ):
            result = await self.graph.ainvoke(
                state,
                config={
                    "configurable": {
                        "thread_id": f"{identity.tenant_id}:{session_id}",
                        "checkpoint_ns": "agent-state-v1",
                    }
                },
            )
        final = result if isinstance(result, AgentState) else AgentState.model_validate(result)
        await self.checkpoint.put(final, ttl_seconds=self.settings.checkpoint_ttl_seconds)
        intent = final.next_agent or AgentIntent.CLARIFY
        status = self._status_for(final, intent)
        return ChatResult(
            request_id=final.request_id,
            run_id=final.run_id,
            session_id=final.session_id,
            status=status,
            answer=final.final_answer,
            intent=intent,
            citations=final.citations,
            source=SourceKind.FAKE
            if intent in {AgentIntent.ORDER, AgentIntent.LOGISTICS, AgentIntent.REFUND}
            else SourceKind.REAL,
        )

    async def _supervisor(self, state: AgentState) -> dict[str, Any]:
        with self.trace.span("agent.supervisor", {"run_id": state.run_id}):
            if state.iteration >= 5:
                return {"next_agent": AgentIntent.ESCALATION, "needs_human": True}
            decision = route(state.query, threshold=self.settings.router_confidence_threshold)
            return {
                "next_agent": decision.intent,
                "confidence": decision.confidence,
                "iteration": 1,
            }

    async def _faq(self, state: AgentState) -> dict[str, Any]:
        now = utc_now()
        active = [
            item
            for item in self.faqs
            if item.tenant_id == state.tenant_id
            and item.status.value == "active"
            and item.valid_from <= now
            and (item.valid_until is None or item.valid_until > now)
        ]
        exact = [
            item
            for item in active
            if any(re.search(pattern, state.query, re.I) for pattern in item.patterns)
        ]
        if len(exact) == 1:
            return {"final_answer": exact[0].answer, "confidence": 1.0}
        scored = sorted(
            ((SequenceMatcher(None, state.query, item.question).ratio(), item) for item in active),
            key=lambda value: (-value[0], value[1].faq_id),
        )
        if (
            scored
            and scored[0][0] >= self.settings.faq_similarity_threshold
            and (len(scored) == 1 or scored[0][0] > scored[1][0])
        ):
            return {"final_answer": scored[0][1].answer, "confidence": scored[0][0]}
        answer, status, retrieval = await self.retrieval.answer(
            IdentityContext(
                tenant_id=state.tenant_id,
                user_id=state.user_id,
                roles=state.roles,
            ),
            state.query,
        )
        return {
            "final_answer": answer,
            "citations": retrieval.citations,
            "confidence": 0.5 if status == AnswerStatus.ANSWERED else 0.0,
        }

    async def _kb(self, state: AgentState) -> dict[str, Any]:
        identity = self._identity(state)
        answer, status, result = await self.tool_executor.invoke(
            "kb.hybrid_retrieve.v1",
            identity,
            agent="kb",
            run_id=state.run_id,
            payload={"query": state.query},
            handler=lambda: self.retrieval.answer(identity, state.query),
        )
        return {
            "final_answer": answer,
            "citations": result.citations,
            "retrieved_chunk_ids": tuple(item.chunk_id for item in result.evidences),
            "confidence": 0.95 if status == AnswerStatus.ANSWERED else 0.0,
        }

    async def _order(self, state: AgentState) -> dict[str, Any]:
        identity = self._identity(state)
        order_id = self._extract_order_id(state.query)
        order = await self.tool_executor.invoke(
            "order.query.v1",
            identity,
            agent="order",
            run_id=state.run_id,
            payload={"query": state.query},
            handler=lambda: self.business.query(identity, order_id),
        )
        return {
            "final_answer": (
                f"[Fake 数据] 订单 {order.order_id} 状态：{order.status}，"
                f"地址：{order.masked_address}"
            ),
            "confidence": 1.0,
        }

    async def _logistics(self, state: AgentState) -> dict[str, Any]:
        identity = self._identity(state)
        order_id = self._extract_order_id(state.query)
        info = await self.tool_executor.invoke(
            "logistics.query.v1",
            identity,
            agent="logistics",
            run_id=state.run_id,
            payload={"query": state.query},
            handler=lambda: self.business.query_logistics(identity, order_id),
        )
        return {
            "final_answer": f"[Fake 数据] 物流状态：{info.status}；" + "；".join(info.events),
            "confidence": 1.0,
        }

    async def _refund(self, state: AgentState) -> dict[str, Any]:
        identity = self._identity(state)
        order_id = self._extract_order_id(state.query)
        without_order = re.sub(r"\b(?:DEMO-)?[0-9]{4,}\b", "", state.query, flags=re.I)
        amount_match = re.search(
            r"(?:退款金额|退款|退钱|退)[^0-9]{0,12}([0-9]+(?:\.[0-9]{1,2})?)",
            without_order,
        )
        if amount_match is None:
            return {"final_answer": "请提供需要计算的退款金额。", "confidence": 0.4}
        quote = await self.tool_executor.invoke(
            "refund.calculate.v1",
            identity,
            agent="refund",
            run_id=state.run_id,
            payload={"query": state.query},
            handler=lambda: self.business.calculate(identity, order_id, amount_match.group(1)),
        )
        key = hashlib.sha256(
            f"refund:{state.tenant_id}:{state.user_id}:{order_id}:{quote.amount}".encode()
        ).hexdigest()
        draft = await self.tool_executor.invoke(
            "refund.create_draft.v1",
            identity,
            agent="refund",
            run_id=state.run_id,
            payload={"query": state.query, "idempotency_key": key},
            handler=lambda: self.business.create_draft(identity, quote, key),
        )
        return {
            "final_answer": (
                f"[Fake 草单] 已计算退款 {quote.amount} {quote.currency}，草单 {draft.draft_id} "
                "等待审批；系统不会执行真实退款。"
            ),
            "draft_action_id": draft.draft_id,
            "needs_approval": True,
            "confidence": 1.0,
        }

    async def _escalation(self, state: AgentState) -> dict[str, Any]:
        query_hash = hashlib.sha256(state.query.encode()).hexdigest()
        key = hashlib.sha256(
            f"ticket:{state.tenant_id}:{state.user_id}:{state.session_id}:{query_hash}".encode()
        ).hexdigest()
        draft = ActionDraft(
            tenant_id=state.tenant_id,
            user_id=state.user_id,
            action_type="ticket",
            idempotency_key=key,
            payload={"query_hash": query_hash},
            risk_level=RiskLevel.LOW_WRITE,
            expires_at=utc_now() + timedelta(hours=24),
        )
        identity = self._identity(state)
        saved = await self.tool_executor.invoke(
            "escalation.create_ticket.v1",
            identity,
            agent="escalation",
            run_id=state.run_id,
            payload={"query": state.query, "idempotency_key": key},
            handler=lambda: self.repository.save_draft(draft),
        )
        return {
            "final_answer": f"已创建 Fake 人工工单 {saved.draft_id}，请保持安全并等待人工协助。",
            "draft_action_id": saved.draft_id,
            "needs_human": True,
            "confidence": 1.0,
        }

    async def _clarify(self, state: AgentState) -> dict[str, Any]:
        return {"final_answer": "请说明你需要查询知识、订单、物流还是退款。", "confidence": 0.0}

    @staticmethod
    def _identity(state: AgentState) -> IdentityContext:
        return IdentityContext(tenant_id=state.tenant_id, user_id=state.user_id, roles=state.roles)

    @staticmethod
    def _extract_order_id(query: str) -> str:
        match = re.search(r"\b(?:DEMO-)?[0-9]{4,}\b", query, re.I)
        if match is None:
            raise ValidationError("请提供订单号，例如 DEMO-1001")
        value = match.group(0).upper()
        return value if value.startswith("DEMO-") else f"DEMO-{value}"

    @staticmethod
    def _status_for(state: AgentState, intent: AgentIntent) -> AnswerStatus:
        if state.needs_human:
            return AnswerStatus.ESCALATED
        if not state.citations and intent == AgentIntent.KB and state.confidence == 0:
            return AnswerStatus.REFUSED
        if intent == AgentIntent.CLARIFY or state.confidence < 0.5:
            return AnswerStatus.CLARIFICATION
        if intent in {AgentIntent.ORDER, AgentIntent.LOGISTICS, AgentIntent.REFUND}:
            return AnswerStatus.FAKE_RESULT
        return AnswerStatus.ANSWERED


def default_faqs(tenant_id: str) -> tuple[FAQItem, ...]:
    return (
        FAQItem(
            tenant_id=tenant_id,
            question="怎么开发票",
            answer="虚构商城支持在订单完成后申请电子发票。",
            patterns=(r"怎么.*开.*票", r"开发票"),
        ),
        FAQItem(
            tenant_id=tenant_id,
            question="退货流程是什么",
            answer="虚构商城退货需先提交申请，再按页面提示寄回商品。",
            patterns=(r"退货.*流程",),
        ),
    )
