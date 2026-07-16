"""LangGraph-backed supervisor and specialist agents with bounded execution."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import timedelta
from difflib import SequenceMatcher
from typing import Any

from langgraph.errors import GraphInterrupt
from langgraph.graph import END, StateGraph
from langgraph.types import Command, interrupt

from graphrag.agents.consolidation import DeterministicResultConsolidator
from graphrag.application.context import ContextAssembler, ConversationMemoryManager
from graphrag.application.prompts import PromptRegistry
from graphrag.config import Settings
from graphrag.domain.errors import (
    AppError,
    ConflictError,
    ErrorCode,
    NotFoundError,
    ValidationError,
)
from graphrag.domain.events import RunEventType
from graphrag.domain.ids import new_id
from graphrag.domain.models import (
    ActionDraft,
    AgentIntent,
    AgentOutcome,
    AnswerStatus,
    ApprovalResumeResult,
    ChatMessage,
    ChatResult,
    ContextManifest,
    ConversationState,
    FAQItem,
    GenerationManifest,
    IdentityContext,
    RiskLevel,
    SafeResumeSnapshot,
    SessionMessage,
    SourceKind,
    utc_now,
)
from graphrag.domain.ports import (
    AuditPort,
    BusinessServicesPort,
    CheckpointStorePort,
    KnowledgeRepositoryPort,
    RouterPort,
    SafeResumeStorePort,
    TracePort,
)
from graphrag.domain.state import AgentState
from graphrag.retrieval.pipeline import GraphRAGPipeline
from graphrag.tools.executor import ToolExecutor

RunEventSink = Callable[[RunEventType, dict[str, Any] | None], Awaitable[Any]]
_RUN_EVENT_SINK: ContextVar[RunEventSink | None] = ContextVar("agent_run_event_sink", default=None)


@dataclass(frozen=True, slots=True)
class AgentExecution:
    result: ChatResult
    conversation_state: ConversationState
    context_manifest: ContextManifest
    generation_manifest: GenerationManifest


@dataclass(slots=True)
class AgentOrchestrator:
    settings: Settings
    repository: KnowledgeRepositoryPort
    checkpoint: CheckpointStorePort
    retrieval: GraphRAGPipeline
    business: BusinessServicesPort
    faqs: tuple[FAQItem, ...]
    trace: TracePort
    graph_checkpointer: Any
    tool_executor: ToolExecutor
    audit: AuditPort
    context_assembler: ContextAssembler
    memory_manager: ConversationMemoryManager
    safe_resume: SafeResumeStorePort
    prompt_registry: PromptRegistry
    router: RouterPort
    graph: Any = field(init=False, repr=False)
    consolidator: DeterministicResultConsolidator = field(
        default_factory=DeterministicResultConsolidator
    )

    def __post_init__(self) -> None:
        self.graph = self._build_graph()

    def _build_graph(self) -> Any:
        graph = StateGraph(AgentState)
        graph.add_node("supervisor", self._instrument("supervisor", self._supervisor))
        graph.add_node("faq", self._instrument("faq", self._faq))
        graph.add_node("kb", self._instrument("kb", self._kb))
        graph.add_node("order", self._instrument("order", self._order))
        graph.add_node("logistics", self._instrument("logistics", self._logistics))
        graph.add_node(
            "compound",
            self._instrument("compound", self._order_logistics),
        )
        graph.add_node("refund", self._instrument("refund", self._refund))
        graph.add_node(
            "approval_interrupt",
            self._instrument("approval_interrupt", self._approval_interrupt),
        )
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
                "compound": "compound",
                "refund": "refund",
                "escalation": "escalation",
                "clarify": "clarify",
            },
        )
        graph.add_edge("refund", "approval_interrupt")
        for node in (
            "faq",
            "kb",
            "order",
            "logistics",
            "compound",
            "escalation",
            "clarify",
        ):
            graph.add_edge(node, END)
        graph.add_edge("approval_interrupt", END)
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
            except GraphInterrupt:
                await self.audit.record(
                    "agent.step",
                    {
                        **fields,
                        "status": "interrupted",
                        "output_summary": {"safe_node": node_name},
                        "finished_at": utc_now().isoformat(),
                    },
                )
                raise
            except AppError as exc:
                if exc.code not in {ErrorCode.DEPENDENCY, ErrorCode.TIMEOUT}:
                    await self.audit.record(
                        "agent.step",
                        {
                            **fields,
                            "status": "failed",
                            "output_summary": {"error_code": exc.code.value},
                            "finished_at": utc_now().isoformat(),
                        },
                    )
                    raise
                result = self.dependency_fallback(exc)
                await self.audit.record(
                    "agent.step",
                    {
                        **fields,
                        "status": "degraded",
                        "output_summary": {
                            "error_code": exc.code.value,
                            "dependency": exc.dependency,
                            "needs_human": True,
                        },
                        "finished_at": utc_now().isoformat(),
                    },
                )
                return result
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

    @staticmethod
    def dependency_fallback(exc: AppError) -> dict[str, Any]:
        """Fail predictably without exposing dependency details to the end user."""

        return {
            "final_answer": "当前依赖服务暂不可用，已安全停止自动处理，请稍后重试或转人工。",
            "needs_human": True,
            "confidence": 0.0,
            "refusal_reason": "dependency_unavailable",
            "errors": (f"{exc.code.value}:{exc.dependency or 'unknown'}",),
        }

    async def run(
        self,
        identity: IdentityContext,
        *,
        request_id: str,
        run_id: str | None = None,
        client_turn_id: str | None = None,
        session_id: str,
        query: str,
        history: Sequence[ChatMessage] = (),
        conversation_state: ConversationState | None = None,
        user_sequence: int = 1,
        event_sink: RunEventSink | None = None,
    ) -> AgentExecution:
        resolved_run_id = run_id or new_id()
        current_state = conversation_state or ConversationState(
            tenant_id=identity.tenant_id,
            session_id=session_id,
        )
        durable_history = tuple(item for item in history if isinstance(item, SessionMessage))
        current_message = SessionMessage(
            message_id=f"{client_turn_id or request_id}:user",
            client_turn_id=client_turn_id or request_id,
            run_id=resolved_run_id,
            sequence=user_sequence,
            role="user",
            content=query,
            content_hash=hashlib.sha256(query.encode()).hexdigest(),
        )
        current_state = self.memory_manager.update(
            current_state,
            (*durable_history, current_message),
        )
        initial_context = self.context_assembler.assemble(
            run_id=resolved_run_id,
            tenant_id=identity.tenant_id,
            session_id=session_id,
            identity=identity,
            model=self.settings.chat_model,
            system_instructions=self.prompt_registry.get("agent-safety").content,
            current_query=query,
            history=durable_history,
            conversation_state=current_state,
        )
        state = AgentState(
            request_id=request_id,
            run_id=resolved_run_id,
            client_turn_id=client_turn_id,
            session_id=session_id,
            tenant_id=identity.tenant_id,
            user_id=identity.user_id,
            roles=identity.roles,
            original_query=query,
            query=query,
            chat_history=initial_context.selected_history,
            conversation_state=current_state,
            context_manifest=initial_context.manifest,
        )
        sink_token = _RUN_EVENT_SINK.set(event_sink)
        try:
            with self.trace.span(
                "agent.run",
                {
                    "request_id": request_id,
                    "run_id": state.run_id,
                    "tenant_id": identity.tenant_id,
                    "state_version": state.state_version,
                },
            ):
                raw_result = await self.graph.ainvoke(
                    state,
                    config={
                        "configurable": {
                            "thread_id": f"{identity.tenant_id}:{session_id}",
                            "checkpoint_ns": "agent-state-v2",
                        }
                    },
                )
        finally:
            _RUN_EVENT_SINK.reset(sink_token)
        interrupted = isinstance(raw_result, dict) and bool(raw_result.get("__interrupt__"))
        result = (
            {key: value for key, value in raw_result.items() if key != "__interrupt__"}
            if isinstance(raw_result, dict)
            else raw_result
        )
        final = result if isinstance(result, AgentState) else AgentState.model_validate(result)
        if interrupted:
            if final.draft_action_id is None:
                raise ValidationError("审批中断缺少草单 ID")
            await self.safe_resume.save(
                SafeResumeSnapshot(
                    tenant_id=final.tenant_id,
                    user_id=final.user_id,
                    roles=final.roles,
                    session_id=final.session_id,
                    run_id=final.run_id,
                    request_id=final.request_id,
                    client_turn_id=client_turn_id or request_id,
                    draft_id=final.draft_action_id,
                    completed_side_effects=(
                        "refund.calculate.v1",
                        "refund.create_draft.v1",
                    ),
                    expires_at=utc_now() + timedelta(hours=24),
                )
            )
        await self.checkpoint.put(final, ttl_seconds=self.settings.checkpoint_ttl_seconds)
        intent = final.next_agent or AgentIntent.CLARIFY
        status = self._status_for(final, intent)
        chat_result = ChatResult(
            request_id=final.request_id,
            run_id=final.run_id,
            client_turn_id=client_turn_id,
            session_id=final.session_id,
            status=status,
            answer=final.final_answer,
            intent=intent,
            citations=final.citations,
            source=SourceKind.FAKE
            if intent
            in {
                AgentIntent.ORDER,
                AgentIntent.LOGISTICS,
                AgentIntent.REFUND,
                AgentIntent.COMPOUND,
            }
            else SourceKind.REAL,
            open_questions=final.conversation_state.open_questions
            if final.conversation_state is not None
            else (),
            actions=(f"draft:{final.draft_action_id}",)
            if final.draft_action_id is not None
            else (),
            refusal_reason=final.refusal_reason
            if final.refusal_reason
            in {
                "insufficient_evidence",
                "conflicting_evidence",
                "safety_policy",
                "dependency_unavailable",
            }
            else None,
        )
        if final.conversation_state is None or final.context_manifest is None:
            raise ValidationError("Agent 运行缺少上下文状态或 Manifest")
        return AgentExecution(
            result=chat_result,
            conversation_state=final.conversation_state,
            context_manifest=final.context_manifest,
            generation_manifest=GenerationManifest(
                run_id=final.run_id,
                tenant_id=final.tenant_id,
                prompt_bundle_version=self.prompt_registry.bundle_version,
                prompt_hashes=self.prompt_registry.hashes(),
                chat_model=self.settings.chat_model,
                router_version=self.router.version,
                embedding_model=self.settings.embedding_model,
                embedding_version=self.settings.embedding_version,
                rerank_model=self.settings.rerank_model,
                state_version=final.state_version,
                context_policy_version=self.settings.context_policy_version,
                evaluation_set_version="memory-reliability-v1",
            ),
        )

    async def resume_approval(
        self,
        identity: IdentityContext,
        *,
        draft_id: str,
        decision: str,
    ) -> ApprovalResumeResult:
        snapshot = await self.safe_resume.get_by_draft(identity.tenant_id, draft_id)
        if snapshot is None:
            raise NotFoundError("审批草单没有可恢复的 Agent 运行")
        if snapshot.status == "resumed":
            if snapshot.decision != decision:
                raise ConflictError("审批草单已使用不同决定恢复")
            if snapshot.final_answer is None:
                raise ValidationError("已恢复快照缺少终态")
            return ApprovalResumeResult(
                run_id=snapshot.run_id,
                draft_id=draft_id,
                decision=decision,
                duplicate=True,
                recovery_source=snapshot.recovery_source,
                final_answer=snapshot.final_answer,
            )
        if snapshot.status != "pending":
            raise ConflictError("审批草单当前不可恢复")

        config = {
            "configurable": {
                "thread_id": f"{snapshot.tenant_id}:{snapshot.session_id}",
                "checkpoint_ns": "agent-state-v2",
            }
        }
        recovery_source = "langgraph"
        try:
            raw = await self.graph.ainvoke(
                Command(resume={"draft_id": draft_id, "decision": decision}),
                config=config,
            )
            if not isinstance(raw, (dict, AgentState)):
                raise ValidationError("审批恢复返回未知状态")
            if isinstance(raw, dict) and raw.get("__interrupt__"):
                raise ValidationError("审批恢复仍停留在中断状态")
            final = raw if isinstance(raw, AgentState) else AgentState.model_validate(raw)
            if final.approval_status != decision or final.needs_approval:
                raise ValidationError("审批恢复终态不一致")
        except Exception as exc:
            recovery_source = "mysql_snapshot"
            final = self._state_from_safe_snapshot(snapshot, decision)
            await self.audit.record(
                "approval.resume_fallback",
                {
                    "tenant_id": snapshot.tenant_id,
                    "run_id": snapshot.run_id,
                    "draft_id": draft_id,
                    "error_type": type(exc).__name__,
                    "recovery_source": recovery_source,
                },
            )

        result_hash = hashlib.sha256(
            json.dumps(
                {
                    "run_id": final.run_id,
                    "draft_id": draft_id,
                    "decision": decision,
                    "final_answer": final.final_answer,
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode()
        ).hexdigest()
        stored = await self.safe_resume.mark_resumed(
            snapshot.tenant_id,
            snapshot.snapshot_id,
            decision=decision,
            result_hash=result_hash,
            final_answer=final.final_answer,
            recovery_source=recovery_source,
        )
        try:
            await self.checkpoint.put(final, ttl_seconds=self.settings.checkpoint_ttl_seconds)
        except Exception as exc:
            await self.audit.record(
                "approval.resume_checkpoint_failed",
                {
                    "tenant_id": snapshot.tenant_id,
                    "run_id": snapshot.run_id,
                    "error_type": type(exc).__name__,
                },
            )
        return ApprovalResumeResult(
            run_id=stored.run_id,
            draft_id=draft_id,
            decision=decision,
            recovery_source=stored.recovery_source,
            final_answer=stored.final_answer or final.final_answer,
        )

    async def _supervisor(self, state: AgentState) -> dict[str, Any]:
        with self.trace.span("agent.supervisor", {"run_id": state.run_id}):
            if state.iteration >= 5:
                await self._emit_event(
                    "route", {"intent": AgentIntent.ESCALATION.value, "bounded": True}
                )
                return {"next_agent": AgentIntent.ESCALATION, "needs_human": True}
            decision = self.router.route(state.query)
            plan = self.router.plan(state.query)
            compound_allowed = (
                len(decision.candidates) == 2
                and len(plan.steps) == 2
                and all(step.read_only for step in plan.steps)
                and {step.intent for step in plan.steps}
                == {AgentIntent.ORDER, AgentIntent.LOGISTICS}
            )
            selected_intent = AgentIntent.COMPOUND if compound_allowed else decision.intent
            await self._emit_event(
                "route",
                {
                    "intent": selected_intent.value,
                    "confidence": decision.confidence,
                    "reason": decision.reason,
                    "candidates": [item.intent.value for item in decision.candidates],
                    "router_version": decision.router_version,
                    "plan_version": plan.plan_version,
                    "plan_steps": [item.intent.value for item in plan.steps],
                    "compound_allowed": compound_allowed,
                },
            )
            return {
                "next_agent": selected_intent,
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
        await self._emit_event("retrieving", {"agent": "faq", "mode": "hybrid"})
        answer, status, retrieval = await self.retrieval.answer(
            IdentityContext(
                tenant_id=state.tenant_id,
                user_id=state.user_id,
                roles=state.roles,
            ),
            state.query,
            history=state.chat_history,
            conversation_state=state.conversation_state,
            run_id=state.run_id,
            session_id=state.session_id,
            on_delta=self._emit_delta,
        )
        return {
            "final_answer": answer,
            "citations": retrieval.citations,
            "confidence": 0.5 if status == AnswerStatus.ANSWERED else 0.0,
            "context_manifest": retrieval.context_manifest,
            "answer_status": status,
            "refusal_reason": self._retrieval_refusal_reason(status, retrieval.branch_status),
        }

    async def _kb(self, state: AgentState) -> dict[str, Any]:
        identity = self._identity(state)
        await self._emit_event("retrieving", {"agent": "kb", "mode": "hybrid"})
        answer, status, result = await self.tool_executor.invoke(
            "kb.hybrid_retrieve.v1",
            identity,
            agent="kb",
            run_id=state.run_id,
            payload={"query": state.query},
            handler=lambda: self.retrieval.answer(
                identity,
                state.query,
                history=state.chat_history,
                conversation_state=state.conversation_state,
                run_id=state.run_id,
                session_id=state.session_id,
                on_delta=self._emit_delta,
            ),
        )
        return {
            "final_answer": answer,
            "citations": result.citations,
            "retrieved_chunk_ids": tuple(item.chunk_id for item in result.evidences),
            "confidence": 0.95 if status == AnswerStatus.ANSWERED else 0.0,
            "context_manifest": result.context_manifest,
            "answer_status": status,
            "refusal_reason": self._retrieval_refusal_reason(status, result.branch_status),
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

    async def _order_logistics(self, state: AgentState) -> dict[str, Any]:
        identity = self._identity(state)
        order_id = self._extract_order_id(state.query)
        order, logistics = await asyncio.gather(
            self.tool_executor.invoke(
                "order.query.v1",
                identity,
                agent="order",
                run_id=state.run_id,
                payload={"query": state.query},
                handler=lambda: self.business.query(identity, order_id),
            ),
            self.tool_executor.invoke(
                "logistics.query.v1",
                identity,
                agent="logistics",
                run_id=state.run_id,
                payload={"query": state.query},
                handler=lambda: self.business.query_logistics(identity, order_id),
            ),
        )
        result = self.consolidator.consolidate_complementary(
            (
                AgentOutcome(
                    intent=AgentIntent.ORDER,
                    answer=(
                        f"[Fake 数据] 订单 {order.order_id} 状态：{order.status}，"
                        f"地址：{order.masked_address}"
                    ),
                    authority_rank=300,
                ),
                AgentOutcome(
                    intent=AgentIntent.LOGISTICS,
                    answer=f"[Fake 数据] 物流状态：{logistics.status}；"
                    + "；".join(logistics.events),
                    authority_rank=300,
                ),
            )
        )
        return {
            "final_answer": result.answer,
            "citations": result.citations,
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
            (
                f"refund:{state.tenant_id}:{state.user_id}:{order_id}:{quote.amount}:"
                f"{state.client_turn_id or state.request_id}"
            ).encode()
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

    async def _approval_interrupt(self, state: AgentState) -> dict[str, Any]:
        if state.draft_action_id is None:
            raise ValidationError("审批节点缺少草单 ID")
        resumed = interrupt(
            {
                "event_version": 1,
                "type": "fake_refund_approval",
                "run_id": state.run_id,
                "draft_id": state.draft_action_id,
                "executed": False,
            }
        )
        if not isinstance(resumed, dict):
            raise ValidationError("审批恢复载荷必须是对象")
        if resumed.get("draft_id") != state.draft_action_id:
            raise ValidationError("审批恢复草单不匹配")
        decision = resumed.get("decision")
        if decision not in {"approved", "rejected"}:
            raise ValidationError("审批决定无效")
        if decision == "approved":
            answer = (
                f"[Fake 审批] 草单 {state.draft_action_id} 已批准；"
                "本演示仅恢复 Agent 状态，仍不会执行真实退款。"
            )
        else:
            answer = f"[Fake 审批] 草单 {state.draft_action_id} 已拒绝；没有执行任何真实退款。"
        return {
            "final_answer": answer,
            "needs_approval": False,
            "approval_status": decision,
            "confidence": 1.0,
        }

    async def _escalation(self, state: AgentState) -> dict[str, Any]:
        query_hash = hashlib.sha256(state.query.encode()).hexdigest()
        key = hashlib.sha256(
            (
                f"ticket:{state.tenant_id}:{state.user_id}:{state.session_id}:{query_hash}:"
                f"{state.client_turn_id or state.request_id}"
            ).encode()
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
    async def _emit_event(event_type: RunEventType, data: dict[str, Any] | None = None) -> None:
        sink = _RUN_EVENT_SINK.get()
        if sink is not None:
            await sink(event_type, data)

    @classmethod
    async def _emit_delta(cls, content: str) -> None:
        await cls._emit_event("delta", {"content": content})

    @staticmethod
    def _state_from_safe_snapshot(
        snapshot: SafeResumeSnapshot,
        decision: str,
    ) -> AgentState:
        if decision == "approved":
            answer = (
                f"[Fake 审批] 草单 {snapshot.draft_id} 已批准；"
                "已从 MySQL 安全快照恢复，仍不会执行真实退款。"
            )
        else:
            answer = (
                f"[Fake 审批] 草单 {snapshot.draft_id} 已拒绝；"
                "已从 MySQL 安全快照恢复，没有执行任何真实退款。"
            )
        return AgentState(
            request_id=snapshot.request_id,
            run_id=snapshot.run_id,
            client_turn_id=snapshot.client_turn_id,
            session_id=snapshot.session_id,
            tenant_id=snapshot.tenant_id,
            user_id=snapshot.user_id,
            roles=snapshot.roles,
            original_query="[redacted-refund-request]",
            query="[redacted-refund-request]",
            next_agent=AgentIntent.REFUND,
            draft_action_id=snapshot.draft_id,
            final_answer=answer,
            needs_approval=False,
            approval_status=decision,
            confidence=1.0,
            conversation_state=ConversationState(
                tenant_id=snapshot.tenant_id,
                session_id=snapshot.session_id,
            ),
        )

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
        if state.answer_status is not None:
            return state.answer_status
        if not state.citations and intent == AgentIntent.KB and state.confidence == 0:
            return AnswerStatus.REFUSED
        if intent == AgentIntent.CLARIFY or state.confidence < 0.5:
            return AnswerStatus.CLARIFICATION
        if intent in {AgentIntent.ORDER, AgentIntent.LOGISTICS, AgentIntent.REFUND}:
            return AnswerStatus.FAKE_RESULT
        return AnswerStatus.ANSWERED

    @staticmethod
    def _retrieval_refusal_reason(
        status: AnswerStatus, branch_status: dict[str, str]
    ) -> str | None:
        if status is not AnswerStatus.REFUSED:
            return None
        return "conflicting_evidence" if "conflict" in branch_status else "insufficient_evidence"


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
