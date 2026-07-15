from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from graphrag.agents.orchestrator import AgentOrchestrator
from graphrag.agents.router import DeterministicRouter, route
from graphrag.domain.errors import (
    AuthorizationError,
    ConflictError,
    DependencyError,
    OperationTimeoutError,
    ValidationError,
)
from graphrag.domain.events import EventEnvelope
from graphrag.domain.ids import new_id
from graphrag.domain.models import (
    AccessPolicy,
    ActionDraft,
    AgentIntent,
    ChunkRecord,
    DocumentRecord,
    DocumentVersion,
    IdentityContext,
    IngestionStatus,
    IngestionTask,
    RiskLevel,
    utc_now,
)
from graphrag.infrastructure.fakes import FakeBusinessServices, FakeModelProvider
from graphrag.infrastructure.memory import (
    InMemoryAudit,
    InMemoryKnowledgeRepository,
    InMemoryLeaseLock,
    InMemoryRateLimiter,
    InMemoryVectorStore,
)
from graphrag.observability.tracing import NoopTrace
from graphrag.tools.executor import ToolExecutor
from graphrag.tools.registry import RetryPolicy, ToolRegistry, build_default_registry


def identity(user_id: str = "demo-user", *roles: str) -> IdentityContext:
    return IdentityContext(
        tenant_id="default", user_id=user_id, roles=frozenset(roles or ("user",))
    )


async def seeded_repository() -> tuple[InMemoryKnowledgeRepository, ChunkRecord]:
    repository = InMemoryKnowledgeRepository()
    document = DocumentRecord(tenant_id="default", title="Policy")
    version = DocumentVersion(
        tenant_id="default",
        document_id=document.document_id,
        version=1,
        object_key="x.txt",
        content_hash="a" * 64,
        mime_type="text/plain",
        size_bytes=1,
    )
    task = IngestionTask(
        tenant_id="default", document_id=document.document_id, version_id=version.version_id
    )
    event = EventEnvelope(
        event_type="document.received",
        tenant_id="default",
        aggregate_id=document.document_id,
        trace_id=new_id(),
    )
    await repository.create_document(document, version, task, event)
    chunk = ChunkRecord(
        tenant_id="default",
        document_id=document.document_id,
        version_id=version.version_id,
        ordinal=0,
        content="warranty policy",
        content_hash="b" * 64,
        embedding_model="fake",
        embedding_dimension=4,
        embedding_version="v1",
    )
    await repository.save_chunks(
        [chunk],
        EventEnvelope(
            event_type="document.chunked",
            tenant_id="default",
            aggregate_id=document.document_id,
            trace_id=new_id(),
        ),
    )
    return repository, chunk


@pytest.mark.asyncio
async def test_acl_deny_wins_and_cross_tenant_is_closed() -> None:
    repository, chunk = await seeded_repository()
    repository.add_policy(
        AccessPolicy(
            chunk_id=chunk.chunk_id,
            tenant_id="default",
            allowed_roles=frozenset({"user"}),
            deny=True,
        )
    )
    assert await repository.authorize_chunks(identity(), [chunk.chunk_id]) == []
    other = IdentityContext(tenant_id="other", user_id="u", roles=frozenset({"user"}))
    assert await repository.authorize_chunks(other, [chunk.chunk_id]) == []


@pytest.mark.asyncio
async def test_retry_requires_failed_state_and_is_explicit() -> None:
    repository, _ = await seeded_repository()
    task = next(iter(repository.tasks.values()))
    with pytest.raises(ConflictError):
        await repository.retry_task("default", task.task_id)
    failed = task.model_copy(update={"status": IngestionStatus.FAILED})
    await repository.update_task(failed)
    retried = await repository.retry_task("default", task.task_id)
    assert retried.status == IngestionStatus.RECEIVED


@pytest.mark.asyncio
async def test_fake_model_vector_contract_stream_and_failure() -> None:
    model = FakeModelProvider(dimension=4, version="v1")
    result = await model.embed(["one", "two"], input_type="document", timeout=1)
    assert result.dimension == 4 and len(result.vectors) == 2
    deltas = [
        item
        async for item in model.stream(
            [{"role": "user", "content": "hello"}], model="fake", timeout=1
        )
    ]
    assert deltas[-1].done
    model.fail_mode = "timeout"
    with pytest.raises(TimeoutError):
        await model.embed(["x"], input_type="query", timeout=1)
    model.fail_mode = None
    extraction = await model.extract("X" * 4096 + " policy", timeout=1)
    assert {entity.normalized_name for entity in extraction.entities} == {"policy"}


@pytest.mark.asyncio
async def test_vector_dimension_lock_rate_limit_and_lease_owner() -> None:
    _, chunk = await seeded_repository()
    store = InMemoryVectorStore(dimension=4, version="v1")
    with pytest.raises(ValueError):
        await store.upsert([chunk], [(1.0, 2.0)])
    limiter = InMemoryRateLimiter()
    assert await limiter.allow("x", limit=1, window_seconds=10)
    assert not await limiter.allow("x", limit=1, window_seconds=10)
    lock = InMemoryLeaseLock()
    assert await lock.acquire("doc", "a", lease_seconds=10)
    assert not await lock.acquire("doc", "b", lease_seconds=10)
    assert not await lock.release("doc", "b")
    assert await lock.release("doc", "a")


@pytest.mark.asyncio
async def test_fake_business_is_authorized_idempotent_and_never_executes() -> None:
    repository, _ = await seeded_repository()
    business = FakeBusinessServices(repository)
    order = await business.query(identity(), "DEMO-1001")
    assert order.source.value == "fake"
    with pytest.raises(AuthorizationError):
        await business.query(identity("intruder"), "DEMO-1001")
    with pytest.raises(ValidationError):
        await business.calculate(identity(), "DEMO-1001", "10000")
    quote = await business.calculate(identity(), "DEMO-1001", "12.50")
    first = await business.create_draft(identity(), quote, "idempotent-key-1")
    second = await business.create_draft(identity(), quote, "idempotent-key-1")
    assert first.draft_id == second.draft_id
    assert first.status == "pending_approval"


def test_router_and_tool_least_privilege() -> None:
    assert route("物流 DEMO-1001", threshold=0.75).intent == AgentIntent.LOGISTICS
    assert route("退款并投诉", threshold=0.75).intent == AgentIntent.CLARIFY
    registry = build_default_registry(10)
    user_tools = registry.allowed_for(identity(), agent="refund", maximum_risk=RiskLevel.READ)
    assert {item.name for item in user_tools} == {"refund.calculate.v1"}
    with pytest.raises(AuthorizationError):
        registry.require("refund.create_draft.v1", identity("guest", "guest"), agent="refund")


def test_router_port_exposes_candidates_and_bounded_compound_plan() -> None:
    router = DeterministicRouter(threshold=0.75, max_experts=2)
    decision = router.route("订单和物流 DEMO-1001")
    assert decision.intent is AgentIntent.CLARIFY
    assert {item.intent for item in decision.candidates} == {
        AgentIntent.ORDER,
        AgentIntent.LOGISTICS,
    }
    plan = router.plan("订单和物流以及保修政策")
    assert len(plan.steps) == plan.max_steps == 2
    assert plan.requires_arbitration is True
    assert all(step.ordinal <= plan.max_steps for step in plan.steps)


def test_action_draft_expiry_contract() -> None:
    draft = ActionDraft(
        tenant_id="default",
        user_id="u",
        action_type="ticket",
        idempotency_key="ticket-key",
        payload={},
        risk_level=RiskLevel.LOW_WRITE,
        expires_at=utc_now() + timedelta(minutes=1),
    )
    assert draft.expires_at > draft.created_at


@pytest.mark.asyncio
async def test_tool_executor_validates_authorizes_and_audits_without_raw_payload() -> None:
    audit = InMemoryAudit()
    executor = ToolExecutor(registry=build_default_registry(1), audit=audit, trace=NoopTrace())

    async def handler() -> str:
        return "ok"

    result = await executor.invoke(
        "faq.match.v1",
        identity(),
        agent="faq",
        run_id="run-1",
        payload={"query": "订单 DEMO-1001"},
        handler=handler,
    )
    assert result == "ok"
    assert audit.records[0][1]["status"] == "succeeded"
    assert "订单 DEMO-1001" not in str(audit.records)
    with pytest.raises(ValidationError):
        await executor.invoke(
            "faq.match.v1",
            identity(),
            agent="faq",
            run_id="run-2",
            payload={"unexpected": "x"},
            handler=handler,
        )


@pytest.mark.asyncio
async def test_tool_executor_rejects_invalid_output_before_agent_state() -> None:
    audit = InMemoryAudit()
    executor = ToolExecutor(registry=build_default_registry(1), audit=audit, trace=NoopTrace())

    async def invalid_handler() -> str:
        return "not-an-order"

    with pytest.raises(ValidationError, match="Tool 输出"):
        await executor.invoke(
            "order.query.v1",
            identity(),
            agent="order",
            run_id="run-invalid-output",
            payload={"query": "DEMO-1001"},
            handler=invalid_handler,
        )
    assert audit.records[-1][1]["status"] == "failed"


@pytest.mark.asyncio
async def test_tool_executor_rejects_oversized_valid_output() -> None:
    defaults = build_default_registry(1)
    faq_spec = defaults.require("faq.match.v1", identity(), agent="faq")
    registry = ToolRegistry()
    registry.register(faq_spec.model_copy(update={"max_output_bytes": 256}))
    executor = ToolExecutor(registry=registry, audit=InMemoryAudit(), trace=NoopTrace())

    async def oversized() -> str:
        return "x" * 300

    with pytest.raises(ValidationError, match="超过允许大小"):
        await executor.invoke(
            "faq.match.v1",
            identity(),
            agent="faq",
            run_id="run-oversized-output",
            payload={"query": "x"},
            handler=oversized,
        )


@pytest.mark.asyncio
async def test_tool_executor_only_retries_idempotent_transient_failures() -> None:
    audit = InMemoryAudit()
    defaults = build_default_registry(1)
    faq_spec = defaults.require("faq.match.v1", identity(), agent="faq")
    registry = ToolRegistry()
    registry.register(faq_spec.model_copy(update={"retry_backoff_seconds": 0.0}))
    executor = ToolExecutor(registry=registry, audit=audit, trace=NoopTrace())
    attempts = 0

    async def flaky_handler() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise DependencyError("knowledge", "temporary", retryable=True)
        return "recovered"

    assert (
        await executor.invoke(
            "faq.match.v1",
            identity(),
            agent="faq",
            run_id="run-retry",
            payload={"query": "退货规则"},
            handler=flaky_handler,
        )
        == "recovered"
    )
    assert attempts == 2
    assert [record[1]["status"] for record in audit.records] == ["retrying", "succeeded"]

    no_retry_registry = ToolRegistry()
    no_retry_registry.register(
        faq_spec.model_copy(
            update={
                "name": "faq.no_retry.v1",
                "retry_policy": RetryPolicy.NONE,
                "retry_backoff_seconds": 0.0,
            }
        )
    )
    no_retry = ToolExecutor(registry=no_retry_registry, audit=InMemoryAudit(), trace=NoopTrace())
    non_idempotent_attempts = 0

    async def always_transient() -> str:
        nonlocal non_idempotent_attempts
        non_idempotent_attempts += 1
        raise ConnectionError("temporary")

    with pytest.raises(ConnectionError):
        await no_retry.invoke(
            "faq.no_retry.v1",
            identity(),
            agent="faq",
            run_id="run-no-retry",
            payload={"query": "x"},
            handler=always_transient,
        )
    assert non_idempotent_attempts == 1


@pytest.mark.asyncio
async def test_tool_executor_circuit_breaker_stops_calling_dependency() -> None:
    defaults = build_default_registry(1)
    faq_spec = defaults.require("faq.match.v1", identity(), agent="faq")
    registry = ToolRegistry()
    registry.register(
        faq_spec.model_copy(
            update={
                "max_attempts": 1,
                "circuit_failure_threshold": 2,
                "circuit_reset_seconds": 60.0,
            }
        )
    )
    executor = ToolExecutor(registry=registry, audit=InMemoryAudit(), trace=NoopTrace())
    calls = 0

    async def unavailable() -> str:
        nonlocal calls
        calls += 1
        raise DependencyError("knowledge", "down", retryable=True)

    for _ in range(2):
        with pytest.raises(DependencyError):
            await executor.invoke(
                "faq.match.v1",
                identity(),
                agent="faq",
                run_id="run-circuit",
                payload={"query": "x"},
                handler=unavailable,
            )
    with pytest.raises(DependencyError, match="熔断"):
        await executor.invoke(
            "faq.match.v1",
            identity(),
            agent="faq",
            run_id="run-circuit-open",
            payload={"query": "x"},
            handler=unavailable,
        )
    assert calls == 2


@pytest.mark.asyncio
async def test_tool_executor_bulkhead_limits_dependency_concurrency() -> None:
    defaults = build_default_registry(1)
    faq_spec = defaults.require("faq.match.v1", identity(), agent="faq")
    registry = ToolRegistry()
    registry.register(faq_spec.model_copy(update={"max_concurrency": 1}))
    executor = ToolExecutor(registry=registry, audit=InMemoryAudit(), trace=NoopTrace())
    active = 0
    maximum_active = 0
    release = asyncio.Event()

    async def blocked() -> str:
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        await release.wait()
        active -= 1
        return "ok"

    first = asyncio.create_task(
        executor.invoke(
            "faq.match.v1",
            identity(),
            agent="faq",
            run_id="run-bulkhead-1",
            payload={"query": "x"},
            handler=blocked,
        )
    )
    await asyncio.sleep(0)
    second = asyncio.create_task(
        executor.invoke(
            "faq.match.v1",
            identity(),
            agent="faq",
            run_id="run-bulkhead-2",
            payload={"query": "y"},
            handler=blocked,
        )
    )
    await asyncio.sleep(0)
    assert active == 1
    release.set()
    results = await asyncio.gather(first, second)
    assert results[0] == "ok" and results[1] == "ok"
    assert maximum_active == 1


@pytest.mark.asyncio
async def test_tool_executor_propagates_cancellation_and_audits_terminal_state() -> None:
    audit = InMemoryAudit()
    executor = ToolExecutor(registry=build_default_registry(1), audit=audit, trace=NoopTrace())
    started = asyncio.Event()

    async def blocked() -> str:
        started.set()
        await asyncio.Event().wait()
        return "unreachable"

    task = asyncio.create_task(
        executor.invoke(
            "faq.match.v1",
            identity(),
            agent="faq",
            run_id="run-cancelled-tool",
            payload={"query": "x"},
            handler=blocked,
        )
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert audit.records[-1][1]["status"] == "cancelled"
    assert audit.records[-1][1]["tool_name"] == "faq.match.v1"


@pytest.mark.asyncio
async def test_tool_total_timeout_has_stable_error_and_agent_fallback() -> None:
    defaults = build_default_registry(1)
    faq_spec = defaults.require("faq.match.v1", identity(), agent="faq")
    registry = ToolRegistry()
    registry.register(faq_spec.model_copy(update={"timeout_seconds": 0.01, "max_attempts": 1}))
    audit = InMemoryAudit()
    executor = ToolExecutor(registry=registry, audit=audit, trace=NoopTrace())

    async def too_slow() -> str:
        await asyncio.Event().wait()
        return "unreachable"

    with pytest.raises(OperationTimeoutError) as caught:
        await executor.invoke(
            "faq.match.v1",
            identity(),
            agent="faq",
            run_id="run-timeout-tool",
            payload={"query": "x"},
            handler=too_slow,
        )
    assert caught.value.code.value == "operation_timeout"
    assert caught.value.dependency == "knowledge"
    assert audit.records[-1][1]["status"] == "failed"
    fallback = AgentOrchestrator.dependency_fallback(caught.value)
    assert fallback["needs_human"] is True
    assert "unreachable" not in str(fallback)
