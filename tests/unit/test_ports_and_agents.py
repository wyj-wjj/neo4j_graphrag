from __future__ import annotations

from datetime import timedelta

import pytest

from graphrag.agents.router import route
from graphrag.domain.errors import AuthorizationError, ConflictError, ValidationError
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
from graphrag.tools.registry import build_default_registry


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
        "order.query.v1",
        identity(),
        agent="order",
        run_id="run-1",
        payload={"query": "订单 DEMO-1001"},
        handler=handler,
    )
    assert result == "ok"
    assert audit.records[0][1]["status"] == "succeeded"
    assert "订单 DEMO-1001" not in str(audit.records)
    with pytest.raises(ValidationError):
        await executor.invoke(
            "order.query.v1",
            identity(),
            agent="order",
            run_id="run-2",
            payload={"unexpected": "x"},
            handler=handler,
        )
