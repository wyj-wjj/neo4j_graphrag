from __future__ import annotations

import asyncio
import hashlib
from datetime import timedelta
from pathlib import Path

import pytest
from scripts.rebuild_derived_indexes import _active_events
from sqlalchemy import select

from graphrag.application.consumer import EventConsumerProcessor
from graphrag.application.context import ContextAssembler, ContextPolicy, HeuristicTokenEstimator
from graphrag.application.index_workers import GraphIndexEventHandler, VectorIndexEventHandler
from graphrag.application.sessions import TurnDisposition
from graphrag.domain.errors import ConflictError, NotFoundError
from graphrag.domain.events import EventEnvelope
from graphrag.domain.ids import new_id
from graphrag.domain.models import (
    ActionDraft,
    AgentIntent,
    AnswerStatus,
    ChatResult,
    ChunkRecord,
    ConversationState,
    DocumentRecord,
    DocumentVersion,
    GenerationManifest,
    IdentityContext,
    IngestionStatus,
    IngestionTask,
    MemoryCategory,
    RiskLevel,
    SafeResumeSnapshot,
    SourceKind,
    utc_now,
)
from graphrag.infrastructure.database import (
    AgentRunORM,
    AgentStepORM,
    Base,
    ChunkACLORM,
    ContextManifestORM,
    Database,
    DeadLetterEventORM,
    GenerationManifestORM,
    InboxEventORM,
    LongTermMemoryORM,
    MessageORM,
    OutboxEventORM,
    SessionORM,
    SQLKnowledgeRepository,
    ToolCallLogORM,
)
from graphrag.infrastructure.fakes import FakeModelProvider
from graphrag.infrastructure.inbox import SQLInboxStore
from graphrag.infrastructure.memory import InMemoryGraphStore, InMemoryVectorStore
from graphrag.infrastructure.outbox import SQLOutboxStore
from graphrag.infrastructure.sql_services import (
    SQLAudit,
    SQLLongTermMemoryStore,
    SQLSafeResumeStore,
    SQLSessionService,
)


@pytest.fixture
async def database(tmp_path: Path) -> Database:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'integration.db'}")
    async with database.engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    yield database
    async with database.engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
    await database.close()


async def create_knowledge(
    repository: SQLKnowledgeRepository,
) -> tuple[DocumentRecord, DocumentVersion, IngestionTask, ChunkRecord]:
    document = DocumentRecord(tenant_id="default", title="SQL policy")
    version = DocumentVersion(
        document_id=document.document_id,
        tenant_id="default",
        version=1,
        object_key="doc/original.txt",
        content_hash="a" * 64,
        mime_type="text/plain",
        size_bytes=10,
    )
    task = IngestionTask(
        tenant_id="default", document_id=document.document_id, version_id=version.version_id
    )
    received = EventEnvelope(
        event_type="document.received",
        tenant_id="default",
        aggregate_id=document.document_id,
        trace_id=new_id(),
    )
    await repository.create_document(document, version, task, received)
    chunk = ChunkRecord(
        tenant_id="default",
        document_id=document.document_id,
        version_id=version.version_id,
        ordinal=0,
        content="SQL-backed warranty",
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
    return document, version, task, chunk


@pytest.mark.asyncio
async def test_sql_long_term_memory_is_confirmed_versioned_and_deleted(database: Database) -> None:
    store = SQLLongTermMemoryStore(database)
    owner = IdentityContext(tenant_id="default", user_id="memory-owner", roles=frozenset({"user"}))
    created = await store.create_confirmed(
        owner,
        category=MemoryCategory.PREFERENCE,
        key="style",
        value="concise",
        source_turn_id="client-turn-memory-0001",
        expires_at=utc_now() + timedelta(days=30),
    )
    corrected = await store.correct(
        owner,
        created.memory_id,
        value="detailed",
        source_turn_id="client-turn-memory-0002",
        expires_at=None,
    )
    assert corrected.version == 2
    assert [item.value for item in await store.list_active(owner)] == ["detailed"]
    await store.delete(owner, corrected.memory_id)
    assert await store.list_active(owner) == []
    async with database.session() as session:
        rows = (
            await session.scalars(
                select(LongTermMemoryORM).where(
                    LongTermMemoryORM.tenant_id == owner.tenant_id,
                    LongTermMemoryORM.user_id == owner.user_id,
                )
            )
        ).all()
        assert {row.status for row in rows} == {"deleted"}
        assert {row.value for row in rows} == {"[deleted]"}


@pytest.mark.asyncio
async def test_sql_truth_source_outbox_acl_and_deny_priority(database: Database) -> None:
    repository = SQLKnowledgeRepository(database)
    _, _, _, chunk = await create_knowledge(repository)
    identity = IdentityContext(tenant_id="default", user_id="u", roles=frozenset({"user"}))
    assert [
        item.chunk_id for item in await repository.authorize_chunks(identity, [chunk.chunk_id])
    ] == [chunk.chunk_id]
    async with database.session() as session:
        allow = await session.scalar(
            select(ChunkACLORM).where(ChunkACLORM.chunk_id == chunk.chunk_id)
        )
        assert allow is not None
        session.add(
            ChunkACLORM(
                chunk_id=chunk.chunk_id,
                tenant_id="default",
                allowed_role="user",
                allowed_department=None,
                deny=True,
                valid_from=chunk.valid_from,
                valid_until=None,
            )
        )
        outbox_count = len((await session.scalars(select(OutboxEventORM))).all())
        assert outbox_count == 2
    assert await repository.authorize_chunks(identity, [chunk.chunk_id]) == []


@pytest.mark.asyncio
async def test_sql_outbox_lease_ack_and_retry_are_owner_guarded(database: Database) -> None:
    repository = SQLKnowledgeRepository(database)
    await create_knowledge(repository)
    store = SQLOutboxStore(database)

    first = (await store.claim(worker_id="relay-a", limit=1, lease_seconds=30))[0]
    claimed_by_other = await store.claim(worker_id="relay-b", limit=10, lease_seconds=30)
    assert claimed_by_other == []

    await store.mark_published(
        first.event.event_id,
        worker_id="relay-a",
        published_at=utc_now(),
    )
    second = (await store.claim(worker_id="relay-b", limit=10, lease_seconds=30))[0]
    retry_at = utc_now() + timedelta(seconds=10)
    await store.release_for_retry(
        second.event.event_id,
        worker_id="relay-b",
        available_at=retry_at,
        error_code="broker_unavailable",
    )

    async with database.session() as session:
        first_row = await session.get(OutboxEventORM, first.event.event_id)
        second_row = await session.get(OutboxEventORM, second.event.event_id)
        assert first_row is not None and first_row.published is True
        assert first_row.published_at is not None and first_row.lease_owner is None
        assert second_row is not None and second_row.retry_count == 1
        assert second_row.last_error_code == "broker_unavailable"
        assert second_row.lease_owner is None

    assert await store.claim(worker_id="relay-c", limit=10, lease_seconds=30) == []

    with pytest.raises(ConflictError):
        await store.mark_published(
            second.event.event_id,
            worker_id="relay-a",
            published_at=utc_now(),
        )


@pytest.mark.asyncio
async def test_sql_outbox_orders_aggregate_versions_before_timestamps(
    database: Database,
) -> None:
    aggregate_id = new_id()
    now = utc_now()
    first = EventEnvelope(
        event_type="document.received",
        aggregate_version=1,
        tenant_id="default",
        aggregate_id=aggregate_id,
        occurred_at=now,
        trace_id=new_id(),
    )
    clock_skewed_second = EventEnvelope(
        event_type="document.chunked",
        aggregate_version=2,
        tenant_id="default",
        aggregate_id=aggregate_id,
        occurred_at=now - timedelta(minutes=5),
        trace_id=new_id(),
    )
    async with database.session() as session:
        session.add_all(
            [
                OutboxEventORM(
                    **first.model_dump(mode="python"),
                    published=False,
                    retry_count=0,
                ),
                OutboxEventORM(
                    **clock_skewed_second.model_dump(mode="python"),
                    published=False,
                    retry_count=0,
                ),
            ]
        )

    store = SQLOutboxStore(database)
    claimed_first = (await store.claim(worker_id="relay-a", limit=10, lease_seconds=30))[0]
    assert claimed_first.event.event_id == first.event_id
    await store.mark_published(
        first.event_id,
        worker_id="relay-a",
        published_at=utc_now(),
    )
    claimed_second = (await store.claim(worker_id="relay-b", limit=10, lease_seconds=30))[0]
    assert claimed_second.event.event_id == clock_skewed_second.event_id


@pytest.mark.asyncio
async def test_sql_inbox_claim_is_consumer_scoped_leased_and_hash_guarded(
    database: Database,
) -> None:
    store = SQLInboxStore(database)
    event = EventEnvelope(
        event_type="document.chunked",
        tenant_id="default",
        aggregate_id=new_id(),
        trace_id=new_id(),
    )
    raw = event.model_dump_json().encode()
    payload_hash = hashlib.sha256(raw).hexdigest()

    claimed = await store.claim(
        consumer_name="embedding-worker",
        event=event,
        payload_hash=payload_hash,
        worker_id="worker-a",
        lease_seconds=30,
    )
    busy = await store.claim(
        consumer_name="embedding-worker",
        event=event,
        payload_hash=payload_hash,
        worker_id="worker-b",
        lease_seconds=30,
    )
    other_consumer = await store.claim(
        consumer_name="kg-worker",
        event=event,
        payload_hash=payload_hash,
        worker_id="worker-b",
        lease_seconds=30,
    )
    assert claimed.disposition == "claimed"
    assert busy.disposition == "busy"
    assert other_consumer.disposition == "claimed"

    await store.mark_processed(
        consumer_name="embedding-worker",
        event_id=event.event_id,
        worker_id="worker-a",
        processed_at=utc_now(),
    )
    duplicate = await store.claim(
        consumer_name="embedding-worker",
        event=event,
        payload_hash=payload_hash,
        worker_id="worker-c",
        lease_seconds=30,
    )
    assert duplicate.disposition == "duplicate"
    with pytest.raises(ConflictError, match="不同载荷"):
        await store.claim(
            consumer_name="embedding-worker",
            event=event,
            payload_hash="f" * 64,
            worker_id="worker-c",
            lease_seconds=30,
        )


@pytest.mark.asyncio
async def test_consumer_processor_deduplicates_and_dlqs_unknown_or_invalid_events(
    database: Database,
) -> None:
    store = SQLInboxStore(database)
    handled: list[str] = []

    async def handle(event: EventEnvelope) -> None:
        handled.append(event.event_id)

    processor = EventConsumerProcessor(
        consumer_name="embedding-worker",
        store=store,
        handler=handle,
        supported_event_version=1,
        max_event_bytes=1024 * 1024,
        lease_seconds=30,
        handler_timeout_seconds=10,
        max_attempts=3,
        retry_base_seconds=1,
        retry_max_seconds=10,
        worker_id="worker-a",
    )
    event = EventEnvelope(
        event_type="document.chunked",
        tenant_id="default",
        aggregate_id=new_id(),
        trace_id=new_id(),
    )
    raw = event.model_dump_json().encode()
    assert (await processor.process(raw)).outcome == "processed"
    assert (await processor.process(raw)).outcome == "duplicate"
    assert handled == [event.event_id]

    unknown = event.model_copy(update={"event_id": new_id(), "event_version": 2})
    assert (await processor.process(unknown.model_dump_json().encode())).outcome == "dlq"
    assert (await processor.process(b"not-json")).outcome == "dlq"

    async with database.session() as session:
        inbox = (await session.scalars(select(InboxEventORM))).all()
        dlq = (await session.scalars(select(DeadLetterEventORM))).all()
        assert {row.status for row in inbox} == {"processed", "dlq"}
        assert {row.failure_kind for row in dlq} == {
            "unknown_event_version",
            "invalid_envelope",
        }


@pytest.mark.asyncio
async def test_index_event_handlers_rebuild_from_mysql_idempotently(database: Database) -> None:
    repository = SQLKnowledgeRepository(database)
    document, version, _, chunk = await create_knowledge(repository)
    provider = FakeModelProvider(dimension=4, version="v1")
    vector = InMemoryVectorStore(dimension=4, version="v1")
    graph = InMemoryGraphStore()
    event = EventEnvelope(
        event_type="document.chunked",
        aggregate_version=version.version,
        tenant_id="default",
        aggregate_id=document.document_id,
        trace_id=new_id(),
        payload_summary={
            "version_id": version.version_id,
            "version": version.version,
            "chunk_count": 1,
        },
    )
    vector_handler = VectorIndexEventHandler(
        repository=repository,
        embedding=provider,
        vector_store=vector,
        timeout_seconds=1,
    )
    graph_handler = GraphIndexEventHandler(
        repository=repository,
        extractor=provider,
        graph_store=graph,
        timeout_seconds=1,
    )

    await vector_handler(event)
    await vector_handler(event)
    await graph_handler(event)
    await graph_handler(event)

    assert await vector.list_ids("default") == {chunk.chunk_id}
    assert await graph.list_chunk_ids("default") == {chunk.chunk_id}
    stored = (await repository.list_chunks_for_version("default", version.version_id))[0]
    assert stored.vector_status.value == "succeeded"
    assert stored.graph_status.value == "succeeded"

    stale = event.model_copy(update={"aggregate_version": 2})
    with pytest.raises(Exception, match="版本"):
        await vector_handler(stale)


@pytest.mark.asyncio
async def test_rebuild_inventory_uses_only_mysql_active_versions(database: Database) -> None:
    repository = SQLKnowledgeRepository(database)
    document, version, _, chunk = await create_knowledge(repository)

    events, expected_ids = await _active_events(repository, "default")

    assert expected_ids == {chunk.chunk_id}
    assert len(events) == 1
    assert events[0].aggregate_id == document.document_id
    assert events[0].aggregate_version == version.version
    assert events[0].payload_summary["version_id"] == version.version_id


@pytest.mark.asyncio
async def test_sql_session_is_tenant_and_user_isolated(database: Database) -> None:
    service = SQLSessionService(database, model_version="fake")
    owner = IdentityContext(tenant_id="default", user_id="owner", roles=frozenset({"user"}))
    intruder = IdentityContext(tenant_id="default", user_id="intruder", roles=frozenset({"user"}))
    record = await service.create(owner)
    loaded = await service.get(owner, record.session_id)
    assert loaded.user_id == "owner"
    with pytest.raises(NotFoundError):
        await service.get(intruder, record.session_id)
    await service.close(owner, record.session_id)
    assert (await service.get(owner, record.session_id)).closed


@pytest.mark.asyncio
async def test_sql_session_turn_is_atomic_ordered_and_idempotent(database: Database) -> None:
    service = SQLSessionService(database, model_version="fake")
    owner = IdentityContext(tenant_id="default", user_id="owner", roles=frozenset({"user"}))
    session = await service.create(owner)
    claim = await service.begin_turn(
        owner,
        session.session_id,
        client_turn_id="client-turn-0001",
        request_id="request-0001",
        query="question",
    )
    assert claim.disposition is TurnDisposition.STARTED
    async with database.session() as db_session:
        stored_session = await db_session.get(SessionORM, session.session_id)
        run = await db_session.get(AgentRunORM, claim.run_id)
        messages = (await db_session.scalars(select(MessageORM))).all()
        assert stored_session is not None
        assert stored_session.revision == 1
        assert stored_session.active_run_id == claim.run_id
        assert run is not None and run.status == "running"
        assert [(message.sequence, message.role) for message in messages] == [(1, "user")]

    result = ChatResult(
        request_id=claim.request_id,
        run_id=claim.run_id,
        client_turn_id=claim.client_turn_id,
        session_id=session.session_id,
        status=AnswerStatus.ANSWERED,
        answer="answer",
        intent=AgentIntent.FAQ,
        source=SourceKind.REAL,
    )
    await service.complete_turn(owner, claim, result)
    replay = await service.begin_turn(
        owner,
        session.session_id,
        client_turn_id="client-turn-0001",
        request_id="request-0002",
        query="question",
    )
    loaded = await service.get(owner, session.session_id)
    assert replay.disposition is TurnDisposition.REPLAY
    assert replay.result == result
    assert [message.sequence for message in loaded.messages] == [1, 2]
    assert loaded.revision == 2
    assert loaded.active_run_id is None


@pytest.mark.asyncio
async def test_sql_session_concurrent_turns_have_single_winner(database: Database) -> None:
    service = SQLSessionService(database, model_version="fake")
    owner = IdentityContext(tenant_id="default", user_id="owner", roles=frozenset({"user"}))
    session = await service.create(owner)

    async def begin(index: int) -> TurnDisposition | str:
        try:
            claim = await service.begin_turn(
                owner,
                session.session_id,
                client_turn_id=f"client-turn-{index:04d}",
                request_id=f"request-{index:04d}",
                query=f"question {index}",
            )
            return claim.disposition
        except ConflictError:
            return "conflict"

    outcomes = await asyncio.gather(*(begin(index) for index in range(6)))
    assert outcomes.count(TurnDisposition.STARTED) == 1
    assert outcomes.count("conflict") == 5


@pytest.mark.asyncio
async def test_sql_session_persists_conversation_state_and_redacted_manifest(
    database: Database,
) -> None:
    service = SQLSessionService(database, model_version="fake")
    owner = IdentityContext(tenant_id="default", user_id="owner", roles=frozenset({"user"}))
    session = await service.create(owner)
    claim = await service.begin_turn(
        owner,
        session.session_id,
        client_turn_id="client-turn-memory-0001",
        request_id="request-memory-0001",
        query="remember this",
    )
    state = ConversationState(
        tenant_id="default",
        session_id=session.session_id,
        summary="Earlier discussion summary",
        summary_version=1,
        summary_through_sequence=claim.user_sequence,
        last_processed_sequence=claim.user_sequence,
    )
    manifest = (
        ContextAssembler(
            policy=ContextPolicy(model_context_window_tokens=1024, reserved_output_tokens=128),
            estimator=HeuristicTokenEstimator(),
        )
        .assemble(
            run_id=claim.run_id,
            tenant_id="default",
            session_id=session.session_id,
            identity=owner,
            model="fake",
            system_instructions="system",
            current_query="remember this",
            history=claim.history,
            conversation_state=state,
        )
        .manifest
    )
    result = ChatResult(
        request_id=claim.request_id,
        run_id=claim.run_id,
        client_turn_id=claim.client_turn_id,
        session_id=session.session_id,
        status=AnswerStatus.ANSWERED,
        answer="done",
        intent=AgentIntent.FAQ,
        source=SourceKind.REAL,
    )
    await service.complete_turn(
        owner,
        claim,
        result,
        conversation_state=state,
        context_manifest=manifest,
        generation_manifest=GenerationManifest(
            run_id=claim.run_id,
            tenant_id="default",
            prompt_bundle_version="prompt-bundle-v1",
            prompt_hashes={"agent-safety": "a" * 64},
            chat_model="fake",
            router_version="rule-router-v1",
            embedding_model="fake-embedding",
            embedding_version="v1",
            rerank_model="fake-rerank",
            state_version=2,
            context_policy_version="context-policy-v1",
            evaluation_set_version="memory-reliability-v1",
        ),
    )
    loaded = await service.get(owner, session.session_id)
    assert loaded.conversation_state == state
    async with database.session() as db_session:
        stored = await db_session.scalar(
            select(ContextManifestORM).where(ContextManifestORM.run_id == claim.run_id)
        )
        assert stored is not None
        assert stored.actual_input_tokens <= stored.input_budget_tokens
        assert "remember this" not in str(stored.dropped_items)
        generation = await db_session.scalar(
            select(GenerationManifestORM).where(GenerationManifestORM.run_id == claim.run_id)
        )
        assert generation is not None
        assert generation.prompt_bundle_version == "prompt-bundle-v1"


@pytest.mark.asyncio
async def test_sql_safe_resume_snapshot_is_idempotent_and_decision_locked(
    database: Database,
) -> None:
    store = SQLSafeResumeStore(database)
    snapshot = SafeResumeSnapshot(
        tenant_id="default",
        user_id="owner",
        roles=frozenset({"user"}),
        session_id=new_id(),
        run_id=new_id(),
        request_id="request-resume-1",
        client_turn_id="client-turn-resume-1",
        draft_id=new_id(),
        completed_side_effects=("refund.calculate.v1", "refund.create_draft.v1"),
        expires_at=utc_now() + timedelta(hours=1),
    )
    assert await store.save(snapshot) == snapshot
    assert await store.save(snapshot) == snapshot
    resumed = await store.mark_resumed(
        "default",
        snapshot.snapshot_id,
        decision="approved",
        result_hash="a" * 64,
        final_answer="fake approved; no execution",
        recovery_source="langgraph",
    )
    assert resumed.status == "resumed"
    assert resumed.decision == "approved"
    duplicate = await store.mark_resumed(
        "default",
        snapshot.snapshot_id,
        decision="approved",
        result_hash="a" * 64,
        final_answer="fake approved; no execution",
        recovery_source="langgraph",
    )
    assert duplicate == resumed
    with pytest.raises(ConflictError, match="不同决定"):
        await store.mark_resumed(
            "default",
            snapshot.snapshot_id,
            decision="rejected",
            result_hash="b" * 64,
            final_answer="rejected",
            recovery_source="langgraph",
        )


@pytest.mark.asyncio
async def test_sql_repository_task_index_keyword_draft_and_session_audit(
    database: Database,
) -> None:
    repository = SQLKnowledgeRepository(database)
    document, version, task, chunk = await create_knowledge(repository)
    assert await repository.get_document("default", document.document_id) == document
    assert await repository.get_version("default", version.version_id) == version
    assert (await repository.get_task("default", task.task_id)) is not None
    claimed = await repository.claim_pending_tasks(limit=1)
    assert claimed[0].attempt == 1
    assert (await repository.list_chunks_for_version("default", version.version_id))[0] == chunk
    identity = IdentityContext(tenant_id="default", user_id="owner", roles=frozenset({"user"}))
    keyword = await repository.keyword_search(identity, "warranty", top_k=3)
    assert keyword[0].chunk_id == chunk.chunk_id
    await repository.set_index_status(
        "default", [chunk.chunk_id], index="vector", succeeded=False, error="injected"
    )
    failed = claimed[0].model_copy(update={"status": IngestionStatus.FAILED})
    await repository.update_task(failed)
    assert (await repository.retry_task("default", task.task_id)).status == IngestionStatus.RECEIVED

    draft = ActionDraft(
        tenant_id="default",
        user_id="owner",
        action_type="ticket",
        idempotency_key="sql-draft-key",
        payload={},
        risk_level=RiskLevel.LOW_WRITE,
        expires_at=utc_now() + timedelta(minutes=10),
    )
    assert await repository.save_draft(draft) == draft
    assert await repository.save_draft(draft) == draft
    assert await repository.get_draft("default", draft.draft_id) == draft

    sessions = SQLSessionService(database, model_version="fake")
    session = await sessions.create(identity)
    result = ChatResult(
        request_id="request-1",
        run_id=new_id(),
        session_id=session.session_id,
        status=AnswerStatus.ANSWERED,
        answer="answer",
        intent=AgentIntent.FAQ,
        source=SourceKind.REAL,
    )
    await sessions.append(identity, session.session_id, "question", result)
    assert len((await sessions.get(identity, session.session_id)).messages) == 2
    audit = SQLAudit(database, default_tenant_id="default")
    await audit.record("test.event", {"request_id": "request-1", "token": "secret-value"})
    started = utc_now().isoformat()
    await audit.record(
        "agent.step",
        {
            "step_id": new_id(),
            "run_id": result.run_id,
            "tenant_id": "default",
            "node_name": "kb",
            "status": "succeeded",
            "input_summary": {"state_version": 1},
            "output_summary": {"updated_fields": ["answer"]},
            "started_at": started,
            "finished_at": utc_now().isoformat(),
        },
    )
    tool_call_id = new_id()
    await audit.record(
        "tool.order.query.v1",
        {
            "tool_call_id": tool_call_id,
            "run_id": result.run_id,
            "tenant_id": "default",
            "tool_name": "order.query.v1",
            "schema_version": 1,
            "risk_level": "read",
            "status": "retrying",
            "input_keys": ["query"],
        },
    )
    await audit.record(
        "tool.order.query.v1",
        {
            "tool_call_id": tool_call_id,
            "run_id": result.run_id,
            "tenant_id": "default",
            "tool_name": "order.query.v1",
            "schema_version": 1,
            "risk_level": "read",
            "status": "succeeded",
            "input_keys": ["query"],
            "attempts": 2,
            "retries": 1,
        },
    )
    async with database.session() as db_session:
        assert len((await db_session.scalars(select(AgentStepORM))).all()) == 1
        logs = (await db_session.scalars(select(ToolCallLogORM))).all()
        assert len(logs) == 1
        assert logs[0].status == "succeeded"
        assert logs[0].output_summary["retries"] == 1


@pytest.mark.asyncio
async def test_sql_document_versions_activate_and_inactivate(database: Database) -> None:
    repository = SQLKnowledgeRepository(database)
    document, _, _, first_chunk = await create_knowledge(repository)
    second = DocumentVersion(
        document_id=document.document_id,
        tenant_id="default",
        version=2,
        object_key="doc/v2/original.txt",
        content_hash="c" * 64,
        mime_type="text/plain",
        size_bytes=12,
    )
    task = IngestionTask(
        tenant_id="default", document_id=document.document_id, version_id=second.version_id
    )
    await repository.create_version(
        second,
        task,
        EventEnvelope(
            event_type="document.version.received",
            tenant_id="default",
            aggregate_id=document.document_id,
            trace_id=new_id(),
        ),
    )
    second_chunk = ChunkRecord(
        tenant_id="default",
        document_id=document.document_id,
        version_id=second.version_id,
        ordinal=0,
        content="new policy",
        content_hash="d" * 64,
        embedding_model="fake",
        embedding_dimension=4,
        embedding_version="v1",
    )
    await repository.save_chunks(
        [second_chunk],
        EventEnvelope(
            event_type="document.chunked",
            tenant_id="default",
            aggregate_id=document.document_id,
            trace_id=new_id(),
        ),
    )
    await repository.activate_version("default", document.document_id, second.version_id)
    versions = await repository.list_versions("default", document.document_id)
    assert [item.version for item in versions] == [1, 2]
    assert versions[0].valid_until is not None
    identity = IdentityContext(tenant_id="default", user_id="u", roles=frozenset({"user"}))
    assert await repository.authorize_chunks(identity, [first_chunk.chunk_id]) == []
    assert [
        item.chunk_id
        for item in await repository.authorize_chunks(identity, [second_chunk.chunk_id])
    ] == [second_chunk.chunk_id]
    assert (await repository.list_documents("default", offset=0, limit=10))[0].document_id == (
        document.document_id
    )

    event = EventEnvelope(
        event_type="document.inactivated",
        tenant_id="default",
        aggregate_id=document.document_id,
        trace_id=new_id(),
    )
    await repository.inactivate_document("default", document.document_id, event)
    assert (await repository.get_document("default", document.document_id)).status.value == (
        "inactive"
    )
    assert await repository.authorize_chunks(identity, [second_chunk.chunk_id]) == []
    async with database.session() as session:
        assert await session.get(OutboxEventORM, event.event_id) is not None
