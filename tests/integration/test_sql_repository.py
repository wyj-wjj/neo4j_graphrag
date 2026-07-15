from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from graphrag.domain.errors import NotFoundError
from graphrag.domain.events import EventEnvelope
from graphrag.domain.ids import new_id
from graphrag.domain.models import (
    ActionDraft,
    AgentIntent,
    AnswerStatus,
    ChatResult,
    ChunkRecord,
    DocumentRecord,
    DocumentVersion,
    IdentityContext,
    IngestionStatus,
    IngestionTask,
    RiskLevel,
    SourceKind,
    utc_now,
)
from graphrag.infrastructure.database import (
    AgentStepORM,
    Base,
    ChunkACLORM,
    Database,
    OutboxEventORM,
    SQLKnowledgeRepository,
    ToolCallLogORM,
)
from graphrag.infrastructure.sql_services import SQLAudit, SQLSessionService


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
    await audit.record(
        "tool.order.query.v1",
        {
            "tool_call_id": new_id(),
            "run_id": result.run_id,
            "tenant_id": "default",
            "tool_name": "order.query.v1",
            "schema_version": 1,
            "risk_level": "read",
            "status": "succeeded",
            "input_keys": ["query"],
        },
    )
    async with database.session() as db_session:
        assert len((await db_session.scalars(select(AgentStepORM))).all()) == 1
        assert len((await db_session.scalars(select(ToolCallLogORM))).all()) == 1


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
