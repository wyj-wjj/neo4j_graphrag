"""SQLAlchemy async lifecycle and MySQL truth-source repository."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    exists,
    or_,
    select,
)
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, aliased, mapped_column

from graphrag.domain.errors import ConflictError, NotFoundError
from graphrag.domain.events import EventEnvelope
from graphrag.domain.models import (
    ActionDraft,
    ChunkRecord,
    DocumentRecord,
    DocumentStatus,
    DocumentVersion,
    IdentityContext,
    IndexStatus,
    IngestionStatus,
    IngestionTask,
    SearchCandidate,
    utc_now,
)


class Base(DeclarativeBase):
    pass


class DocumentORM(Base):
    __tablename__ = "documents"
    document_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class DocumentVersionORM(Base):
    __tablename__ = "document_versions"
    __table_args__ = (
        UniqueConstraint("tenant_id", "content_hash", name="uq_document_version_tenant_hash"),
        UniqueConstraint("document_id", "version", name="uq_document_version_number"),
    )
    version_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    document_id: Mapped[str] = mapped_column(
        ForeignKey("documents.document_id", ondelete="CASCADE"), nullable=False, index=True
    )
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    object_key: Mapped[str] = mapped_column(String(1000), nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    mime_type: Mapped[str] = mapped_column(String(255), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    valid_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class IngestionTaskORM(Base):
    __tablename__ = "ingestion_tasks"
    task_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    document_id: Mapped[str] = mapped_column(
        ForeignKey("documents.document_id", ondelete="CASCADE"), nullable=False, index=True
    )
    version_id: Mapped[str | None] = mapped_column(
        ForeignKey("document_versions.version_id", ondelete="CASCADE"), index=True
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_code: Mapped[str | None] = mapped_column(String(100))
    error_message: Mapped[str | None] = mapped_column(String(500))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ChunkORM(Base):
    __tablename__ = "chunks"
    __table_args__ = (
        UniqueConstraint("version_id", "ordinal", name="uq_chunk_version_ordinal"),
        Index("ix_chunks_tenant_status_valid", "tenant_id", "status", "valid_until"),
    )
    chunk_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    document_id: Mapped[str] = mapped_column(
        ForeignKey("documents.document_id", ondelete="CASCADE"), nullable=False, index=True
    )
    version_id: Mapped[str] = mapped_column(
        ForeignKey("document_versions.version_id", ondelete="CASCADE"), nullable=False, index=True
    )
    parent_chunk_id: Mapped[str | None] = mapped_column(
        ForeignKey("chunks.chunk_id", ondelete="SET NULL"), index=True
    )
    chunk_kind: Mapped[str] = mapped_column(String(16), nullable=False, default="child")
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    title_path: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    source_location: Mapped[str] = mapped_column(String(1000), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    embedding_model: Mapped[str] = mapped_column(String(255), nullable=False)
    embedding_dimension: Mapped[int] = mapped_column(Integer, nullable=False)
    embedding_version: Mapped[str] = mapped_column(String(100), nullable=False)
    vector_status: Mapped[str] = mapped_column(String(32), nullable=False)
    graph_status: Mapped[str] = mapped_column(String(32), nullable=False)
    vector_error: Mapped[str | None] = mapped_column(String(255))
    graph_error: Mapped[str | None] = mapped_column(String(255))
    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    valid_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ChunkACLORM(Base):
    __tablename__ = "chunk_acl"
    acl_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chunk_id: Mapped[str] = mapped_column(
        ForeignKey("chunks.chunk_id", ondelete="CASCADE"), nullable=False, index=True
    )
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    allowed_role: Mapped[str | None] = mapped_column(String(128), index=True)
    allowed_department: Mapped[str | None] = mapped_column(String(128), index=True)
    deny: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    valid_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OutboxEventORM(Base):
    __tablename__ = "outbox_events"
    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    event_version: Mapped[int] = mapped_column(Integer, nullable=False)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    aggregate_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    trace_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    payload_summary: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    published: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class ActionDraftORM(Base):
    __tablename__ = "action_drafts"
    __table_args__ = (
        UniqueConstraint("tenant_id", "idempotency_key", name="uq_draft_tenant_idempotency"),
    )
    draft_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    user_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    action_type: Mapped[str] = mapped_column(String(50), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(200), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    risk_level: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    source: Mapped[str] = mapped_column(String(16), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ApprovalORM(Base):
    __tablename__ = "approval_requests"
    approval_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    draft_id: Mapped[str] = mapped_column(
        ForeignKey("action_drafts.draft_id", ondelete="CASCADE"), nullable=False, index=True
    )
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    decision_actor_id: Mapped[str | None] = mapped_column(String(128))
    idempotency_key: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class SessionORM(Base):
    __tablename__ = "sessions"
    session_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    user_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    revision: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_message_sequence: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    active_run_id: Mapped[str | None] = mapped_column(String(36), index=True)
    context_policy_version: Mapped[str] = mapped_column(
        String(100), nullable=False, default="context-policy-v1"
    )
    conversation_state: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    summary: Mapped[str] = mapped_column(Text, nullable=False, default="")
    summary_version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    summary_through_sequence: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class MessageORM(Base):
    __tablename__ = "messages"
    __table_args__ = (
        UniqueConstraint("tenant_id", "session_id", "sequence", name="uq_message_session_sequence"),
        UniqueConstraint(
            "tenant_id",
            "session_id",
            "client_turn_id",
            "role",
            name="uq_message_turn_role",
        ),
    )
    message_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    session_id: Mapped[str] = mapped_column(
        ForeignKey("sessions.session_id", ondelete="CASCADE"), nullable=False, index=True
    )
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    client_turn_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    run_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="committed")
    content: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AgentRunORM(Base):
    __tablename__ = "agent_runs"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "session_id", "client_turn_id", name="uq_agent_run_client_turn"
        ),
    )
    run_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    request_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    session_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    client_turn_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    result_hash: Mapped[str | None] = mapped_column(String(64))
    result_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    error_code: Mapped[str | None] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    model_version: Mapped[str] = mapped_column(String(255), nullable=False)
    prompt_version: Mapped[str] = mapped_column(String(100), nullable=False)
    state_version: Mapped[int] = mapped_column(Integer, nullable=False, default=2)
    checkpoint_version: Mapped[str] = mapped_column(
        String(100), nullable=False, default="agent-state-v2"
    )
    context_policy_version: Mapped[str] = mapped_column(
        String(100), nullable=False, default="context-policy-v1"
    )
    recovery_source: Mapped[str] = mapped_column(String(32), nullable=False, default="fresh")
    session_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ContextManifestORM(Base):
    __tablename__ = "context_manifests"
    __table_args__ = (UniqueConstraint("tenant_id", "run_id", name="uq_context_manifest_run"),)
    manifest_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    session_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    manifest_version: Mapped[int] = mapped_column(Integer, nullable=False)
    model: Mapped[str] = mapped_column(String(255), nullable=False)
    context_policy_version: Mapped[str] = mapped_column(String(100), nullable=False)
    model_context_window_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    reserved_output_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    input_budget_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    target_tokens: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    actual_tokens: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    actual_input_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    borrowed_tokens: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    selected_message_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    selected_evidence_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    selected_tool_result_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    dropped_items: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class GenerationManifestORM(Base):
    __tablename__ = "generation_manifests"
    __table_args__ = (UniqueConstraint("tenant_id", "run_id", name="uq_generation_manifest_run"),)
    manifest_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    manifest_version: Mapped[int] = mapped_column(Integer, nullable=False)
    prompt_bundle_version: Mapped[str] = mapped_column(String(100), nullable=False)
    prompt_hashes: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    chat_model: Mapped[str] = mapped_column(String(255), nullable=False)
    router_version: Mapped[str] = mapped_column(String(100), nullable=False)
    embedding_model: Mapped[str] = mapped_column(String(255), nullable=False)
    embedding_version: Mapped[str] = mapped_column(String(100), nullable=False)
    rerank_model: Mapped[str] = mapped_column(String(255), nullable=False)
    state_version: Mapped[int] = mapped_column(Integer, nullable=False)
    context_policy_version: Mapped[str] = mapped_column(String(100), nullable=False)
    evaluation_set_version: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class UserMemorySettingsORM(Base):
    __tablename__ = "user_memory_settings"
    tenant_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    auto_write_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class LongTermMemoryORM(Base):
    __tablename__ = "long_term_memories"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "user_id", "key", "version", name="uq_long_memory_key_version"
        ),
        Index(
            "ix_long_memory_active",
            "tenant_id",
            "user_id",
            "status",
            "expires_at",
        ),
    )
    memory_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    user_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    category: Mapped[str] = mapped_column(String(32), nullable=False)
    key: Mapped[str] = mapped_column(String(100), nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    source_turn_id: Mapped[str] = mapped_column(String(128), nullable=False)
    confirmation_method: Mapped[str] = mapped_column(String(32), nullable=False)
    confirmed_by: Mapped[str] = mapped_column(String(128), nullable=False)
    confidence: Mapped[float] = mapped_column(nullable=False)
    sensitivity: Mapped[str] = mapped_column(String(32), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    supersedes_memory_id: Mapped[str | None] = mapped_column(String(36))
    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class SafeResumeSnapshotORM(Base):
    __tablename__ = "safe_resume_snapshots"
    __table_args__ = (UniqueConstraint("tenant_id", "draft_id", name="uq_safe_resume_draft"),)
    snapshot_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    user_id: Mapped[str] = mapped_column(String(128), nullable=False)
    roles: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    session_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    run_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    request_id: Mapped[str] = mapped_column(String(128), nullable=False)
    client_turn_id: Mapped[str] = mapped_column(String(128), nullable=False)
    draft_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    safe_node: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    decision: Mapped[str | None] = mapped_column(String(32))
    next_action: Mapped[str] = mapped_column(String(100), nullable=False)
    completed_side_effects: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    state_version: Mapped[int] = mapped_column(Integer, nullable=False)
    checkpoint_version: Mapped[str] = mapped_column(String(100), nullable=False)
    recovery_source: Mapped[str] = mapped_column(String(32), nullable=False)
    resume_result_hash: Mapped[str | None] = mapped_column(String(64))
    final_answer: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )


class AgentStepORM(Base):
    __tablename__ = "agent_steps"
    step_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    node_name: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    input_summary: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    output_summary: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ToolCallLogORM(Base):
    __tablename__ = "tool_call_logs"
    tool_call_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    tool_name: Mapped[str] = mapped_column(String(255), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    risk_level: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    input_summary: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    output_summary: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class FAQItemORM(Base):
    __tablename__ = "faq_items"
    faq_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    question: Mapped[str] = mapped_column(String(1000), nullable=False)
    answer: Mapped[str] = mapped_column(Text, nullable=False)
    patterns: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    valid_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AuditEventORM(Base):
    __tablename__ = "audit_events"
    audit_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    request_id: Mapped[str | None] = mapped_column(String(128), index=True)
    fields: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Database:
    def __init__(self, url: str, *, pool_size: int = 10) -> None:
        kwargs: dict[str, Any] = {"pool_pre_ping": True}
        if not url.startswith("sqlite"):
            kwargs["pool_size"] = pool_size
        self.engine: AsyncEngine = create_async_engine(url, **kwargs)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        async with self.sessions() as session:
            try:
                yield session
                await session.commit()
            except BaseException:
                await session.rollback()
                raise

    async def close(self) -> None:
        await self.engine.dispose()

    async def health(self) -> bool:
        async with self.engine.connect() as connection:
            await connection.execute(select(1))
        return True


class SQLKnowledgeRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def create_document(
        self,
        document: DocumentRecord,
        version: DocumentVersion,
        task: IngestionTask,
        event: EventEnvelope,
    ) -> None:
        async with self.database.session() as session:
            duplicate = await session.scalar(
                select(DocumentVersionORM.version_id).where(
                    DocumentVersionORM.tenant_id == version.tenant_id,
                    DocumentVersionORM.content_hash == version.content_hash,
                )
            )
            if duplicate is not None:
                raise ConflictError("同一租户中已存在相同内容的文档版本")
            session.add(DocumentORM(**document.model_dump(mode="python")))
            session.add(DocumentVersionORM(**version.model_dump(mode="python")))
            session.add(IngestionTaskORM(**task.model_dump(mode="python")))
            session.add(
                OutboxEventORM(**event.model_dump(mode="python"), published=False, retry_count=0)
            )

    async def create_version(
        self, version: DocumentVersion, task: IngestionTask, event: EventEnvelope
    ) -> None:
        async with self.database.session() as session:
            document = await session.scalar(
                select(DocumentORM)
                .where(
                    DocumentORM.document_id == version.document_id,
                    DocumentORM.tenant_id == version.tenant_id,
                )
                .with_for_update()
            )
            if document is None:
                raise NotFoundError("文档不存在")
            duplicate = await session.scalar(
                select(DocumentVersionORM.version_id).where(
                    DocumentVersionORM.tenant_id == version.tenant_id,
                    DocumentVersionORM.content_hash == version.content_hash,
                )
            )
            if duplicate is not None:
                raise ConflictError("同一租户中已存在相同内容的文档版本")
            existing_number = await session.scalar(
                select(DocumentVersionORM.version_id).where(
                    DocumentVersionORM.document_id == version.document_id,
                    DocumentVersionORM.version == version.version,
                )
            )
            if existing_number is not None:
                raise ConflictError("文档版本号冲突，请重试")
            session.add(DocumentVersionORM(**version.model_dump(mode="python")))
            session.add(IngestionTaskORM(**task.model_dump(mode="python")))
            session.add(
                OutboxEventORM(**event.model_dump(mode="python"), published=False, retry_count=0)
            )

    async def list_documents(
        self, tenant_id: str, *, offset: int, limit: int
    ) -> list[DocumentRecord]:
        async with self.database.session() as session:
            rows = (
                await session.scalars(
                    select(DocumentORM)
                    .where(DocumentORM.tenant_id == tenant_id)
                    .order_by(DocumentORM.created_at.desc(), DocumentORM.document_id.desc())
                    .offset(offset)
                    .limit(limit)
                )
            ).all()
            return [self._document(row) for row in rows]

    async def list_versions(self, tenant_id: str, document_id: str) -> list[DocumentVersion]:
        async with self.database.session() as session:
            document = await session.scalar(
                select(DocumentORM.document_id).where(
                    DocumentORM.document_id == document_id,
                    DocumentORM.tenant_id == tenant_id,
                )
            )
            if document is None:
                raise NotFoundError("文档不存在")
            rows = (
                await session.scalars(
                    select(DocumentVersionORM)
                    .where(
                        DocumentVersionORM.tenant_id == tenant_id,
                        DocumentVersionORM.document_id == document_id,
                    )
                    .order_by(DocumentVersionORM.version)
                )
            ).all()
            return [self._version(row) for row in rows]

    async def activate_version(self, tenant_id: str, document_id: str, version_id: str) -> None:
        now = utc_now()
        async with self.database.session() as session:
            document = await session.scalar(
                select(DocumentORM)
                .where(
                    DocumentORM.document_id == document_id,
                    DocumentORM.tenant_id == tenant_id,
                )
                .with_for_update()
            )
            target = await session.scalar(
                select(DocumentVersionORM).where(
                    DocumentVersionORM.version_id == version_id,
                    DocumentVersionORM.document_id == document_id,
                    DocumentVersionORM.tenant_id == tenant_id,
                )
            )
            if document is None or target is None:
                raise NotFoundError("文档版本不存在")
            versions = (
                await session.scalars(
                    select(DocumentVersionORM).where(
                        DocumentVersionORM.document_id == document_id,
                        DocumentVersionORM.tenant_id == tenant_id,
                        DocumentVersionORM.version_id != version_id,
                        DocumentVersionORM.valid_until.is_(None),
                    )
                )
            ).all()
            for version in versions:
                version.valid_until = now
            chunks = (
                await session.scalars(
                    select(ChunkORM).where(
                        ChunkORM.document_id == document_id,
                        ChunkORM.tenant_id == tenant_id,
                        ChunkORM.version_id != version_id,
                    )
                )
            ).all()
            for chunk in chunks:
                chunk.status = DocumentStatus.INACTIVE.value
                chunk.valid_until = now
            document.status = DocumentStatus.ACTIVE.value
            document.updated_at = now

    async def inactivate_document(
        self, tenant_id: str, document_id: str, event: EventEnvelope
    ) -> None:
        now = utc_now()
        async with self.database.session() as session:
            document = await session.scalar(
                select(DocumentORM)
                .where(
                    DocumentORM.document_id == document_id,
                    DocumentORM.tenant_id == tenant_id,
                )
                .with_for_update()
            )
            if document is None:
                raise NotFoundError("文档不存在")
            document.status = DocumentStatus.INACTIVE.value
            document.updated_at = now
            versions = (
                await session.scalars(
                    select(DocumentVersionORM).where(
                        DocumentVersionORM.document_id == document_id,
                        DocumentVersionORM.tenant_id == tenant_id,
                        DocumentVersionORM.valid_until.is_(None),
                    )
                )
            ).all()
            for version in versions:
                version.valid_until = now
            chunks = (
                await session.scalars(
                    select(ChunkORM).where(
                        ChunkORM.document_id == document_id,
                        ChunkORM.tenant_id == tenant_id,
                    )
                )
            ).all()
            for chunk in chunks:
                chunk.status = DocumentStatus.INACTIVE.value
                chunk.valid_until = now
            session.add(
                OutboxEventORM(**event.model_dump(mode="python"), published=False, retry_count=0)
            )

    async def save_chunks(self, chunks: Sequence[ChunkRecord], event: EventEnvelope) -> None:
        async with self.database.session() as session:
            for chunk in chunks:
                values = chunk.model_dump(mode="python")
                values["title_path"] = list(chunk.title_path)
                session.add(ChunkORM(**values))
                session.add(
                    ChunkACLORM(
                        chunk_id=chunk.chunk_id,
                        tenant_id=chunk.tenant_id,
                        allowed_role="user",
                        allowed_department=None,
                        deny=False,
                        valid_from=chunk.valid_from,
                        valid_until=chunk.valid_until,
                    )
                )
                session.add(
                    ChunkACLORM(
                        chunk_id=chunk.chunk_id,
                        tenant_id=chunk.tenant_id,
                        allowed_role="admin",
                        allowed_department=None,
                        deny=False,
                        valid_from=chunk.valid_from,
                        valid_until=chunk.valid_until,
                    )
                )
            event_values = event.model_dump(mode="python")
            session.add(OutboxEventORM(**event_values, published=False, retry_count=0))

    async def update_task(self, task: IngestionTask) -> None:
        async with self.database.session() as session:
            stored = await session.get(IngestionTaskORM, task.task_id)
            if stored is None or stored.tenant_id != task.tenant_id:
                return
            for key, value in task.model_dump(mode="python").items():
                setattr(stored, key, value)
            stored.updated_at = utc_now()

    async def get_task(self, tenant_id: str, task_id: str) -> IngestionTask | None:
        async with self.database.session() as session:
            row = await session.scalar(
                select(IngestionTaskORM).where(
                    IngestionTaskORM.task_id == task_id,
                    IngestionTaskORM.tenant_id == tenant_id,
                )
            )
            return self._task(row) if row is not None else None

    async def retry_task(self, tenant_id: str, task_id: str) -> IngestionTask:
        async with self.database.session() as session:
            row = await session.scalar(
                select(IngestionTaskORM)
                .where(
                    IngestionTaskORM.task_id == task_id,
                    IngestionTaskORM.tenant_id == tenant_id,
                )
                .with_for_update()
            )
            if row is None:
                raise NotFoundError("入库任务不存在")
            if row.status not in {
                IngestionStatus.FAILED.value,
                IngestionStatus.PARTIAL_FAILED.value,
            }:
                raise ConflictError("只有失败或部分失败的入库任务可以重试")
            row.status = IngestionStatus.RECEIVED.value
            row.error_code = None
            row.error_message = None
            row.updated_at = utc_now()
            return self._task(row)

    async def get_version(self, tenant_id: str, version_id: str) -> DocumentVersion | None:
        async with self.database.session() as session:
            row = await session.scalar(
                select(DocumentVersionORM).where(
                    DocumentVersionORM.version_id == version_id,
                    DocumentVersionORM.tenant_id == tenant_id,
                )
            )
            return self._version(row) if row is not None else None

    async def get_document(self, tenant_id: str, document_id: str) -> DocumentRecord | None:
        async with self.database.session() as session:
            row = await session.scalar(
                select(DocumentORM).where(
                    DocumentORM.document_id == document_id,
                    DocumentORM.tenant_id == tenant_id,
                )
            )
            return self._document(row) if row is not None else None

    async def claim_pending_tasks(self, *, limit: int) -> list[IngestionTask]:
        async with self.database.session() as session:
            rows = (
                await session.scalars(
                    select(IngestionTaskORM)
                    .where(IngestionTaskORM.status == IngestionStatus.RECEIVED.value)
                    .order_by(IngestionTaskORM.created_at)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            result = []
            for row in rows:
                row.status = IngestionStatus.PARSING.value
                row.attempt += 1
                row.updated_at = utc_now()
                result.append(self._task(row))
            return result

    async def list_chunks_for_version(self, tenant_id: str, version_id: str) -> list[ChunkRecord]:
        async with self.database.session() as session:
            rows = (
                await session.scalars(
                    select(ChunkORM)
                    .where(ChunkORM.tenant_id == tenant_id, ChunkORM.version_id == version_id)
                    .order_by(ChunkORM.ordinal)
                )
            ).all()
            return [self._chunk(row) for row in rows]

    async def authorize_chunks(
        self, identity: IdentityContext, chunk_ids: Sequence[str]
    ) -> list[ChunkRecord]:
        if not chunk_ids:
            return []
        now = utc_now()
        async with self.database.session() as session:
            denied_acl = aliased(ChunkACLORM)
            deny_exists = exists(
                select(denied_acl.acl_id).where(
                    denied_acl.chunk_id == ChunkORM.chunk_id,
                    denied_acl.tenant_id == identity.tenant_id,
                    denied_acl.deny.is_(True),
                    denied_acl.valid_from <= now,
                    or_(denied_acl.valid_until.is_(None), denied_acl.valid_until > now),
                    or_(
                        denied_acl.allowed_role.is_(None),
                        denied_acl.allowed_role.in_(identity.roles),
                    ),
                    or_(
                        denied_acl.allowed_department.is_(None),
                        denied_acl.allowed_department.in_(identity.departments),
                    ),
                )
            )
            rows = (
                await session.scalars(
                    select(ChunkORM)
                    .join(ChunkACLORM, ChunkACLORM.chunk_id == ChunkORM.chunk_id)
                    .where(
                        ChunkORM.chunk_id.in_(set(chunk_ids)),
                        ChunkORM.tenant_id == identity.tenant_id,
                        ChunkORM.status == DocumentStatus.ACTIVE.value,
                        ChunkORM.valid_from <= now,
                        or_(ChunkORM.valid_until.is_(None), ChunkORM.valid_until > now),
                        ChunkACLORM.tenant_id == identity.tenant_id,
                        ChunkACLORM.deny.is_(False),
                        ChunkACLORM.valid_from <= now,
                        or_(ChunkACLORM.valid_until.is_(None), ChunkACLORM.valid_until > now),
                        or_(
                            ChunkACLORM.allowed_role.is_(None),
                            ChunkACLORM.allowed_role.in_(identity.roles),
                        ),
                        or_(
                            ChunkACLORM.allowed_department.is_(None),
                            ChunkACLORM.allowed_department.in_(identity.departments),
                        ),
                        ~deny_exists,
                    )
                    .distinct()
                )
            ).all()
        by_id = {row.chunk_id: self._chunk(row) for row in rows}
        return [by_id[item] for item in dict.fromkeys(chunk_ids) if item in by_id]

    async def keyword_search(
        self, identity: IdentityContext, query: str, *, top_k: int
    ) -> list[SearchCandidate]:
        terms = [term for term in query.split() if term][:10]
        if not terms:
            return []
        async with self.database.session() as session:
            condition = or_(*(ChunkORM.content.like(f"%{term}%") for term in terms))
            rows = (
                await session.scalars(
                    select(ChunkORM)
                    .where(ChunkORM.tenant_id == identity.tenant_id, condition)
                    .limit(top_k * 2)
                )
            ).all()
        allowed = await self.authorize_chunks(identity, [row.chunk_id for row in rows])
        return [
            SearchCandidate(chunk_id=item.chunk_id, score=1.0, source="keyword", rank=rank)
            for rank, item in enumerate(allowed[:top_k], start=1)
        ]

    async def set_index_status(
        self,
        tenant_id: str,
        chunk_ids: Sequence[str],
        *,
        index: str,
        succeeded: bool,
        error: str | None = None,
    ) -> None:
        if index not in {"vector", "graph"}:
            raise ValueError("index must be vector or graph")
        async with self.database.session() as session:
            rows = (
                await session.scalars(
                    select(ChunkORM).where(
                        ChunkORM.tenant_id == tenant_id, ChunkORM.chunk_id.in_(set(chunk_ids))
                    )
                )
            ).all()
            for row in rows:
                setattr(
                    row,
                    f"{index}_status",
                    IndexStatus.SUCCEEDED.value if succeeded else IndexStatus.FAILED.value,
                )
                setattr(row, f"{index}_error", error)

    async def save_draft(self, draft: ActionDraft) -> ActionDraft:
        async with self.database.session() as session:
            existing = await session.scalar(
                select(ActionDraftORM).where(
                    ActionDraftORM.tenant_id == draft.tenant_id,
                    ActionDraftORM.idempotency_key == draft.idempotency_key,
                )
            )
            if existing is not None:
                return self._draft(existing)
            values = draft.model_dump(mode="python")
            session.add(ActionDraftORM(**values))
        return draft

    async def get_draft(self, tenant_id: str, draft_id: str) -> ActionDraft | None:
        async with self.database.session() as session:
            row = await session.scalar(
                select(ActionDraftORM).where(
                    ActionDraftORM.draft_id == draft_id,
                    ActionDraftORM.tenant_id == tenant_id,
                )
            )
            return self._draft(row) if row is not None else None

    @staticmethod
    def _document(row: DocumentORM) -> DocumentRecord:
        SQLKnowledgeRepository._ensure_utc(row, "created_at", "updated_at")
        return DocumentRecord.model_validate(row, from_attributes=True)

    @staticmethod
    def _version(row: DocumentVersionORM) -> DocumentVersion:
        SQLKnowledgeRepository._ensure_utc(row, "valid_from", "valid_until", "created_at")
        return DocumentVersion.model_validate(row, from_attributes=True)

    @staticmethod
    def _task(row: IngestionTaskORM) -> IngestionTask:
        SQLKnowledgeRepository._ensure_utc(row, "created_at", "updated_at")
        return IngestionTask.model_validate(row, from_attributes=True)

    @staticmethod
    def _chunk(row: ChunkORM) -> ChunkRecord:
        SQLKnowledgeRepository._ensure_utc(row, "valid_from", "valid_until")
        return ChunkRecord.model_validate(row, from_attributes=True)

    @staticmethod
    def _draft(row: ActionDraftORM) -> ActionDraft:
        SQLKnowledgeRepository._ensure_utc(row, "created_at", "expires_at")
        return ActionDraft.model_validate(row, from_attributes=True)

    @staticmethod
    def _ensure_utc(row: Any, *fields: str) -> None:
        for field in fields:
            value = getattr(row, field, None)
            if isinstance(value, datetime) and value.tzinfo is None:
                setattr(row, field, value.replace(tzinfo=UTC))
