"""MySQL-backed session history and durable audit adapters."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy import or_, select

from graphrag.application.sessions import (
    SessionRecord,
    TurnClaim,
    TurnDisposition,
    TurnStatus,
)
from graphrag.domain.errors import ConflictError, NotFoundError
from graphrag.domain.ids import new_id
from graphrag.domain.models import (
    ChatResult,
    ContextManifest,
    ConversationState,
    GenerationManifest,
    IdentityContext,
    LongTermMemory,
    MemoryCategory,
    MemorySettings,
    MemoryStatus,
    SafeResumeSnapshot,
    SessionMessage,
    utc_now,
)
from graphrag.infrastructure.database import (
    AgentRunORM,
    AgentStepORM,
    AuditEventORM,
    ContextManifestORM,
    Database,
    GenerationManifestORM,
    LongTermMemoryORM,
    MessageORM,
    SafeResumeSnapshotORM,
    SessionORM,
    ToolCallLogORM,
    UserMemorySettingsORM,
)
from graphrag.observability.redaction import redact


class SQLSessionService:
    def __init__(
        self,
        database: Database,
        *,
        model_version: str,
        prompt_version: str = "prompt-bundle-v1",
    ) -> None:
        self.database = database
        self.model_version = model_version
        self.prompt_version = prompt_version
        self._session_locks: dict[str, asyncio.Lock] = {}

    async def create(self, identity: IdentityContext) -> SessionRecord:
        now = utc_now()
        record = SessionRecord(new_id(), identity.tenant_id, identity.user_id)
        record.conversation_state = ConversationState(
            tenant_id=identity.tenant_id,
            session_id=record.session_id,
        )
        async with self.database.session() as session:
            session.add(
                SessionORM(
                    session_id=record.session_id,
                    tenant_id=record.tenant_id,
                    user_id=record.user_id,
                    status="active",
                    revision=0,
                    last_message_sequence=0,
                    active_run_id=None,
                    context_policy_version=record.context_policy_version,
                    conversation_state=record.conversation_state.model_dump(mode="json")
                    if record.conversation_state
                    else None,
                    summary="",
                    summary_version=0,
                    summary_through_sequence=0,
                    created_at=now,
                    updated_at=now,
                )
            )
        return record

    async def get(self, identity: IdentityContext, session_id: str) -> SessionRecord:
        async with self.database.session() as session:
            stored = await session.scalar(
                select(SessionORM).where(
                    SessionORM.session_id == session_id,
                    SessionORM.tenant_id == identity.tenant_id,
                )
            )
            if stored is None or (
                stored.user_id != identity.user_id and not identity.has_role("admin")
            ):
                raise NotFoundError("会话不存在")
            rows = (
                await session.scalars(
                    select(MessageORM)
                    .where(
                        MessageORM.session_id == session_id,
                        MessageORM.tenant_id == identity.tenant_id,
                    )
                    .order_by(MessageORM.sequence)
                )
            ).all()
            runs = (
                await session.scalars(
                    select(AgentRunORM)
                    .where(
                        AgentRunORM.session_id == session_id,
                        AgentRunORM.tenant_id == identity.tenant_id,
                        AgentRunORM.status == TurnStatus.COMPLETED.value,
                    )
                    .order_by(AgentRunORM.started_at, AgentRunORM.run_id)
                )
            ).all()
            record = SessionRecord(
                session_id=stored.session_id,
                tenant_id=stored.tenant_id,
                user_id=stored.user_id,
                messages=[
                    SessionMessage(
                        message_id=row.message_id,
                        client_turn_id=row.client_turn_id,
                        run_id=row.run_id,
                        sequence=row.sequence,
                        role=cast(Any, row.role),
                        content=row.content,
                        status="committed",
                        content_hash=row.content_hash,
                        created_at=(
                            row.created_at
                            if row.created_at.tzinfo is not None
                            else row.created_at.replace(tzinfo=UTC)
                        ),
                    )
                    for row in rows
                ],
                results=[
                    ChatResult.model_validate(run.result_payload)
                    for run in runs
                    if run.result_payload is not None
                ],
                closed=stored.status == "closed",
                revision=stored.revision,
                last_message_sequence=stored.last_message_sequence,
                active_run_id=stored.active_run_id,
                context_policy_version=stored.context_policy_version,
                conversation_state=(
                    ConversationState.model_validate(stored.conversation_state)
                    if stored.conversation_state is not None
                    else ConversationState(
                        tenant_id=stored.tenant_id,
                        session_id=stored.session_id,
                    )
                ),
            )
        return record

    async def begin_turn(
        self,
        identity: IdentityContext,
        session_id: str,
        *,
        client_turn_id: str,
        request_id: str,
        query: str,
    ) -> TurnClaim:
        input_hash = self._hash(query)
        async with self._lock_for(session_id), self.database.session() as session:
            stored = await session.scalar(
                select(SessionORM)
                .where(
                    SessionORM.session_id == session_id,
                    SessionORM.tenant_id == identity.tenant_id,
                )
                .with_for_update()
            )
            self._require_access(stored, identity)
            assert stored is not None
            if stored.status == "closed":
                raise ConflictError("会话已关闭")
            existing = await session.scalar(
                select(AgentRunORM).where(
                    AgentRunORM.tenant_id == identity.tenant_id,
                    AgentRunORM.session_id == session_id,
                    AgentRunORM.client_turn_id == client_turn_id,
                )
            )
            history = await self._history(session, identity.tenant_id, session_id)
            if existing is not None:
                if existing.input_hash != input_hash:
                    raise ConflictError("client_turn_id 已用于不同输入")
                user_sequence = self._user_sequence(history, client_turn_id)
                if existing.status == TurnStatus.COMPLETED.value:
                    result = (
                        ChatResult.model_validate(existing.result_payload)
                        if existing.result_payload is not None
                        else None
                    )
                    return self._claim(
                        existing,
                        tuple(item for item in history if item.client_turn_id != client_turn_id),
                        TurnDisposition.REPLAY,
                        user_sequence=user_sequence,
                        result=result,
                    )
                if existing.status == TurnStatus.RUNNING.value:
                    return self._claim(
                        existing,
                        tuple(item for item in history if item.client_turn_id != client_turn_id),
                        TurnDisposition.IN_PROGRESS,
                        user_sequence=user_sequence,
                    )
                if stored.active_run_id not in {None, existing.run_id}:
                    raise ConflictError("当前会话正在处理另一轮请求")
                stored.revision += 1
                stored.active_run_id = existing.run_id
                stored.updated_at = utc_now()
                existing.status = TurnStatus.RUNNING.value
                existing.error_code = None
                existing.finished_at = None
                existing.session_revision = stored.revision
                return self._claim(
                    existing,
                    tuple(item for item in history if item.client_turn_id != client_turn_id),
                    TurnDisposition.STARTED,
                    user_sequence=user_sequence,
                )
            if stored.active_run_id is not None:
                raise ConflictError("当前会话正在处理另一轮请求")

            now = utc_now()
            run_id = new_id()
            sequence = stored.last_message_sequence + 1
            stored.last_message_sequence = sequence
            stored.revision += 1
            stored.active_run_id = run_id
            stored.updated_at = now
            session.add(
                MessageORM(
                    message_id=new_id(),
                    session_id=session_id,
                    tenant_id=identity.tenant_id,
                    client_turn_id=client_turn_id,
                    run_id=run_id,
                    sequence=sequence,
                    role="user",
                    status="committed",
                    content=query,
                    content_hash=input_hash,
                    created_at=now,
                )
            )
            run = AgentRunORM(
                run_id=run_id,
                request_id=request_id,
                session_id=session_id,
                tenant_id=identity.tenant_id,
                client_turn_id=client_turn_id,
                input_hash=input_hash,
                result_hash=None,
                result_payload=None,
                error_code=None,
                status=TurnStatus.RUNNING.value,
                model_version=self.model_version,
                prompt_version=self.prompt_version,
                state_version=2,
                checkpoint_version="agent-state-v2",
                context_policy_version=stored.context_policy_version,
                recovery_source="fresh",
                session_revision=stored.revision,
                started_at=now,
                finished_at=None,
            )
            session.add(run)
            return self._claim(
                run,
                tuple(history),
                TurnDisposition.STARTED,
                user_sequence=sequence,
            )

    async def complete_turn(
        self,
        identity: IdentityContext,
        claim: TurnClaim,
        result: ChatResult,
        *,
        conversation_state: ConversationState | None = None,
        context_manifest: ContextManifest | None = None,
        generation_manifest: GenerationManifest | None = None,
    ) -> None:
        async with self._lock_for(claim.session_id), self.database.session() as session:
            stored = await session.scalar(
                select(SessionORM)
                .where(
                    SessionORM.session_id == claim.session_id,
                    SessionORM.tenant_id == identity.tenant_id,
                )
                .with_for_update()
            )
            self._require_access(stored, identity)
            assert stored is not None
            run = await session.get(AgentRunORM, claim.run_id)
            if (
                run is None
                or run.tenant_id != identity.tenant_id
                or run.session_id != claim.session_id
                or run.client_turn_id != claim.client_turn_id
            ):
                raise ConflictError("运行归属不匹配")
            payload = result.model_dump(mode="json")
            result_hash = self._json_hash(payload)
            if run.status == TurnStatus.COMPLETED.value:
                if run.result_hash != result_hash:
                    raise ConflictError("运行已由不同结果完成")
                return
            if (
                run.status != TurnStatus.RUNNING.value
                or stored.active_run_id != claim.run_id
                or stored.revision != claim.expected_revision
                or run.session_revision != claim.expected_revision
            ):
                raise ConflictError("会话 revision 已变化，拒绝覆盖")
            now = utc_now()
            sequence = stored.last_message_sequence + 1
            stored.last_message_sequence = sequence
            stored.revision += 1
            stored.active_run_id = None
            stored.updated_at = now
            if conversation_state is not None:
                stored.conversation_state = conversation_state.model_dump(mode="json")
                stored.summary = conversation_state.summary
                stored.summary_version = conversation_state.summary_version
                stored.summary_through_sequence = conversation_state.summary_through_sequence
            session.add(
                MessageORM(
                    message_id=new_id(),
                    session_id=claim.session_id,
                    tenant_id=identity.tenant_id,
                    client_turn_id=claim.client_turn_id,
                    run_id=claim.run_id,
                    sequence=sequence,
                    role="assistant",
                    status="committed",
                    content=result.answer,
                    content_hash=self._hash(result.answer),
                    created_at=now,
                )
            )
            run.status = TurnStatus.COMPLETED.value
            run.result_payload = payload
            run.result_hash = result_hash
            run.error_code = None
            run.finished_at = now
            if context_manifest is not None:
                existing_manifest = await session.scalar(
                    select(ContextManifestORM.manifest_id).where(
                        ContextManifestORM.tenant_id == identity.tenant_id,
                        ContextManifestORM.run_id == claim.run_id,
                    )
                )
                if existing_manifest is None:
                    values = context_manifest.model_dump(mode="python")
                    values["selected_message_ids"] = list(context_manifest.selected_message_ids)
                    values["selected_evidence_ids"] = list(context_manifest.selected_evidence_ids)
                    values["selected_tool_result_ids"] = list(
                        context_manifest.selected_tool_result_ids
                    )
                    values["dropped_items"] = [
                        item.model_dump(mode="json") for item in context_manifest.dropped_items
                    ]
                    session.add(ContextManifestORM(**values))
            if generation_manifest is not None:
                existing_generation = await session.scalar(
                    select(GenerationManifestORM.manifest_id).where(
                        GenerationManifestORM.tenant_id == identity.tenant_id,
                        GenerationManifestORM.run_id == claim.run_id,
                    )
                )
                if existing_generation is None:
                    session.add(
                        GenerationManifestORM(**generation_manifest.model_dump(mode="python"))
                    )

    async def abort_turn(
        self,
        identity: IdentityContext,
        claim: TurnClaim,
        *,
        error_code: str,
    ) -> None:
        async with self._lock_for(claim.session_id), self.database.session() as session:
            stored = await session.scalar(
                select(SessionORM)
                .where(
                    SessionORM.session_id == claim.session_id,
                    SessionORM.tenant_id == identity.tenant_id,
                )
                .with_for_update()
            )
            self._require_access(stored, identity)
            assert stored is not None
            run = await session.get(AgentRunORM, claim.run_id)
            if run is None or run.tenant_id != identity.tenant_id:
                return
            if run.status == TurnStatus.COMPLETED.value:
                return
            run.status = (
                TurnStatus.CANCELLED.value if error_code == "cancelled" else TurnStatus.FAILED.value
            )
            run.error_code = error_code
            run.finished_at = utc_now()
            if stored.active_run_id == claim.run_id:
                stored.active_run_id = None
                stored.revision += 1
                stored.updated_at = utc_now()

    async def append(
        self, identity: IdentityContext, session_id: str, query: str, result: ChatResult
    ) -> None:
        client_turn_id = result.client_turn_id or result.request_id
        claim = await self.begin_turn(
            identity,
            session_id,
            client_turn_id=client_turn_id,
            request_id=result.request_id,
            query=query,
        )
        if claim.disposition is TurnDisposition.STARTED:
            normalized = result.model_copy(
                update={"run_id": claim.run_id, "client_turn_id": client_turn_id}
            )
            await self.complete_turn(identity, claim, normalized)

    async def close(self, identity: IdentityContext, session_id: str) -> None:
        async with self._lock_for(session_id), self.database.session() as session:
            stored = await session.scalar(
                select(SessionORM)
                .where(
                    SessionORM.session_id == session_id,
                    SessionORM.tenant_id == identity.tenant_id,
                )
                .with_for_update()
            )
            self._require_access(stored, identity)
            assert stored is not None
            if stored.active_run_id is not None:
                raise ConflictError("会话存在运行中的请求，暂不能关闭")
            stored.status = "closed"
            stored.revision += 1
            stored.updated_at = utc_now()

    async def _history(
        self,
        session: Any,
        tenant_id: str,
        session_id: str,
    ) -> list[SessionMessage]:
        rows = (
            await session.scalars(
                select(MessageORM)
                .where(
                    MessageORM.session_id == session_id,
                    MessageORM.tenant_id == tenant_id,
                )
                .order_by(MessageORM.sequence)
            )
        ).all()
        return [self._message(row) for row in rows]

    @staticmethod
    def _message(row: MessageORM) -> SessionMessage:
        created_at = (
            row.created_at
            if row.created_at.tzinfo is not None
            else row.created_at.replace(tzinfo=UTC)
        )
        return SessionMessage(
            message_id=row.message_id,
            client_turn_id=row.client_turn_id,
            run_id=row.run_id,
            sequence=row.sequence,
            role=cast(Any, row.role),
            status="committed",
            content=row.content,
            content_hash=row.content_hash,
            created_at=created_at,
        )

    @staticmethod
    def _claim(
        run: AgentRunORM,
        history: tuple[SessionMessage, ...],
        disposition: TurnDisposition,
        *,
        user_sequence: int,
        result: ChatResult | None = None,
    ) -> TurnClaim:
        return TurnClaim(
            session_id=run.session_id,
            client_turn_id=run.client_turn_id,
            request_id=run.request_id,
            run_id=run.run_id,
            input_hash=run.input_hash,
            user_sequence=user_sequence,
            expected_revision=run.session_revision,
            history=history,
            disposition=disposition,
            result=result,
        )

    @staticmethod
    def _user_sequence(history: list[SessionMessage], client_turn_id: str) -> int:
        for item in history:
            if item.client_turn_id == client_turn_id and item.role == "user":
                return item.sequence
        raise ConflictError("运行缺少对应的用户消息")

    @staticmethod
    def _require_access(stored: SessionORM | None, identity: IdentityContext) -> None:
        if stored is None or (
            stored.user_id != identity.user_id and not identity.has_role("admin")
        ):
            raise NotFoundError("会话不存在")

    def _lock_for(self, session_id: str) -> asyncio.Lock:
        return self._session_locks.setdefault(session_id, asyncio.Lock())

    @staticmethod
    def _hash(value: str) -> str:
        return hashlib.sha256(value.encode()).hexdigest()

    @staticmethod
    def _json_hash(value: dict[str, Any]) -> str:
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode()).hexdigest()


class SQLSafeResumeStore:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def save(self, snapshot: SafeResumeSnapshot) -> SafeResumeSnapshot:
        async with self.database.session() as session:
            existing = await session.scalar(
                select(SafeResumeSnapshotORM).where(
                    SafeResumeSnapshotORM.tenant_id == snapshot.tenant_id,
                    SafeResumeSnapshotORM.draft_id == snapshot.draft_id,
                )
            )
            if existing is not None:
                if existing.run_id != snapshot.run_id:
                    raise ConflictError("审批草单已关联到不同运行")
                return self._snapshot(existing)
            values = snapshot.model_dump(mode="python")
            values["roles"] = sorted(snapshot.roles)
            values["completed_side_effects"] = list(snapshot.completed_side_effects)
            session.add(SafeResumeSnapshotORM(**values))
        return snapshot

    async def get_by_draft(self, tenant_id: str, draft_id: str) -> SafeResumeSnapshot | None:
        async with self.database.session() as session:
            stored = await session.scalar(
                select(SafeResumeSnapshotORM)
                .where(
                    SafeResumeSnapshotORM.tenant_id == tenant_id,
                    SafeResumeSnapshotORM.draft_id == draft_id,
                )
                .with_for_update()
            )
            if stored is None:
                return None
            if stored.status == "pending" and self._utc(stored.expires_at) <= utc_now():
                stored.status = "expired"
                stored.next_action = "none"
                stored.updated_at = utc_now()
            return self._snapshot(stored)

    async def mark_resumed(
        self,
        tenant_id: str,
        snapshot_id: str,
        *,
        decision: str,
        result_hash: str,
        final_answer: str,
        recovery_source: str,
    ) -> SafeResumeSnapshot:
        async with self.database.session() as session:
            stored = await session.scalar(
                select(SafeResumeSnapshotORM)
                .where(
                    SafeResumeSnapshotORM.snapshot_id == snapshot_id,
                    SafeResumeSnapshotORM.tenant_id == tenant_id,
                )
                .with_for_update()
            )
            if stored is None:
                raise NotFoundError("安全恢复快照不存在")
            if stored.status == "resumed":
                if stored.decision != decision:
                    raise ConflictError("审批草单已由不同决定恢复")
                return self._snapshot(stored)
            if stored.status != "pending" or self._utc(stored.expires_at) <= utc_now():
                raise ConflictError("安全恢复快照不可恢复")
            stored.status = "resumed"
            stored.decision = decision
            stored.next_action = "none"
            stored.resume_result_hash = result_hash
            stored.final_answer = final_answer
            stored.recovery_source = recovery_source
            stored.updated_at = utc_now()
            return self._snapshot(stored)

    @classmethod
    def _snapshot(cls, stored: SafeResumeSnapshotORM) -> SafeResumeSnapshot:
        return SafeResumeSnapshot(
            snapshot_id=stored.snapshot_id,
            tenant_id=stored.tenant_id,
            user_id=stored.user_id,
            roles=frozenset(stored.roles),
            session_id=stored.session_id,
            run_id=stored.run_id,
            request_id=stored.request_id,
            client_turn_id=stored.client_turn_id,
            draft_id=stored.draft_id,
            safe_node="approval_interrupt",
            status=cast(Any, stored.status),
            decision=cast(Any, stored.decision),
            next_action=cast(Any, stored.next_action),
            completed_side_effects=tuple(stored.completed_side_effects),
            state_version=stored.state_version,
            checkpoint_version=stored.checkpoint_version,
            recovery_source=cast(Any, stored.recovery_source),
            resume_result_hash=stored.resume_result_hash,
            final_answer=stored.final_answer,
            created_at=cls._utc(stored.created_at),
            updated_at=cls._utc(stored.updated_at),
            expires_at=cls._utc(stored.expires_at),
        )

    @staticmethod
    def _utc(value: datetime) -> datetime:
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


class SQLLongTermMemoryStore:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def settings(self, identity: IdentityContext) -> MemorySettings:
        async with self.database.session() as session:
            stored = await session.get(
                UserMemorySettingsORM, (identity.tenant_id, identity.user_id)
            )
            if stored is None:
                return MemorySettings(
                    tenant_id=identity.tenant_id,
                    user_id=identity.user_id,
                )
            return MemorySettings(
                tenant_id=stored.tenant_id,
                user_id=stored.user_id,
                enabled=stored.enabled,
                auto_write_enabled=False,
                updated_at=self._utc(stored.updated_at),
            )

    async def set_enabled(self, identity: IdentityContext, *, enabled: bool) -> MemorySettings:
        now = utc_now()
        async with self.database.session() as session:
            stored = await session.get(
                UserMemorySettingsORM, (identity.tenant_id, identity.user_id)
            )
            if stored is None:
                stored = UserMemorySettingsORM(
                    tenant_id=identity.tenant_id,
                    user_id=identity.user_id,
                    enabled=enabled,
                    auto_write_enabled=False,
                    updated_at=now,
                )
                session.add(stored)
            else:
                stored.enabled = enabled
                stored.auto_write_enabled = False
                stored.updated_at = now
        return MemorySettings(
            tenant_id=identity.tenant_id,
            user_id=identity.user_id,
            enabled=enabled,
            auto_write_enabled=False,
            updated_at=now,
        )

    async def create_confirmed(
        self,
        identity: IdentityContext,
        *,
        category: MemoryCategory,
        key: str,
        value: str,
        source_turn_id: str,
        expires_at: datetime | None,
    ) -> LongTermMemory:
        if expires_at is not None and expires_at <= utc_now():
            raise ConflictError("长期记忆过期时间必须在未来")
        async with self.database.session() as session:
            existing = await session.scalar(
                select(LongTermMemoryORM).where(
                    LongTermMemoryORM.tenant_id == identity.tenant_id,
                    LongTermMemoryORM.user_id == identity.user_id,
                    LongTermMemoryORM.key == key,
                    LongTermMemoryORM.status == MemoryStatus.ACTIVE.value,
                )
            )
            if existing is not None:
                raise ConflictError("同名长期记忆已存在，请使用纠正接口")
            memory = LongTermMemory(
                tenant_id=identity.tenant_id,
                user_id=identity.user_id,
                category=category,
                key=key,
                value=value,
                source_turn_id=source_turn_id,
                confirmation_method="explicit_user",
                confirmed_by=identity.user_id,
                expires_at=expires_at,
            )
            session.add(LongTermMemoryORM(**memory.model_dump(mode="python")))
            return memory

    async def list_active(self, identity: IdentityContext) -> list[LongTermMemory]:
        if not (await self.settings(identity)).enabled:
            return []
        now = utc_now()
        async with self.database.session() as session:
            rows = (
                await session.scalars(
                    select(LongTermMemoryORM)
                    .where(
                        LongTermMemoryORM.tenant_id == identity.tenant_id,
                        LongTermMemoryORM.user_id == identity.user_id,
                        LongTermMemoryORM.status == MemoryStatus.ACTIVE.value,
                        or_(
                            LongTermMemoryORM.expires_at.is_(None),
                            LongTermMemoryORM.expires_at > now,
                        ),
                    )
                    .order_by(LongTermMemoryORM.key, LongTermMemoryORM.version.desc())
                )
            ).all()
            return [self._memory(row) for row in rows]

    async def correct(
        self,
        identity: IdentityContext,
        memory_id: str,
        *,
        value: str,
        source_turn_id: str,
        expires_at: datetime | None,
    ) -> LongTermMemory:
        if expires_at is not None and expires_at <= utc_now():
            raise ConflictError("长期记忆过期时间必须在未来")
        async with self.database.session() as session:
            stored = await session.scalar(
                select(LongTermMemoryORM)
                .where(
                    LongTermMemoryORM.memory_id == memory_id,
                    LongTermMemoryORM.tenant_id == identity.tenant_id,
                    LongTermMemoryORM.user_id == identity.user_id,
                )
                .with_for_update()
            )
            if stored is None:
                raise NotFoundError("长期记忆不存在")
            if stored.status != MemoryStatus.ACTIVE.value:
                raise ConflictError("只有有效长期记忆可以纠正")
            current = self._memory(stored)
            now = utc_now()
            stored.status = MemoryStatus.CORRECTED.value
            stored.updated_at = now
            corrected = current.model_copy(
                update={
                    "memory_id": new_id(),
                    "value": value,
                    "source_turn_id": source_turn_id,
                    "confirmed_by": identity.user_id,
                    "version": current.version + 1,
                    "status": MemoryStatus.ACTIVE,
                    "supersedes_memory_id": current.memory_id,
                    "expires_at": expires_at,
                    "created_at": now,
                    "updated_at": now,
                    "deleted_at": None,
                }
            )
            session.add(LongTermMemoryORM(**corrected.model_dump(mode="python")))
            return corrected

    async def delete(self, identity: IdentityContext, memory_id: str) -> None:
        async with self.database.session() as session:
            stored = await session.scalar(
                select(LongTermMemoryORM)
                .where(
                    LongTermMemoryORM.memory_id == memory_id,
                    LongTermMemoryORM.tenant_id == identity.tenant_id,
                    LongTermMemoryORM.user_id == identity.user_id,
                )
                .with_for_update()
            )
            if stored is None:
                raise NotFoundError("长期记忆不存在")
            now = utc_now()
            lineage = (
                await session.scalars(
                    select(LongTermMemoryORM)
                    .where(
                        LongTermMemoryORM.tenant_id == identity.tenant_id,
                        LongTermMemoryORM.user_id == identity.user_id,
                        LongTermMemoryORM.key == stored.key,
                    )
                    .with_for_update()
                )
            ).all()
            for item in lineage:
                item.value = "[deleted]"
                item.status = MemoryStatus.DELETED.value
                item.updated_at = now
                item.deleted_at = now

    @classmethod
    def _memory(cls, stored: LongTermMemoryORM) -> LongTermMemory:
        return LongTermMemory(
            memory_id=stored.memory_id,
            tenant_id=stored.tenant_id,
            user_id=stored.user_id,
            category=MemoryCategory(stored.category),
            key=stored.key,
            value=stored.value,
            source_turn_id=stored.source_turn_id,
            confirmation_method=cast(Any, stored.confirmation_method),
            confirmed_by=stored.confirmed_by,
            confidence=stored.confidence,
            sensitivity=cast(Any, stored.sensitivity),
            version=stored.version,
            status=MemoryStatus(stored.status),
            supersedes_memory_id=stored.supersedes_memory_id,
            valid_from=cls._utc(stored.valid_from),
            expires_at=cls._utc(stored.expires_at) if stored.expires_at is not None else None,
            created_at=cls._utc(stored.created_at),
            updated_at=cls._utc(stored.updated_at),
            deleted_at=cls._utc(stored.deleted_at) if stored.deleted_at is not None else None,
        )

    @staticmethod
    def _utc(value: datetime) -> datetime:
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


class SQLAudit:
    def __init__(self, database: Database, *, default_tenant_id: str) -> None:
        self.database = database
        self.default_tenant_id = default_tenant_id

    async def record(self, event_type: str, fields: dict[str, Any]) -> None:
        safe = redact(fields)
        payload = safe if isinstance(safe, dict) else {"summary": str(safe)}
        tenant_id = str(payload.get("tenant_id", self.default_tenant_id))
        request_id = payload.get("request_id")
        async with self.database.session() as session:
            if event_type == "agent.step":
                session.add(
                    AgentStepORM(
                        step_id=str(payload["step_id"]),
                        run_id=str(payload["run_id"]),
                        tenant_id=tenant_id,
                        node_name=str(payload["node_name"]),
                        status=str(payload["status"]),
                        input_summary=cast(dict[str, Any], payload.get("input_summary", {})),
                        output_summary=cast(dict[str, Any], payload.get("output_summary", {})),
                        started_at=datetime.fromisoformat(str(payload["started_at"])),
                        finished_at=datetime.fromisoformat(str(payload["finished_at"])),
                    )
                )
            elif event_type.startswith("tool."):
                tool_call_id = str(payload["tool_call_id"])
                tool_log = await session.get(ToolCallLogORM, tool_call_id)
                output_summary = {
                    "error_type": payload.get("error_type"),
                    "attempts": payload.get("attempts"),
                    "retries": payload.get("retries"),
                }
                output_summary = {
                    key: value for key, value in output_summary.items() if value is not None
                }
                if tool_log is None:
                    session.add(
                        ToolCallLogORM(
                            tool_call_id=tool_call_id,
                            run_id=str(payload["run_id"]),
                            tenant_id=tenant_id,
                            tool_name=str(payload["tool_name"]),
                            schema_version=int(payload["schema_version"]),
                            risk_level=str(payload["risk_level"]),
                            status=str(payload["status"]),
                            input_summary={"input_keys": payload.get("input_keys", [])},
                            output_summary=output_summary,
                            created_at=utc_now(),
                        )
                    )
                else:
                    tool_log.status = str(payload["status"])
                    tool_log.output_summary = output_summary
            session.add(
                AuditEventORM(
                    audit_id=new_id(),
                    tenant_id=tenant_id,
                    event_type=event_type,
                    request_id=str(request_id) if request_id is not None else None,
                    fields=payload,
                    created_at=utc_now(),
                )
            )
