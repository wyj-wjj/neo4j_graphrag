"""Tenant-isolated, idempotent conversation turn coordination."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol

from graphrag.domain.errors import ConflictError, NotFoundError
from graphrag.domain.ids import new_id
from graphrag.domain.models import (
    ChatResult,
    ContextManifest,
    ConversationState,
    GenerationManifest,
    IdentityContext,
    SessionMessage,
)


class TurnDisposition(StrEnum):
    STARTED = "started"
    IN_PROGRESS = "in_progress"
    REPLAY = "replay"


class TurnStatus(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class TurnClaim:
    session_id: str
    client_turn_id: str
    request_id: str
    run_id: str
    input_hash: str
    user_sequence: int
    expected_revision: int
    history: tuple[SessionMessage, ...]
    disposition: TurnDisposition
    result: ChatResult | None = None


@dataclass(slots=True)
class TurnRecord:
    client_turn_id: str
    request_id: str
    run_id: str
    input_hash: str
    status: TurnStatus
    user_sequence: int
    expected_revision: int
    result: ChatResult | None = None
    error_code: str | None = None


@dataclass(slots=True)
class SessionRecord:
    session_id: str
    tenant_id: str
    user_id: str
    messages: list[SessionMessage] = field(default_factory=list)
    results: list[ChatResult] = field(default_factory=list)
    turns: dict[str, TurnRecord] = field(default_factory=dict)
    closed: bool = False
    revision: int = 0
    last_message_sequence: int = 0
    active_run_id: str | None = None
    context_policy_version: str = "context-policy-v1"
    conversation_state: ConversationState | None = None
    context_manifests: list[ContextManifest] = field(default_factory=list)
    generation_manifests: list[GenerationManifest] = field(default_factory=list)


class SessionService(Protocol):
    async def create(self, identity: IdentityContext) -> SessionRecord: ...

    async def get(self, identity: IdentityContext, session_id: str) -> SessionRecord: ...

    async def begin_turn(
        self,
        identity: IdentityContext,
        session_id: str,
        *,
        client_turn_id: str,
        request_id: str,
        query: str,
    ) -> TurnClaim: ...

    async def complete_turn(
        self,
        identity: IdentityContext,
        claim: TurnClaim,
        result: ChatResult,
        *,
        conversation_state: ConversationState | None = None,
        context_manifest: ContextManifest | None = None,
        generation_manifest: GenerationManifest | None = None,
    ) -> None: ...

    async def abort_turn(
        self,
        identity: IdentityContext,
        claim: TurnClaim,
        *,
        error_code: str,
    ) -> None: ...

    async def close(self, identity: IdentityContext, session_id: str) -> None: ...


class InMemorySessionService:
    def __init__(self) -> None:
        self.sessions: dict[str, SessionRecord] = {}
        self._index_lock = asyncio.Lock()
        self._session_locks: dict[str, asyncio.Lock] = {}

    async def create(self, identity: IdentityContext) -> SessionRecord:
        record = SessionRecord(new_id(), identity.tenant_id, identity.user_id)
        record.conversation_state = ConversationState(
            tenant_id=identity.tenant_id,
            session_id=record.session_id,
        )
        async with self._index_lock:
            self.sessions[record.session_id] = record
            self._session_locks[record.session_id] = asyncio.Lock()
        return record

    async def get(self, identity: IdentityContext, session_id: str) -> SessionRecord:
        record = self.sessions.get(session_id)
        if (
            record is None
            or record.tenant_id != identity.tenant_id
            or (record.user_id != identity.user_id and not identity.has_role("admin"))
        ):
            raise NotFoundError("会话不存在")
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
        record = await self.get(identity, session_id)
        input_hash = self._hash(query)
        async with self._lock_for(session_id):
            if record.closed:
                raise ConflictError("会话已关闭")
            existing = record.turns.get(client_turn_id)
            if existing is not None:
                if existing.input_hash != input_hash:
                    raise ConflictError("client_turn_id 已用于不同输入")
                if existing.status is TurnStatus.COMPLETED:
                    return self._claim(record, existing, TurnDisposition.REPLAY)
                if existing.status is TurnStatus.RUNNING:
                    return self._claim(record, existing, TurnDisposition.IN_PROGRESS)
                if record.active_run_id not in {None, existing.run_id}:
                    raise ConflictError("当前会话正在处理另一轮请求")
                existing.status = TurnStatus.RUNNING
                existing.error_code = None
                existing.expected_revision = record.revision + 1
                record.active_run_id = existing.run_id
                record.revision += 1
                return self._claim(record, existing, TurnDisposition.STARTED)
            if record.active_run_id is not None:
                raise ConflictError("当前会话正在处理另一轮请求")

            run_id = new_id()
            history = tuple(record.messages)
            record.last_message_sequence += 1
            record.revision += 1
            turn = TurnRecord(
                client_turn_id=client_turn_id,
                request_id=request_id,
                run_id=run_id,
                input_hash=input_hash,
                status=TurnStatus.RUNNING,
                user_sequence=record.last_message_sequence,
                expected_revision=record.revision,
            )
            record.turns[client_turn_id] = turn
            record.active_run_id = run_id
            record.messages.append(
                SessionMessage(
                    client_turn_id=client_turn_id,
                    run_id=run_id,
                    sequence=record.last_message_sequence,
                    role="user",
                    content=query,
                    content_hash=input_hash,
                )
            )
            return TurnClaim(
                session_id=session_id,
                client_turn_id=client_turn_id,
                request_id=request_id,
                run_id=run_id,
                input_hash=input_hash,
                user_sequence=turn.user_sequence,
                expected_revision=record.revision,
                history=history,
                disposition=TurnDisposition.STARTED,
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
        record = await self.get(identity, claim.session_id)
        async with self._lock_for(claim.session_id):
            turn = record.turns.get(claim.client_turn_id)
            if turn is None or turn.run_id != claim.run_id:
                raise ConflictError("运行归属不匹配")
            if turn.status is TurnStatus.COMPLETED:
                if turn.result != result:
                    raise ConflictError("运行已由不同结果完成")
                return
            if (
                turn.status is not TurnStatus.RUNNING
                or record.active_run_id != claim.run_id
                or record.revision != claim.expected_revision
            ):
                raise ConflictError("会话 revision 已变化，拒绝覆盖")
            record.last_message_sequence += 1
            record.messages.append(
                SessionMessage(
                    client_turn_id=claim.client_turn_id,
                    run_id=claim.run_id,
                    sequence=record.last_message_sequence,
                    role="assistant",
                    content=result.answer,
                    content_hash=self._hash(result.answer),
                )
            )
            record.results.append(result)
            if conversation_state is not None:
                record.conversation_state = conversation_state
            if context_manifest is not None and all(
                item.manifest_id != context_manifest.manifest_id
                for item in record.context_manifests
            ):
                record.context_manifests.append(context_manifest)
            if generation_manifest is not None and all(
                item.manifest_id != generation_manifest.manifest_id
                for item in record.generation_manifests
            ):
                record.generation_manifests.append(generation_manifest)
            turn.status = TurnStatus.COMPLETED
            turn.result = result
            record.active_run_id = None
            record.revision += 1

    async def abort_turn(
        self,
        identity: IdentityContext,
        claim: TurnClaim,
        *,
        error_code: str,
    ) -> None:
        record = await self.get(identity, claim.session_id)
        async with self._lock_for(claim.session_id):
            turn = record.turns.get(claim.client_turn_id)
            if turn is None or turn.run_id != claim.run_id:
                return
            if turn.status is TurnStatus.COMPLETED:
                return
            turn.status = TurnStatus.CANCELLED if error_code == "cancelled" else TurnStatus.FAILED
            turn.error_code = error_code
            if record.active_run_id == claim.run_id:
                record.active_run_id = None
                record.revision += 1

    async def append(
        self, identity: IdentityContext, session_id: str, query: str, result: ChatResult
    ) -> None:
        """Compatibility path for callers migrating to the turn lifecycle."""

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
        record = await self.get(identity, session_id)
        async with self._lock_for(session_id):
            if record.active_run_id is not None:
                raise ConflictError("会话存在运行中的请求，暂不能关闭")
            record.closed = True
            record.revision += 1

    def _lock_for(self, session_id: str) -> asyncio.Lock:
        return self._session_locks.setdefault(session_id, asyncio.Lock())

    @staticmethod
    def _hash(value: str) -> str:
        return hashlib.sha256(value.encode()).hexdigest()

    @staticmethod
    def _claim(
        record: SessionRecord,
        turn: TurnRecord,
        disposition: TurnDisposition,
    ) -> TurnClaim:
        history = tuple(
            message for message in record.messages if message.client_turn_id != turn.client_turn_id
        )
        return TurnClaim(
            session_id=record.session_id,
            client_turn_id=turn.client_turn_id,
            request_id=turn.request_id,
            run_id=turn.run_id,
            input_hash=turn.input_hash,
            user_sequence=turn.user_sequence,
            expected_revision=turn.expected_revision,
            history=history,
            disposition=disposition,
            result=turn.result,
        )
