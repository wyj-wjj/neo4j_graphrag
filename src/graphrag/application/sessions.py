"""Tenant/user-isolated session history for the offline stage-one runtime."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from graphrag.domain.errors import ConflictError, NotFoundError
from graphrag.domain.ids import new_id
from graphrag.domain.models import ChatMessage, ChatResult, IdentityContext


@dataclass(slots=True)
class SessionRecord:
    session_id: str
    tenant_id: str
    user_id: str
    messages: list[ChatMessage] = field(default_factory=list)
    results: list[ChatResult] = field(default_factory=list)
    closed: bool = False


class InMemorySessionService:
    def __init__(self) -> None:
        self.sessions: dict[str, SessionRecord] = {}
        self._lock = asyncio.Lock()

    async def create(self, identity: IdentityContext) -> SessionRecord:
        record = SessionRecord(new_id(), identity.tenant_id, identity.user_id)
        async with self._lock:
            self.sessions[record.session_id] = record
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

    async def append(
        self, identity: IdentityContext, session_id: str, query: str, result: ChatResult
    ) -> None:
        record = await self.get(identity, session_id)
        if record.closed:
            raise ConflictError("会话已关闭")
        async with self._lock:
            record.messages.extend(
                [
                    ChatMessage(role="user", content=query),
                    ChatMessage(role="assistant", content=result.answer),
                ]
            )
            record.results.append(result)

    async def close(self, identity: IdentityContext, session_id: str) -> None:
        record = await self.get(identity, session_id)
        record.closed = True
