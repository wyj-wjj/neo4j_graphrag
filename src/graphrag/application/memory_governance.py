"""Explicit, user-governed long-term memory use cases."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from graphrag.domain.errors import UnsafeOperationError
from graphrag.domain.models import (
    IdentityContext,
    LongTermMemory,
    MemoryCategory,
    MemorySettings,
)
from graphrag.domain.ports import AuditPort, LongTermMemoryPort

_FORBIDDEN_MEMORY = re.compile(
    r"(?:password|passwd|密码|token|api[_-]?key|secret|Bearer\s+|"
    r"\b1[3-9]\d{9}\b|\b\d{17}[0-9Xx]\b|\bDEMO-\d{4,}\b)",
    re.IGNORECASE,
)


@dataclass(slots=True)
class LongTermMemoryService:
    store: LongTermMemoryPort
    audit: AuditPort
    auto_write_enabled: bool = False

    async def settings(self, identity: IdentityContext) -> MemorySettings:
        result = await self.store.settings(identity)
        await self._audit(identity, "memory.settings.view", {})
        return result

    async def set_enabled(self, identity: IdentityContext, *, enabled: bool) -> MemorySettings:
        result = await self.store.set_enabled(identity, enabled=enabled)
        await self._audit(identity, "memory.settings.update", {"enabled": enabled})
        return result

    async def create_confirmed(
        self,
        identity: IdentityContext,
        *,
        category: MemoryCategory,
        key: str,
        value: str,
        source_turn_id: str,
        expires_at: datetime | None,
        explicitly_confirmed: bool,
    ) -> LongTermMemory:
        if not explicitly_confirmed:
            raise UnsafeOperationError("长期记忆必须由用户明确确认")
        self._validate_value(value)
        settings = await self.store.settings(identity)
        if not settings.enabled:
            raise UnsafeOperationError("用户已禁用长期记忆")
        result = await self.store.create_confirmed(
            identity,
            category=category,
            key=key,
            value=value,
            source_turn_id=source_turn_id,
            expires_at=expires_at,
        )
        await self._audit(
            identity,
            "memory.created",
            {"memory_id": result.memory_id, "category": result.category.value},
        )
        return result

    async def list_active(self, identity: IdentityContext) -> list[LongTermMemory]:
        result = await self.store.list_active(identity)
        await self._audit(identity, "memory.listed", {"count": len(result)})
        return result

    async def correct(
        self,
        identity: IdentityContext,
        memory_id: str,
        *,
        value: str,
        source_turn_id: str,
        expires_at: datetime | None,
        explicitly_confirmed: bool,
    ) -> LongTermMemory:
        if not explicitly_confirmed:
            raise UnsafeOperationError("纠正长期记忆必须由用户明确确认")
        self._validate_value(value)
        result = await self.store.correct(
            identity,
            memory_id,
            value=value,
            source_turn_id=source_turn_id,
            expires_at=expires_at,
        )
        await self._audit(
            identity,
            "memory.corrected",
            {"memory_id": memory_id, "new_memory_id": result.memory_id},
        )
        return result

    async def delete(self, identity: IdentityContext, memory_id: str) -> None:
        await self.store.delete(identity, memory_id)
        await self._audit(identity, "memory.deleted", {"memory_id": memory_id})

    @staticmethod
    def _validate_value(value: str) -> None:
        if _FORBIDDEN_MEMORY.search(value):
            raise UnsafeOperationError("该内容包含禁止保存到长期记忆的敏感或业务事实")

    async def _audit(
        self, identity: IdentityContext, event_type: str, fields: dict[str, object]
    ) -> None:
        await self.audit.record(
            event_type,
            {
                "tenant_id": identity.tenant_id,
                "user_id": identity.user_id,
                "auto_write_enabled": self.auto_write_enabled,
                **fields,
            },
        )
