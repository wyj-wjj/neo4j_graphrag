"""Fail-closed migration registry for durable Agent State payloads."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any

from pydantic import ValidationError as PydanticValidationError

from graphrag.domain.errors import ValidationError
from graphrag.domain.models import ConversationState
from graphrag.domain.state import AgentState

Migration = Callable[[dict[str, Any]], dict[str, Any]]


class StateMigrationRegistry:
    CURRENT_VERSION = 2

    def __init__(self) -> None:
        self._migrations: dict[int, Migration] = {1: self._v1_to_v2}

    def load(self, payload: Mapping[str, Any]) -> AgentState:
        current = dict(payload)
        raw_version = current.get("state_version", 1)
        if not isinstance(raw_version, int) or raw_version < 1:
            raise ValidationError("Agent State 版本无效")
        if raw_version > self.CURRENT_VERSION:
            raise ValidationError(f"不支持的 Agent State 版本：{raw_version}")
        version = raw_version
        try:
            while version < self.CURRENT_VERSION:
                migration = self._migrations.get(version)
                if migration is None:
                    raise ValidationError(f"缺少 Agent State v{version} 迁移")
                current = migration(current)
                next_version = current.get("state_version")
                if not isinstance(next_version, int) or next_version <= version:
                    raise ValidationError("Agent State 迁移没有推进版本")
                version = next_version
            return AgentState.model_validate(current)
        except ValidationError:
            raise
        except (KeyError, TypeError, ValueError, PydanticValidationError) as exc:
            raise ValidationError("Agent State 迁移失败，已拒绝恢复") from exc

    def load_json(self, raw: str | bytes | bytearray) -> AgentState:
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as exc:
            raise ValidationError("Agent State 序列化内容损坏，已拒绝恢复") from exc
        if not isinstance(payload, dict):
            raise ValidationError("Agent State 必须是对象")
        return self.load(payload)

    @staticmethod
    def _v1_to_v2(payload: dict[str, Any]) -> dict[str, Any]:
        tenant_id = str(payload["tenant_id"])
        session_id = str(payload["session_id"])
        migrated = dict(payload)
        migrated.update(
            {
                "state_version": 2,
                "conversation_state": ConversationState(
                    tenant_id=tenant_id,
                    session_id=session_id,
                ).model_dump(mode="json"),
                "context_manifest": None,
                "approval_status": None,
                "client_turn_id": payload.get("request_id"),
            }
        )
        return migrated
