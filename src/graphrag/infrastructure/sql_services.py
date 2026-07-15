"""MySQL-backed session history and durable audit adapters."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy import select

from graphrag.application.sessions import SessionRecord
from graphrag.domain.errors import ConflictError, NotFoundError
from graphrag.domain.ids import new_id
from graphrag.domain.models import ChatMessage, ChatResult, IdentityContext, utc_now
from graphrag.infrastructure.database import (
    AgentRunORM,
    AgentStepORM,
    AuditEventORM,
    Database,
    MessageORM,
    SessionORM,
    ToolCallLogORM,
)
from graphrag.observability.redaction import redact


class SQLSessionService:
    def __init__(self, database: Database, *, model_version: str) -> None:
        self.database = database
        self.model_version = model_version

    async def create(self, identity: IdentityContext) -> SessionRecord:
        now = utc_now()
        record = SessionRecord(new_id(), identity.tenant_id, identity.user_id)
        async with self.database.session() as session:
            session.add(
                SessionORM(
                    session_id=record.session_id,
                    tenant_id=record.tenant_id,
                    user_id=record.user_id,
                    status="active",
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
                    .order_by(MessageORM.created_at, MessageORM.message_id)
                )
            ).all()
            record = SessionRecord(
                session_id=stored.session_id,
                tenant_id=stored.tenant_id,
                user_id=stored.user_id,
                messages=[
                    ChatMessage(
                        role=cast(Any, row.role),
                        content=row.content,
                        created_at=(
                            row.created_at
                            if row.created_at.tzinfo is not None
                            else row.created_at.replace(tzinfo=UTC)
                        ),
                    )
                    for row in rows
                ],
                closed=stored.status == "closed",
            )
        return record

    async def append(
        self, identity: IdentityContext, session_id: str, query: str, result: ChatResult
    ) -> None:
        record = await self.get(identity, session_id)
        if record.closed:
            raise ConflictError("会话已关闭")
        now = utc_now()
        messages = (("user", query), ("assistant", result.answer))
        async with self.database.session() as session:
            for role, content in messages:
                session.add(
                    MessageORM(
                        message_id=new_id(),
                        session_id=session_id,
                        tenant_id=identity.tenant_id,
                        role=role,
                        content=content,
                        content_hash=hashlib.sha256(content.encode()).hexdigest(),
                        created_at=now,
                    )
                )
            session.add(
                AgentRunORM(
                    run_id=result.run_id,
                    request_id=result.request_id,
                    session_id=session_id,
                    tenant_id=identity.tenant_id,
                    status=result.status.value,
                    model_version=self.model_version,
                    prompt_version="agent-v1",
                    started_at=now,
                    finished_at=now,
                )
            )
            stored = await session.get(SessionORM, session_id)
            if stored is not None:
                stored.updated_at = now

    async def close(self, identity: IdentityContext, session_id: str) -> None:
        await self.get(identity, session_id)
        async with self.database.session() as session:
            stored = await session.get(SessionORM, session_id)
            if stored is not None and stored.tenant_id == identity.tenant_id:
                stored.status = "closed"
                stored.updated_at = utc_now()


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
                session.add(
                    ToolCallLogORM(
                        tool_call_id=str(payload["tool_call_id"]),
                        run_id=str(payload["run_id"]),
                        tenant_id=tenant_id,
                        tool_name=str(payload["tool_name"]),
                        schema_version=int(payload["schema_version"]),
                        risk_level=str(payload["risk_level"]),
                        status=str(payload["status"]),
                        input_summary={"input_keys": payload.get("input_keys", [])},
                        output_summary={"error_type": payload.get("error_type")}
                        if payload.get("error_type")
                        else {},
                        created_at=utc_now(),
                    )
                )
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
