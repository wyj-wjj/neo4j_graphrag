"""MySQL Inbox and DLQ state for at-least-once event consumers."""

from __future__ import annotations

import base64
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from sqlalchemy import func, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from graphrag.domain.errors import ConflictError
from graphrag.domain.events import EventEnvelope, InboxClaimResult
from graphrag.domain.ids import new_id
from graphrag.domain.models import utc_now
from graphrag.infrastructure.database import (
    Database,
    DeadLetterEventORM,
    InboxEventORM,
)


class SQLInboxStore:
    """Persist consumer ownership, attempts, terminal state, and poison payloads."""

    def __init__(self, database: Database) -> None:
        self.database = database

    async def claim(
        self,
        *,
        consumer_name: str,
        event: EventEnvelope,
        payload_hash: str,
        worker_id: str,
        lease_seconds: int,
    ) -> InboxClaimResult:
        consumer = self._name(consumer_name, "consumer_name")
        worker = self._name(worker_id, "worker_id")
        self._hash(payload_hash)
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        now = utc_now()
        lease_expires_at = now + timedelta(seconds=lease_seconds)
        try:
            async with self.database.session() as session:
                row = await session.scalar(
                    select(InboxEventORM)
                    .where(
                        InboxEventORM.consumer_name == consumer,
                        InboxEventORM.event_id == event.event_id,
                    )
                    .with_for_update()
                )
                if row is None:
                    latest_version = await session.scalar(
                        select(func.max(InboxEventORM.aggregate_version)).where(
                            InboxEventORM.consumer_name == consumer,
                            InboxEventORM.tenant_id == event.tenant_id,
                            InboxEventORM.aggregate_id == event.aggregate_id,
                            InboxEventORM.status.in_({"processed", "stale"}),
                        )
                    )
                    if latest_version is not None and latest_version > event.aggregate_version:
                        session.add(
                            InboxEventORM(
                                inbox_id=new_id(),
                                consumer_name=consumer,
                                event_id=event.event_id,
                                event_type=event.event_type,
                                event_version=event.event_version,
                                aggregate_version=event.aggregate_version,
                                tenant_id=event.tenant_id,
                                aggregate_id=event.aggregate_id,
                                payload_hash=payload_hash,
                                status="stale",
                                attempt_count=0,
                                available_at=now,
                                lease_owner=None,
                                lease_expires_at=None,
                                last_error_code="stale_aggregate_version",
                                received_at=now,
                                processed_at=now,
                            )
                        )
                        await session.flush()
                        return InboxClaimResult(disposition="stale", attempt_count=0)
                    session.add(
                        InboxEventORM(
                            inbox_id=new_id(),
                            consumer_name=consumer,
                            event_id=event.event_id,
                            event_type=event.event_type,
                            event_version=event.event_version,
                            aggregate_version=event.aggregate_version,
                            tenant_id=event.tenant_id,
                            aggregate_id=event.aggregate_id,
                            payload_hash=payload_hash,
                            status="processing",
                            attempt_count=1,
                            available_at=now,
                            lease_owner=worker,
                            lease_expires_at=lease_expires_at,
                            last_error_code=None,
                            received_at=now,
                            processed_at=None,
                        )
                    )
                    await session.flush()
                    return InboxClaimResult(disposition="claimed", attempt_count=1)
                if row.payload_hash != payload_hash:
                    raise ConflictError("相同 event_id 对应不同载荷")
                if row.status in {"processed", "dlq"}:
                    return InboxClaimResult(
                        disposition="duplicate", attempt_count=row.attempt_count
                    )
                if self._future(row.available_at, now) or (
                    row.lease_owner not in {None, worker}
                    and row.lease_expires_at is not None
                    and self._future(row.lease_expires_at, now)
                ):
                    return InboxClaimResult(disposition="busy", attempt_count=row.attempt_count)
                row.status = "processing"
                row.attempt_count += 1
                row.lease_owner = worker
                row.lease_expires_at = lease_expires_at
                row.last_error_code = None
                await session.flush()
                return InboxClaimResult(disposition="claimed", attempt_count=row.attempt_count)
        except IntegrityError:
            return await self._claim_after_insert_race(
                consumer_name=consumer,
                event=event,
                payload_hash=payload_hash,
                worker_id=worker,
                lease_seconds=lease_seconds,
            )

    async def _claim_after_insert_race(
        self,
        *,
        consumer_name: str,
        event: EventEnvelope,
        payload_hash: str,
        worker_id: str,
        lease_seconds: int,
    ) -> InboxClaimResult:
        async with self.database.session() as session:
            row = await session.scalar(
                select(InboxEventORM).where(
                    InboxEventORM.consumer_name == consumer_name,
                    InboxEventORM.event_id == event.event_id,
                )
            )
            if row is None:
                raise ConflictError("Inbox 并发领取失败，请重试")
        return await self.claim(
            consumer_name=consumer_name,
            event=event,
            payload_hash=payload_hash,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
        )

    async def mark_processed(
        self,
        *,
        consumer_name: str,
        event_id: str,
        worker_id: str,
        processed_at: datetime,
    ) -> None:
        await self._owned_update(
            consumer_name=consumer_name,
            event_id=event_id,
            worker_id=worker_id,
            values={
                "status": "processed",
                "processed_at": processed_at,
                "lease_owner": None,
                "lease_expires_at": None,
                "last_error_code": None,
            },
            error="Inbox 处理完成时租约已丢失",
        )

    async def release_for_retry(
        self,
        *,
        consumer_name: str,
        event_id: str,
        worker_id: str,
        available_at: datetime,
        error_code: str,
    ) -> None:
        await self._owned_update(
            consumer_name=consumer_name,
            event_id=event_id,
            worker_id=worker_id,
            values={
                "status": "retry",
                "available_at": available_at,
                "lease_owner": None,
                "lease_expires_at": None,
                "last_error_code": self._error(error_code),
            },
            error="Inbox 重试时租约已丢失",
        )

    async def move_to_dlq(
        self,
        *,
        consumer_name: str,
        event: EventEnvelope,
        payload_hash: str,
        raw_payload: bytes,
        worker_id: str,
        failure_kind: str,
        error_code: str,
    ) -> None:
        async with self.database.session() as session:
            result = cast(
                CursorResult[Any],
                await session.execute(
                    update(InboxEventORM)
                    .where(
                        InboxEventORM.consumer_name == consumer_name,
                        InboxEventORM.event_id == event.event_id,
                        InboxEventORM.status == "processing",
                        InboxEventORM.lease_owner == worker_id,
                    )
                    .values(
                        status="dlq",
                        lease_owner=None,
                        lease_expires_at=None,
                        last_error_code=self._error(error_code),
                    )
                ),
            )
            if result.rowcount != 1:
                raise ConflictError("Inbox 转入 DLQ 时租约已丢失")
            await self._add_dlq(
                session,
                consumer_name=consumer_name,
                event=event,
                payload_hash=payload_hash,
                raw_payload=raw_payload,
                failure_kind=failure_kind,
                error_code=error_code,
            )

    async def record_invalid(
        self,
        *,
        consumer_name: str,
        payload_hash: str,
        raw_payload: bytes,
        failure_kind: str,
        error_code: str,
    ) -> None:
        async with self.database.session() as session:
            await self._add_dlq(
                session,
                consumer_name=consumer_name,
                event=None,
                payload_hash=payload_hash,
                raw_payload=raw_payload,
                failure_kind=failure_kind,
                error_code=error_code,
            )

    async def _owned_update(
        self,
        *,
        consumer_name: str,
        event_id: str,
        worker_id: str,
        values: dict[str, Any],
        error: str,
    ) -> None:
        async with self.database.session() as session:
            result = cast(
                CursorResult[Any],
                await session.execute(
                    update(InboxEventORM)
                    .where(
                        InboxEventORM.consumer_name == consumer_name,
                        InboxEventORM.event_id == event_id,
                        InboxEventORM.status == "processing",
                        InboxEventORM.lease_owner == worker_id,
                    )
                    .values(**values)
                ),
            )
            if result.rowcount != 1:
                raise ConflictError(error)

    async def _add_dlq(
        self,
        session: AsyncSession,
        *,
        consumer_name: str,
        event: EventEnvelope | None,
        payload_hash: str,
        raw_payload: bytes,
        failure_kind: str,
        error_code: str,
    ) -> None:
        existing = await session.scalar(
            select(DeadLetterEventORM.dlq_id).where(
                DeadLetterEventORM.consumer_name == consumer_name,
                DeadLetterEventORM.payload_hash == payload_hash,
            )
        )
        if existing is not None:
            return
        session.add(
            DeadLetterEventORM(
                dlq_id=new_id(),
                consumer_name=self._name(consumer_name, "consumer_name"),
                event_id=event.event_id if event else None,
                tenant_id=event.tenant_id if event else None,
                event_type=event.event_type if event else None,
                event_version=event.event_version if event else None,
                payload_hash=self._hash(payload_hash),
                raw_payload_b64=base64.b64encode(raw_payload).decode("ascii"),
                failure_kind=self._error(failure_kind, maximum=64),
                error_code=self._error(error_code),
                status="open",
                replay_count=0,
                created_at=utc_now(),
                resolved_at=None,
            )
        )
        await session.flush()

    @staticmethod
    def _future(value: datetime, now: datetime) -> bool:
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value > now

    @staticmethod
    def _name(value: str, name: str) -> str:
        cleaned = value.strip()
        if not cleaned or len(cleaned) > 128:
            raise ValueError(f"{name} must contain 1-128 characters")
        return cleaned

    @staticmethod
    def _hash(value: str) -> str:
        if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
            raise ValueError("payload_hash must be lowercase SHA-256")
        return value

    @staticmethod
    def _error(value: str, *, maximum: int = 128) -> str:
        return value.strip()[:maximum] or "unknown_error"


__all__ = ["SQLInboxStore"]
