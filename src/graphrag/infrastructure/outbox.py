"""MySQL-backed Outbox leases used by the phase-two Kafka Relay."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, cast

from sqlalchemy import and_, exists, or_, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import aliased

from graphrag.domain.errors import ConflictError
from graphrag.domain.events import EventEnvelope, OutboxRecord
from graphrag.domain.models import utc_now
from graphrag.infrastructure.database import Database, OutboxEventORM


class SQLOutboxStore:
    """Claim unpublished events with recoverable, short-lived database leases."""

    def __init__(self, database: Database) -> None:
        self.database = database

    async def claim(
        self,
        *,
        worker_id: str,
        limit: int,
        lease_seconds: int,
    ) -> list[OutboxRecord]:
        if not worker_id or len(worker_id) > 128:
            raise ValueError("worker_id must contain 1-128 characters")
        if limit < 1:
            return []
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        now = utc_now()
        lease_expires_at = now + timedelta(seconds=lease_seconds)
        earlier = aliased(OutboxEventORM)
        earlier_unpublished_for_aggregate = exists(
            select(earlier.event_id).where(
                earlier.published.is_(False),
                earlier.tenant_id == OutboxEventORM.tenant_id,
                earlier.aggregate_id == OutboxEventORM.aggregate_id,
                or_(
                    earlier.aggregate_version < OutboxEventORM.aggregate_version,
                    and_(
                        earlier.aggregate_version == OutboxEventORM.aggregate_version,
                        or_(
                            earlier.occurred_at < OutboxEventORM.occurred_at,
                            and_(
                                earlier.occurred_at == OutboxEventORM.occurred_at,
                                earlier.event_id < OutboxEventORM.event_id,
                            ),
                        ),
                    ),
                ),
            )
        )
        async with self.database.session() as session:
            rows = list(
                (
                    await session.scalars(
                        select(OutboxEventORM)
                        .where(
                            OutboxEventORM.published.is_(False),
                            OutboxEventORM.available_at <= now,
                            ~earlier_unpublished_for_aggregate,
                            or_(
                                OutboxEventORM.lease_owner.is_(None),
                                OutboxEventORM.lease_expires_at.is_(None),
                                OutboxEventORM.lease_expires_at <= now,
                            ),
                        )
                        .order_by(OutboxEventORM.occurred_at, OutboxEventORM.event_id)
                        .limit(limit)
                        .with_for_update(skip_locked=True)
                    )
                ).all()
            )
            for row in rows:
                row.lease_owner = worker_id
                row.lease_expires_at = lease_expires_at
            await session.flush()
            return [self._record(row) for row in rows]

    async def mark_published(
        self,
        event_id: str,
        *,
        worker_id: str,
        published_at: datetime,
    ) -> None:
        async with self.database.session() as session:
            result = cast(
                CursorResult[Any],
                await session.execute(
                    update(OutboxEventORM)
                    .where(
                        OutboxEventORM.event_id == event_id,
                        OutboxEventORM.published.is_(False),
                        OutboxEventORM.lease_owner == worker_id,
                    )
                    .values(
                        published=True,
                        published_at=published_at,
                        lease_owner=None,
                        lease_expires_at=None,
                        last_error_code=None,
                    )
                ),
            )
            if result.rowcount != 1:
                raise ConflictError("Outbox 事件租约已丢失或已发布")

    async def release_for_retry(
        self,
        event_id: str,
        *,
        worker_id: str,
        available_at: datetime,
        error_code: str,
    ) -> None:
        cleaned_error = error_code.strip()[:128] or "unknown_publish_error"
        async with self.database.session() as session:
            result = cast(
                CursorResult[Any],
                await session.execute(
                    update(OutboxEventORM)
                    .where(
                        OutboxEventORM.event_id == event_id,
                        OutboxEventORM.published.is_(False),
                        OutboxEventORM.lease_owner == worker_id,
                    )
                    .values(
                        retry_count=OutboxEventORM.retry_count + 1,
                        available_at=available_at,
                        lease_owner=None,
                        lease_expires_at=None,
                        last_error_code=cleaned_error,
                    )
                ),
            )
            if result.rowcount != 1:
                raise ConflictError("Outbox 事件重试租约已丢失")

    @staticmethod
    def _record(row: OutboxEventORM) -> OutboxRecord:
        if row.lease_owner is None or row.lease_expires_at is None:
            raise RuntimeError("claimed Outbox row is missing its lease")
        return OutboxRecord(
            event=EventEnvelope(
                event_id=row.event_id,
                event_type=row.event_type,
                event_version=row.event_version,
                aggregate_version=row.aggregate_version,
                tenant_id=row.tenant_id,
                aggregate_id=row.aggregate_id,
                occurred_at=row.occurred_at,
                trace_id=row.trace_id,
                payload_summary=row.payload_summary,
            ),
            retry_count=row.retry_count,
            available_at=row.available_at,
            lease_owner=row.lease_owner,
            lease_expires_at=row.lease_expires_at,
        )


__all__ = ["SQLOutboxStore"]
