"""Recoverable Outbox Relay application service."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta

import structlog

from graphrag.domain.ids import new_id
from graphrag.domain.models import utc_now
from graphrag.domain.ports import EventPublisherPort, OutboxStorePort

logger = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class OutboxRelayResult:
    claimed: int
    published: int
    failed: int


class OutboxRelay:
    """Relay MySQL Outbox records to Kafka with at-least-once delivery."""

    def __init__(
        self,
        *,
        store: OutboxStorePort,
        publisher: EventPublisherPort,
        batch_size: int,
        lease_seconds: int,
        poll_interval_seconds: float,
        retry_base_seconds: float,
        retry_max_seconds: float,
        worker_id: str | None = None,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if retry_base_seconds <= 0 or retry_max_seconds < retry_base_seconds:
            raise ValueError("invalid Outbox retry interval")
        self.store = store
        self.publisher = publisher
        self.batch_size = batch_size
        self.lease_seconds = lease_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.retry_base_seconds = retry_base_seconds
        self.retry_max_seconds = retry_max_seconds
        self.worker_id = worker_id or f"outbox-{new_id()}"
        self._stopping = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is None:
            self._stopping.clear()
            self._task = asyncio.create_task(self._run(), name="outbox-relay")

    async def stop(self) -> None:
        self._stopping.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def run_once(self) -> OutboxRelayResult:
        records = await self.store.claim(
            worker_id=self.worker_id,
            limit=self.batch_size,
            lease_seconds=self.lease_seconds,
        )
        published = 0
        failed = 0
        for record in records:
            try:
                await self.publisher.publish(record.event)
                await self.store.mark_published(
                    record.event.event_id,
                    worker_id=self.worker_id,
                    published_at=utc_now(),
                )
                published += 1
            except asyncio.CancelledError:
                logger.warning(
                    "outbox_publish_cancelled",
                    event_id=record.event.event_id,
                    event_type=record.event.event_type,
                    tenant_id=record.event.tenant_id,
                    retry_count=record.retry_count,
                )
                raise
            except Exception as exc:
                failed += 1
                delay = self._retry_delay(record.retry_count)
                await self.store.release_for_retry(
                    record.event.event_id,
                    worker_id=self.worker_id,
                    available_at=utc_now() + timedelta(seconds=delay),
                    error_code=type(exc).__name__,
                )
                logger.warning(
                    "outbox_publish_failed",
                    event_id=record.event.event_id,
                    event_type=record.event.event_type,
                    tenant_id=record.event.tenant_id,
                    retry_count=record.retry_count + 1,
                    retry_delay_seconds=delay,
                    error_type=type(exc).__name__,
                )
                # Stop this ordered batch so later events cannot overtake the failed event.
                break
        return OutboxRelayResult(
            claimed=len(records),
            published=published,
            failed=failed,
        )

    async def _run(self) -> None:
        while not self._stopping.is_set():
            try:
                result = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("outbox_relay_cycle_failed", error_type=type(exc).__name__)
                result = OutboxRelayResult(claimed=0, published=0, failed=1)
            if result.claimed == 0 or result.failed:
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        self._stopping.wait(),
                        timeout=self.poll_interval_seconds,
                    )

    def _retry_delay(self, retry_count: int) -> float:
        factor = float(2 ** min(retry_count, 16))
        return float(min(self.retry_max_seconds, self.retry_base_seconds * factor))


__all__ = ["OutboxRelay", "OutboxRelayResult"]
