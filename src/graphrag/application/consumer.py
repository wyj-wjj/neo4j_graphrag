"""Version-aware, idempotent consumer processing independent of Kafka transport."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

from pydantic import ValidationError as PydanticValidationError

from graphrag.domain.errors import AppError, ConflictError
from graphrag.domain.events import EventEnvelope
from graphrag.domain.ids import new_id
from graphrag.domain.models import utc_now
from graphrag.domain.ports import InboxStorePort

EventHandler = Callable[[EventEnvelope], Awaitable[None]]
ConsumerOutcome = Literal["processed", "duplicate", "stale", "busy", "retry", "dlq"]


@dataclass(frozen=True, slots=True)
class ConsumerProcessResult:
    outcome: ConsumerOutcome
    event_id: str | None
    attempt_count: int = 0


class EventConsumerProcessor:
    """Apply Inbox ownership and DLQ policy before Kafka offsets are committed."""

    def __init__(
        self,
        *,
        consumer_name: str,
        store: InboxStorePort,
        handler: EventHandler,
        supported_event_version: int,
        max_event_bytes: int,
        lease_seconds: int,
        handler_timeout_seconds: float,
        max_attempts: int,
        retry_base_seconds: float,
        retry_max_seconds: float,
        worker_id: str | None = None,
    ) -> None:
        if not consumer_name.strip():
            raise ValueError("consumer_name is required")
        if supported_event_version < 1 or max_event_bytes < 1 or lease_seconds < 1:
            raise ValueError("invalid consumer limits")
        if handler_timeout_seconds <= 0 or lease_seconds <= handler_timeout_seconds:
            raise ValueError("Inbox lease must exceed the handler timeout")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if retry_base_seconds <= 0 or retry_max_seconds < retry_base_seconds:
            raise ValueError("invalid consumer retry interval")
        self.consumer_name = consumer_name.strip()
        self.store = store
        self.handler = handler
        self.supported_event_version = supported_event_version
        self.max_event_bytes = max_event_bytes
        self.lease_seconds = lease_seconds
        self.handler_timeout_seconds = handler_timeout_seconds
        self.max_attempts = max_attempts
        self.retry_base_seconds = retry_base_seconds
        self.retry_max_seconds = retry_max_seconds
        self.worker_id = worker_id or f"consumer-{new_id()}"

    async def process(self, raw_payload: bytes) -> ConsumerProcessResult:
        payload_hash = hashlib.sha256(raw_payload).hexdigest()
        if len(raw_payload) > self.max_event_bytes:
            await self.store.record_invalid(
                consumer_name=self.consumer_name,
                payload_hash=payload_hash,
                raw_payload=b"",
                failure_kind="event_too_large",
                error_code="event_too_large",
            )
            return ConsumerProcessResult(outcome="dlq", event_id=None)
        try:
            event = EventEnvelope.model_validate_json(raw_payload)
        except (PydanticValidationError, UnicodeDecodeError, ValueError) as exc:
            await self.store.record_invalid(
                consumer_name=self.consumer_name,
                payload_hash=payload_hash,
                raw_payload=raw_payload,
                failure_kind="invalid_envelope",
                error_code=type(exc).__name__,
            )
            return ConsumerProcessResult(outcome="dlq", event_id=None)
        try:
            claim = await self.store.claim(
                consumer_name=self.consumer_name,
                event=event,
                payload_hash=payload_hash,
                worker_id=self.worker_id,
                lease_seconds=self.lease_seconds,
            )
        except ConflictError:
            await self.store.record_invalid(
                consumer_name=self.consumer_name,
                payload_hash=payload_hash,
                raw_payload=raw_payload,
                failure_kind="event_id_payload_conflict",
                error_code="event_id_payload_conflict",
            )
            return ConsumerProcessResult(outcome="dlq", event_id=event.event_id)
        if claim.disposition == "duplicate":
            return ConsumerProcessResult(
                outcome="duplicate",
                event_id=event.event_id,
                attempt_count=claim.attempt_count,
            )
        if claim.disposition == "stale":
            return ConsumerProcessResult(
                outcome="stale",
                event_id=event.event_id,
                attempt_count=claim.attempt_count,
            )
        if claim.disposition == "busy":
            return ConsumerProcessResult(
                outcome="busy",
                event_id=event.event_id,
                attempt_count=claim.attempt_count,
            )
        if event.event_version != self.supported_event_version:
            await self._dlq(
                event,
                payload_hash,
                raw_payload,
                failure_kind="unknown_event_version",
                error_code=f"unsupported_event_version_{event.event_version}",
            )
            return ConsumerProcessResult(
                outcome="dlq", event_id=event.event_id, attempt_count=claim.attempt_count
            )
        try:
            async with asyncio.timeout(self.handler_timeout_seconds):
                await self.handler(event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            permanent = isinstance(exc, AppError) and not exc.retryable
            if permanent or claim.attempt_count >= self.max_attempts:
                await self._dlq(
                    event,
                    payload_hash,
                    raw_payload,
                    failure_kind="handler_permanent" if permanent else "attempts_exhausted",
                    error_code=type(exc).__name__,
                )
                return ConsumerProcessResult(
                    outcome="dlq",
                    event_id=event.event_id,
                    attempt_count=claim.attempt_count,
                )
            await self.store.release_for_retry(
                consumer_name=self.consumer_name,
                event_id=event.event_id,
                worker_id=self.worker_id,
                available_at=utc_now() + timedelta(seconds=self._retry_delay(claim.attempt_count)),
                error_code=type(exc).__name__,
            )
            return ConsumerProcessResult(
                outcome="retry",
                event_id=event.event_id,
                attempt_count=claim.attempt_count,
            )
        await self.store.mark_processed(
            consumer_name=self.consumer_name,
            event_id=event.event_id,
            worker_id=self.worker_id,
            processed_at=utc_now(),
        )
        return ConsumerProcessResult(
            outcome="processed",
            event_id=event.event_id,
            attempt_count=claim.attempt_count,
        )

    async def _dlq(
        self,
        event: EventEnvelope,
        payload_hash: str,
        raw_payload: bytes,
        *,
        failure_kind: str,
        error_code: str,
    ) -> None:
        await self.store.move_to_dlq(
            consumer_name=self.consumer_name,
            event=event,
            payload_hash=payload_hash,
            raw_payload=raw_payload,
            worker_id=self.worker_id,
            failure_kind=failure_kind,
            error_code=error_code,
        )

    def _retry_delay(self, attempt_count: int) -> float:
        factor = float(2 ** min(max(attempt_count - 1, 0), 16))
        return float(min(self.retry_max_seconds, self.retry_base_seconds * factor))


__all__ = ["ConsumerProcessResult", "EventConsumerProcessor", "EventHandler"]
