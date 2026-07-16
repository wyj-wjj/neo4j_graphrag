"""Kafka transport loop that commits offsets only for terminal Inbox outcomes."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from typing import Protocol, cast

import structlog

from graphrag.application.consumer import ConsumerProcessResult, EventConsumerProcessor
from graphrag.domain.errors import DependencyError

logger = structlog.get_logger(__name__)


class MessageLike(Protocol):
    def error(self) -> object | None: ...

    def value(self) -> bytes | None: ...

    def topic(self) -> str: ...

    def partition(self) -> int: ...

    def offset(self) -> int: ...


class ConsumerLike(Protocol):
    def subscribe(self, topics: list[str]) -> None: ...

    def poll(self, timeout: float) -> MessageLike | None: ...

    def commit(self, *, message: MessageLike, asynchronous: bool) -> object: ...

    def seek(self, partition: object) -> None: ...

    def list_topics(self, *, timeout: float) -> object: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class KafkaDelivery:
    message: MessageLike
    payload: bytes


class KafkaConsumerClient:
    """Thin injectable wrapper around a Confluent consumer group member."""

    def __init__(
        self,
        config: dict[str, object],
        *,
        topics: tuple[str, ...],
        metadata_timeout_seconds: float,
        consumer: ConsumerLike | None = None,
    ) -> None:
        if not topics:
            raise ValueError("at least one Kafka topic is required")
        self.metadata_timeout_seconds = metadata_timeout_seconds
        if consumer is None:
            from confluent_kafka import Consumer

            consumer = cast(ConsumerLike, Consumer(config))
        self.consumer = consumer
        self.consumer.subscribe(list(topics))

    async def poll(self, timeout_seconds: float) -> KafkaDelivery | None:
        message = await asyncio.to_thread(self.consumer.poll, timeout_seconds)
        if message is None:
            return None
        error = message.error()
        if error is not None:
            raise DependencyError("kafka", "Kafka Consumer 拉取失败")
        payload = message.value()
        if payload is None:
            payload = b""
        return KafkaDelivery(message=message, payload=payload)

    async def commit(self, delivery: KafkaDelivery) -> None:
        try:
            await asyncio.to_thread(
                self.consumer.commit,
                message=delivery.message,
                asynchronous=False,
            )
        except Exception as exc:
            raise DependencyError("kafka", "Kafka Consumer offset 提交失败") from exc

    async def rewind(self, delivery: KafkaDelivery) -> None:
        from confluent_kafka import TopicPartition

        partition = TopicPartition(
            delivery.message.topic(),
            delivery.message.partition(),
            delivery.message.offset(),
        )
        try:
            await asyncio.to_thread(self.consumer.seek, partition)
        except Exception as exc:
            raise DependencyError("kafka", "Kafka Consumer offset 回退失败") from exc

    async def health(self) -> dict[str, str]:
        try:
            await asyncio.to_thread(
                self.consumer.list_topics,
                timeout=self.metadata_timeout_seconds,
            )
        except Exception as exc:
            raise DependencyError("kafka", "Kafka Consumer 元数据查询失败") from exc
        return {"status": "ready"}

    async def close(self) -> None:
        await asyncio.to_thread(self.consumer.close)


class KafkaConsumerRunner:
    """Drive one processor and preserve redelivery for non-terminal outcomes."""

    TERMINAL_OUTCOMES = frozenset({"processed", "duplicate", "stale", "dlq"})

    def __init__(
        self,
        *,
        client: KafkaConsumerClient,
        processor: EventConsumerProcessor,
        poll_timeout_seconds: float = 1.0,
        retry_pause_seconds: float = 1.0,
    ) -> None:
        if poll_timeout_seconds <= 0 or retry_pause_seconds <= 0:
            raise ValueError("Kafka consumer timing must be positive")
        self.client = client
        self.processor = processor
        self.poll_timeout_seconds = poll_timeout_seconds
        self.retry_pause_seconds = retry_pause_seconds
        self._stopping = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is None:
            self._stopping.clear()
            self._task = asyncio.create_task(self._run(), name=self.processor.consumer_name)

    async def stop(self) -> None:
        self._stopping.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def run_once(self) -> ConsumerProcessResult | None:
        delivery = await self.client.poll(self.poll_timeout_seconds)
        if delivery is None:
            return None
        result = await self.processor.process(delivery.payload)
        if result.outcome in self.TERMINAL_OUTCOMES:
            await self.client.commit(delivery)
        else:
            await self.client.rewind(delivery)
        return result

    async def _run(self) -> None:
        while not self._stopping.is_set():
            try:
                result = await self.run_once()
                if result is not None and result.outcome not in self.TERMINAL_OUTCOMES:
                    await self._wait(self.retry_pause_seconds)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "kafka_consumer_cycle_failed",
                    consumer_name=self.processor.consumer_name,
                    error_type=type(exc).__name__,
                )
                await self._wait(self.retry_pause_seconds)

    async def _wait(self, timeout: float) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), timeout=timeout)


def kafka_consumer_config(
    *,
    bootstrap_servers: str,
    group_id: str,
    client_id: str,
    security: dict[str, object],
    max_event_bytes: int,
) -> dict[str, object]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "client.id": client_id,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
        "fetch.message.max.bytes": max_event_bytes,
        **security,
    }


__all__ = [
    "ConsumerLike",
    "KafkaConsumerClient",
    "KafkaConsumerRunner",
    "KafkaDelivery",
    "MessageLike",
    "kafka_consumer_config",
]
