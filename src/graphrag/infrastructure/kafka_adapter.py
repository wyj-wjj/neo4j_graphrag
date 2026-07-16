"""Confluent Kafka publisher for versioned domain events."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable, Mapping
from typing import Protocol, cast

from graphrag.domain.errors import DependencyError, OperationTimeoutError, ValidationError
from graphrag.domain.events import EventEnvelope

DeliveryCallback = Callable[[object | None, object | None], None]


class ProducerLike(Protocol):
    def produce(
        self,
        *,
        topic: str,
        key: bytes,
        value: bytes,
        headers: list[tuple[str, bytes]],
        on_delivery: DeliveryCallback,
    ) -> None: ...

    def poll(self, timeout: float) -> object: ...

    def list_topics(self, *, timeout: float) -> object: ...

    def flush(self, timeout: float) -> int: ...


class KafkaEventPublisher:
    """Publish one Envelope only after a Broker delivery acknowledgement."""

    def __init__(
        self,
        config: Mapping[str, object],
        *,
        knowledge_topic: str,
        action_topic: str,
        publish_timeout_seconds: float,
        producer: ProducerLike | None = None,
    ) -> None:
        if publish_timeout_seconds <= 0:
            raise ValueError("publish_timeout_seconds must be positive")
        self.publish_timeout_seconds = publish_timeout_seconds
        self.knowledge_topic = self._validate_topic(knowledge_topic)
        self.action_topic = self._validate_topic(action_topic)
        self._lock = asyncio.Lock()
        if producer is None:
            from confluent_kafka import Producer

            producer = cast(ProducerLike, Producer(dict(config)))
        self.producer = producer

    async def publish(self, event: EventEnvelope) -> None:
        topic = self._topic(event.event_type)
        async with self._lock:
            await asyncio.to_thread(self._publish_blocking, topic, event)

    async def health(self) -> dict[str, str]:
        async with self._lock:
            try:
                await asyncio.to_thread(
                    self.producer.list_topics,
                    timeout=min(self.publish_timeout_seconds, 5.0),
                )
            except Exception as exc:
                raise DependencyError("kafka", "Kafka 元数据查询失败") from exc
        return {"status": "ready"}

    async def close(self) -> None:
        async with self._lock:
            remaining = await asyncio.to_thread(
                self.producer.flush,
                min(self.publish_timeout_seconds, 10.0),
            )
        if remaining:
            raise DependencyError("kafka", "Kafka Publisher 关闭时仍有未确认消息")

    def _publish_blocking(self, topic: str, event: EventEnvelope) -> None:
        delivered = threading.Event()
        delivery_error: list[object] = []

        def on_delivery(error: object | None, _message: object | None) -> None:
            if error is not None:
                delivery_error.append(error)
            delivered.set()

        try:
            self.producer.produce(
                topic=topic,
                key=f"{event.tenant_id}:{event.aggregate_id}".encode(),
                value=event.model_dump_json().encode(),
                headers=[
                    ("event_id", event.event_id.encode()),
                    ("event_type", event.event_type.encode()),
                    ("event_version", str(event.event_version).encode()),
                    ("aggregate_version", str(event.aggregate_version).encode()),
                    ("trace_id", event.trace_id.encode()),
                ],
                on_delivery=on_delivery,
            )
        except BufferError as exc:
            raise DependencyError("kafka", "Kafka Producer 本地队列已满") from exc
        except Exception as exc:
            raise DependencyError("kafka", "Kafka 事件提交失败") from exc

        deadline = time.monotonic() + self.publish_timeout_seconds
        while not delivered.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise OperationTimeoutError("kafka")
            self.producer.poll(min(remaining, 0.05))
        if delivery_error:
            raise DependencyError("kafka", "Kafka Broker 拒绝事件")

    def _topic(self, event_type: str) -> str:
        if event_type.startswith("document.") or event_type.startswith(("vector.", "graph.")):
            return self.knowledge_topic
        if event_type.startswith("action."):
            return self.action_topic
        raise ValidationError("事件类型没有受控 Kafka Topic 映射")

    @staticmethod
    def _validate_topic(value: str) -> str:
        cleaned = value.strip()
        if not cleaned or len(cleaned) > 249:
            raise ValueError("Kafka topic must contain 1-249 characters")
        if any(
            character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
            for character in cleaned
        ):
            raise ValueError("Kafka topic contains unsafe characters")
        return cleaned


__all__ = ["KafkaEventPublisher", "ProducerLike"]
