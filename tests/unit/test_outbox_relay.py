from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from graphrag.application.consumer import ConsumerProcessResult
from graphrag.application.outbox import OutboxRelay
from graphrag.config import KafkaSecurityProtocol, Settings
from graphrag.domain.events import EventEnvelope, OutboxRecord
from graphrag.domain.ids import new_id
from graphrag.domain.models import utc_now
from graphrag.index_worker import (
    IndexWorkerRole,
    build_index_worker_runtime,
    kafka_security_config,
)
from graphrag.infrastructure.kafka_adapter import KafkaEventPublisher
from graphrag.infrastructure.kafka_consumer import (
    KafkaConsumerClient,
    KafkaConsumerRunner,
    kafka_consumer_config,
)


def _record(event_type: str = "document.received", *, retry_count: int = 0) -> OutboxRecord:
    now = utc_now()
    return OutboxRecord(
        event=EventEnvelope(
            event_type=event_type,  # type: ignore[arg-type]
            tenant_id="tenant-a",
            aggregate_id=new_id(),
            trace_id=new_id(),
        ),
        retry_count=retry_count,
        available_at=now,
        lease_owner="relay-a",
        lease_expires_at=now + timedelta(seconds=30),
    )


class MemoryOutboxStore:
    def __init__(self, records: list[OutboxRecord]) -> None:
        self.records = records
        self.marked: list[str] = []
        self.released: list[tuple[str, str]] = []

    async def claim(self, *, worker_id: str, limit: int, lease_seconds: int) -> list[OutboxRecord]:
        del worker_id, lease_seconds
        return self.records[:limit]

    async def mark_published(self, event_id: str, *, worker_id: str, published_at: Any) -> None:
        del worker_id, published_at
        self.marked.append(event_id)

    async def release_for_retry(
        self,
        event_id: str,
        *,
        worker_id: str,
        available_at: Any,
        error_code: str,
    ) -> None:
        del worker_id, available_at
        self.released.append((event_id, error_code))


class RecordingPublisher:
    def __init__(self, *, fail_on: str | None = None) -> None:
        self.fail_on = fail_on
        self.events: list[EventEnvelope] = []

    async def publish(self, event: EventEnvelope) -> None:
        self.events.append(event)
        if event.event_id == self.fail_on:
            raise RuntimeError("broker unavailable")


@pytest.mark.asyncio
async def test_outbox_relay_marks_acknowledged_events() -> None:
    records = [_record(), _record("document.chunked")]
    store = MemoryOutboxStore(records)
    publisher = RecordingPublisher()
    relay = OutboxRelay(
        store=store,
        publisher=publisher,
        batch_size=10,
        lease_seconds=30,
        poll_interval_seconds=0.1,
        retry_base_seconds=1,
        retry_max_seconds=60,
        worker_id="relay-a",
    )

    result = await relay.run_once()

    assert result.claimed == 2
    assert result.published == 2
    assert result.failed == 0
    assert store.marked == [record.event.event_id for record in records]
    assert store.released == []


@pytest.mark.asyncio
async def test_outbox_relay_releases_failure_without_overtaking_later_event() -> None:
    records = [_record(retry_count=2), _record("document.chunked")]
    store = MemoryOutboxStore(records)
    publisher = RecordingPublisher(fail_on=records[0].event.event_id)
    relay = OutboxRelay(
        store=store,
        publisher=publisher,
        batch_size=10,
        lease_seconds=30,
        poll_interval_seconds=0.1,
        retry_base_seconds=1,
        retry_max_seconds=60,
        worker_id="relay-a",
    )

    result = await relay.run_once()

    assert result.published == 0
    assert result.failed == 1
    assert [event.event_id for event in publisher.events] == [records[0].event.event_id]
    assert store.marked == []
    assert store.released == [(records[0].event.event_id, "RuntimeError")]


class FakeProducer:
    def __init__(self, *, delivery_error: object | None = None) -> None:
        self.delivery_error = delivery_error
        self.pending: Any = None
        self.messages: list[dict[str, Any]] = []

    def produce(self, **kwargs: Any) -> None:
        self.messages.append(kwargs)
        self.pending = kwargs["on_delivery"]

    def poll(self, _timeout: float) -> int:
        if self.pending is not None:
            callback = self.pending
            self.pending = None
            callback(self.delivery_error, object())
        return 1

    def list_topics(self, *, timeout: float) -> object:
        del timeout
        return object()

    def flush(self, timeout: float) -> int:
        del timeout
        return 0


@pytest.mark.asyncio
async def test_kafka_publisher_routes_versioned_envelope_after_broker_ack() -> None:
    producer = FakeProducer()
    publisher = KafkaEventPublisher(
        {},
        knowledge_topic="knowledge.events.v1",
        action_topic="action.events.v1",
        publish_timeout_seconds=1,
        producer=producer,
    )
    event = _record("vector.index.requested").event

    await publisher.publish(event)

    message = producer.messages[0]
    assert message["topic"] == "knowledge.events.v1"
    assert message["key"] == f"{event.tenant_id}:{event.aggregate_id}".encode()
    assert EventEnvelope.model_validate_json(message["value"]) == event
    headers = dict(message["headers"])
    assert headers["event_id"] == event.event_id.encode()
    assert headers["aggregate_version"] == str(event.aggregate_version).encode()
    assert await publisher.health() == {"status": "ready"}


@pytest.mark.asyncio
async def test_kafka_publisher_surfaces_negative_broker_ack() -> None:
    producer = FakeProducer(delivery_error=RuntimeError("rejected"))
    publisher = KafkaEventPublisher(
        {},
        knowledge_topic="knowledge.events.v1",
        action_topic="action.events.v1",
        publish_timeout_seconds=1,
        producer=producer,
    )

    with pytest.raises(Exception, match="Broker"):
        await publisher.publish(_record("action.draft.created").event)


class FakeMessage:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def error(self) -> None:
        return None

    def value(self) -> bytes:
        return self.payload

    def topic(self) -> str:
        return "knowledge.events.v1"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 12


class FakeConsumer:
    def __init__(self, message: FakeMessage) -> None:
        self.message: FakeMessage | None = message
        self.subscriptions: list[str] = []
        self.committed = 0
        self.sought = 0

    def subscribe(self, topics: list[str]) -> None:
        self.subscriptions = topics

    def poll(self, timeout: float) -> FakeMessage | None:
        del timeout
        message = self.message
        self.message = None
        return message

    def commit(self, *, message: Any, asynchronous: bool) -> object:
        del message, asynchronous
        self.committed += 1
        return object()

    def seek(self, partition: object) -> None:
        del partition
        self.sought += 1

    def list_topics(self, *, timeout: float) -> object:
        del timeout
        return object()

    def close(self) -> None:
        return None


class StubProcessor:
    consumer_name = "embedding-worker"

    def __init__(self, outcome: str) -> None:
        self.outcome = outcome

    async def process(self, payload: bytes) -> ConsumerProcessResult:
        del payload
        return ConsumerProcessResult(outcome=self.outcome, event_id="event")  # type: ignore[arg-type]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "commits", "seeks"),
    [("processed", 1, 0), ("duplicate", 1, 0), ("dlq", 1, 0), ("retry", 0, 1)],
)
async def test_kafka_consumer_commits_only_terminal_inbox_outcomes(
    outcome: str, commits: int, seeks: int
) -> None:
    event = _record().event
    consumer = FakeConsumer(FakeMessage(event.model_dump_json().encode()))
    client = KafkaConsumerClient(
        {},
        topics=("knowledge.events.v1",),
        metadata_timeout_seconds=1,
        consumer=consumer,
    )
    runner = KafkaConsumerRunner(
        client=client,
        processor=StubProcessor(outcome),  # type: ignore[arg-type]
    )

    result = await runner.run_once()

    assert result is not None and result.outcome == outcome
    assert consumer.committed == commits
    assert consumer.sought == seeks


def test_kafka_consumer_config_disables_automatic_offset_commit() -> None:
    config = kafka_consumer_config(
        bootstrap_servers="broker:9092",
        group_id="embedding-v1",
        client_id="embedding-1",
        security={"security.protocol": "SSL"},
        max_event_bytes=1024 * 1024,
    )
    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False
    assert config["isolation.level"] == "read_committed"


def test_index_worker_kafka_security_and_fail_closed_composition() -> None:
    credential = new_id()
    settings = Settings(
        kafka_enabled=True,
        kafka_bootstrap_servers="broker:9093",
        kafka_security_protocol=KafkaSecurityProtocol.SASL_SSL,
        kafka_sasl_username="worker",
        kafka_sasl_password=credential,
    )

    security = kafka_security_config(settings)

    assert security == {
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "worker",
        "sasl.password": credential,
    }
    with pytest.raises(ValueError, match="SQL-backed"):
        build_index_worker_runtime(settings, IndexWorkerRole.VECTOR)
