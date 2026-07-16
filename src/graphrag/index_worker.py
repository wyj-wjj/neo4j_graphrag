"""Standalone Kafka consumers for rebuildable vector and graph indexes."""

from __future__ import annotations

import argparse
import asyncio
import signal
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import structlog

from graphrag.application.consumer import EventConsumerProcessor, EventHandler
from graphrag.application.index_workers import GraphIndexEventHandler, VectorIndexEventHandler
from graphrag.config import KafkaSecurityProtocol, Settings, get_settings
from graphrag.infrastructure.database import Database, SQLKnowledgeRepository
from graphrag.infrastructure.inbox import SQLInboxStore
from graphrag.infrastructure.kafka_consumer import (
    KafkaConsumerClient,
    KafkaConsumerRunner,
    kafka_consumer_config,
)
from graphrag.infrastructure.milvus_adapter import MilvusVectorStore
from graphrag.infrastructure.model_adapter import OpenAICompatibleProvider
from graphrag.infrastructure.neo4j_adapter import Neo4jGraphStore
from graphrag.observability.logging import configure_logging

logger = structlog.get_logger(__name__)


class IndexWorkerRole(StrEnum):
    VECTOR = "vector"
    GRAPH = "graph"


@dataclass(slots=True)
class IndexWorkerRuntime:
    role: IndexWorkerRole
    database: Database
    client: KafkaConsumerClient
    runner: KafkaConsumerRunner
    schema_store: Any
    closeables: tuple[Any, ...]

    async def start(self) -> None:
        await self.database.health()
        await self.schema_store.ensure_schema()
        await self.client.health()
        await self.runner.start()

    async def close(self) -> None:
        errors: list[Exception] = []
        try:
            await self.runner.stop()
        except Exception as exc:
            errors.append(exc)
        try:
            await self.client.close()
        except Exception as exc:
            errors.append(exc)
        for resource in reversed(self.closeables):
            try:
                await resource.close()
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise errors[0]


def kafka_security_config(settings: Settings) -> dict[str, object]:
    config: dict[str, object] = {
        "security.protocol": settings.kafka_security_protocol.value,
    }
    if settings.kafka_security_protocol in {
        KafkaSecurityProtocol.SASL_PLAINTEXT,
        KafkaSecurityProtocol.SASL_SSL,
    }:
        if settings.kafka_sasl_username is None or settings.kafka_sasl_password is None:
            raise ValueError("SASL Kafka credentials are required")
        config.update(
            {
                "sasl.mechanism": settings.kafka_sasl_mechanism,
                "sasl.username": settings.kafka_sasl_username,
                "sasl.password": settings.kafka_sasl_password.get_secret_value(),
            }
        )
    return config


def build_index_worker_runtime(
    settings: Settings,
    role: IndexWorkerRole,
) -> IndexWorkerRuntime:
    """Compose one index worker without performing network I/O."""

    if settings.use_fake_external_clients:
        raise ValueError("index workers require SQL-backed real adapters")
    if not settings.kafka_enabled:
        raise ValueError("index workers require KAFKA_ENABLED=true")
    if settings.llm_api_key is None:
        raise ValueError("index workers require LLM_API_KEY")

    database = Database(settings.database_url, pool_size=settings.database_pool_size)
    repository = SQLKnowledgeRepository(database)
    provider = OpenAICompatibleProvider(
        api_key=settings.llm_api_key.get_secret_value(),
        base_url=str(settings.llm_base_url),
        embedding_model=settings.embedding_model,
        embedding_dimension=settings.embedding_dimension,
        embedding_version=settings.embedding_version,
        extractor_model=settings.router_model,
    )
    handler: EventHandler
    if role is IndexWorkerRole.VECTOR:
        schema_store: Any = MilvusVectorStore(
            uri=settings.milvus_uri,
            token=settings.milvus_token.get_secret_value() if settings.milvus_token else None,
            collection=settings.milvus_collection,
            dimension=settings.embedding_dimension,
            embedding_version=settings.embedding_version,
        )
        handler = VectorIndexEventHandler(
            repository=repository,
            embedding=provider,
            vector_store=schema_store,
            timeout_seconds=settings.embedding_timeout_seconds,
        )
        consumer_name = settings.kafka_vector_consumer_group
    else:
        schema_store = Neo4jGraphStore(
            settings.neo4j_uri,
            settings.neo4j_username,
            settings.neo4j_password.get_secret_value() if settings.neo4j_password else "",
            settings.neo4j_database,
        )
        handler = GraphIndexEventHandler(
            repository=repository,
            extractor=provider,
            graph_store=schema_store,
            timeout_seconds=settings.generation_timeout_seconds,
            concurrency=settings.ingestion_concurrency,
        )
        consumer_name = settings.kafka_graph_consumer_group

    client = KafkaConsumerClient(
        kafka_consumer_config(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=consumer_name,
            client_id=f"{settings.kafka_client_id}-{role.value}",
            security=kafka_security_config(settings),
            max_event_bytes=settings.kafka_max_event_bytes,
        ),
        topics=(settings.kafka_knowledge_topic,),
        metadata_timeout_seconds=settings.kafka_metadata_timeout_seconds,
    )
    processor = EventConsumerProcessor(
        consumer_name=consumer_name,
        store=SQLInboxStore(database),
        handler=handler,
        supported_event_version=1,
        max_event_bytes=settings.kafka_max_event_bytes,
        lease_seconds=settings.inbox_lease_seconds,
        handler_timeout_seconds=settings.consumer_handler_timeout_seconds,
        max_attempts=settings.consumer_max_attempts,
        retry_base_seconds=settings.consumer_retry_base_seconds,
        retry_max_seconds=settings.consumer_retry_max_seconds,
    )
    runner = KafkaConsumerRunner(
        client=client,
        processor=processor,
        poll_timeout_seconds=settings.kafka_consumer_poll_timeout_seconds,
        retry_pause_seconds=settings.kafka_consumer_retry_pause_seconds,
    )
    return IndexWorkerRuntime(
        role=role,
        database=database,
        client=client,
        runner=runner,
        schema_store=schema_store,
        closeables=(provider, schema_store, database),
    )


async def serve_index_worker(runtime: IndexWorkerRuntime) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)
    try:
        await runtime.start()
        logger.info("index_worker_started", role=runtime.role.value)
        await stop.wait()
    finally:
        await runtime.close()
        logger.info("index_worker_stopped", role=runtime.role.value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one Kafka-backed derived-index worker")
    parser.add_argument("role", choices=[item.value for item in IndexWorkerRole])
    args = parser.parse_args()
    settings = get_settings()
    configure_logging(settings.log_level)
    runtime = build_index_worker_runtime(settings, IndexWorkerRole(args.role))
    asyncio.run(serve_index_worker(runtime))


if __name__ == "__main__":
    main()


__all__ = [
    "IndexWorkerRole",
    "IndexWorkerRuntime",
    "build_index_worker_runtime",
    "kafka_security_config",
    "serve_index_worker",
]
