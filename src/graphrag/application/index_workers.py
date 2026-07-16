"""Idempotent Kafka handlers for rebuildable vector and graph indexes."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

from graphrag.domain.errors import NotFoundError, ValidationError
from graphrag.domain.events import EventEnvelope
from graphrag.domain.models import ChunkRecord, GraphExtraction, StrictModel
from graphrag.domain.ports import (
    EmbeddingPort,
    EntityExtractorPort,
    GraphStorePort,
    KnowledgeRepositoryPort,
    VectorStorePort,
)


class ChunkedEventPayload(StrictModel):
    version_id: str
    version: int
    chunk_count: int


class VectorIndexEventHandler:
    def __init__(
        self,
        *,
        repository: KnowledgeRepositoryPort,
        embedding: EmbeddingPort,
        vector_store: VectorStorePort,
        timeout_seconds: float,
    ) -> None:
        self.repository = repository
        self.embedding = embedding
        self.vector_store = vector_store
        self.timeout_seconds = timeout_seconds

    async def __call__(self, event: EventEnvelope) -> None:
        if event.event_type == "document.chunked":
            chunks = await _load_event_chunks(self.repository, event)
            try:
                result = await self.embedding.embed(
                    [item.content for item in chunks],
                    input_type="document",
                    timeout=self.timeout_seconds,
                )
                expected_dimension = chunks[0].embedding_dimension
                expected_version = chunks[0].embedding_version
                if result.dimension != expected_dimension or result.version != expected_version:
                    raise ValidationError("Embedding 响应与 Chunk 版本或维度不匹配")
                await self.vector_store.upsert(chunks, result.vectors)
            except Exception as exc:
                await self.repository.set_index_status(
                    event.tenant_id,
                    [item.chunk_id for item in chunks],
                    index="vector",
                    succeeded=False,
                    error=type(exc).__name__,
                )
                raise
            await self.repository.set_index_status(
                event.tenant_id,
                [item.chunk_id for item in chunks],
                index="vector",
                succeeded=True,
            )
        elif event.event_type == "document.inactivated":
            await self._delete_document(event)

    async def _delete_document(self, event: EventEnvelope) -> None:
        versions = await self.repository.list_versions(event.tenant_id, event.aggregate_id)
        for version in versions:
            chunks = await self.repository.list_chunks_for_version(
                event.tenant_id, version.version_id
            )
            if chunks:
                await self.vector_store.delete(event.tenant_id, [item.chunk_id for item in chunks])


class GraphIndexEventHandler:
    def __init__(
        self,
        *,
        repository: KnowledgeRepositoryPort,
        extractor: EntityExtractorPort,
        graph_store: GraphStorePort,
        timeout_seconds: float,
        concurrency: int = 4,
    ) -> None:
        if concurrency < 1:
            raise ValueError("graph extraction concurrency must be positive")
        self.repository = repository
        self.extractor = extractor
        self.graph_store = graph_store
        self.timeout_seconds = timeout_seconds
        self.concurrency = concurrency

    async def __call__(self, event: EventEnvelope) -> None:
        if event.event_type == "document.chunked":
            chunks = await _load_event_chunks(self.repository, event)
            semaphore = asyncio.Semaphore(self.concurrency)

            async def extract(chunk: ChunkRecord) -> GraphExtraction:
                async with semaphore:
                    return await self.extractor.extract(
                        chunk.content,
                        timeout=self.timeout_seconds,
                    )

            try:
                extractions = await asyncio.gather(*(extract(item) for item in chunks))
                await self.graph_store.upsert(chunks, extractions)
            except Exception as exc:
                await self.repository.set_index_status(
                    event.tenant_id,
                    [item.chunk_id for item in chunks],
                    index="graph",
                    succeeded=False,
                    error=type(exc).__name__,
                )
                raise
            await self.repository.set_index_status(
                event.tenant_id,
                [item.chunk_id for item in chunks],
                index="graph",
                succeeded=True,
            )
        elif event.event_type == "document.inactivated":
            versions = await self.repository.list_versions(event.tenant_id, event.aggregate_id)
            for version in versions:
                await self.graph_store.delete_document_version(event.tenant_id, version.version_id)


async def _load_event_chunks(
    repository: KnowledgeRepositoryPort,
    event: EventEnvelope,
) -> Sequence[ChunkRecord]:
    payload = ChunkedEventPayload.model_validate(event.payload_summary)
    if payload.version != event.aggregate_version:
        raise ValidationError("事件聚合版本与载荷版本不一致")
    version = await repository.get_version(event.tenant_id, payload.version_id)
    if version is None:
        raise NotFoundError("事件引用的文档版本不存在")
    if (
        version.document_id != event.aggregate_id
        or version.tenant_id != event.tenant_id
        or version.version != event.aggregate_version
    ):
        raise ValidationError("事件与 MySQL 文档版本不一致")
    chunks = [
        item
        for item in await repository.list_chunks_for_version(event.tenant_id, payload.version_id)
        if item.chunk_kind == "child"
    ]
    if not chunks or payload.chunk_count < len(chunks):
        raise ValidationError("事件 Chunk 数量与 MySQL 不一致")
    return chunks


__all__ = ["GraphIndexEventHandler", "VectorIndexEventHandler"]
