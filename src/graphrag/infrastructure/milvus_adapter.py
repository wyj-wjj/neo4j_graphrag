"""Milvus 2.6 adapter with schema/version guards and bounded blocking calls."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from functools import partial
from typing import Any

from pymilvus import DataType, MilvusClient

from graphrag.domain.errors import DependencyError
from graphrag.domain.models import ChunkRecord, SearchCandidate


class MilvusVectorStore:
    def __init__(
        self,
        *,
        uri: str,
        token: str | None,
        collection: str,
        dimension: int,
        embedding_version: str,
        max_concurrency: int = 8,
    ) -> None:
        self.client = MilvusClient(uri=uri, token=token or "")
        self.collection = collection
        self.dimension = dimension
        self.embedding_version = embedding_version
        self._semaphore = asyncio.Semaphore(max_concurrency)

    async def _call(self, function: Any, *args: Any, **kwargs: Any) -> Any:
        async with self._semaphore:
            return await asyncio.to_thread(partial(function, *args, **kwargs))

    async def ensure_schema(self) -> None:
        try:
            exists = await self._call(self.client.has_collection, self.collection)
            if not exists:
                schema = self.client.create_schema(auto_id=False, enable_dynamic_field=False)
                schema.add_field("chunk_id", DataType.VARCHAR, is_primary=True, max_length=64)
                schema.add_field("dense_vector", DataType.FLOAT_VECTOR, dim=self.dimension)
                schema.add_field("tenant_id", DataType.VARCHAR, max_length=128)
                schema.add_field("document_id", DataType.VARCHAR, max_length=64)
                schema.add_field("status", DataType.VARCHAR, max_length=32)
                schema.add_field("embedding_version", DataType.VARCHAR, max_length=100)
                index = self.client.prepare_index_params()
                index.add_index(
                    field_name="dense_vector",
                    index_type="HNSW",
                    metric_type="COSINE",
                    params={"M": 16, "efConstruction": 200},
                )
                await self._call(
                    self.client.create_collection,
                    collection_name=self.collection,
                    schema=schema,
                    index_params=index,
                )
                return
            description = await self._call(self.client.describe_collection, self.collection)
            vector_fields = [
                item for item in description["fields"] if item["name"] == "dense_vector"
            ]
            if not vector_fields or int(vector_fields[0]["params"]["dim"]) != self.dimension:
                raise ValueError("existing Milvus collection has incompatible vector dimension")
        except Exception as exc:
            if isinstance(exc, ValueError):
                raise
            raise DependencyError("milvus", "Milvus Schema 检查失败") from exc

    async def upsert(
        self, chunks: Sequence[ChunkRecord], vectors: Sequence[Sequence[float]]
    ) -> None:
        if len(chunks) != len(vectors):
            raise ValueError("chunks and vectors must have equal length")
        data = []
        for chunk, vector in zip(chunks, vectors, strict=True):
            if len(vector) != self.dimension or chunk.embedding_version != self.embedding_version:
                raise ValueError("incompatible embedding dimension or version")
            data.append(
                {
                    "chunk_id": chunk.chunk_id,
                    "dense_vector": list(vector),
                    "tenant_id": chunk.tenant_id,
                    "document_id": chunk.document_id,
                    "status": chunk.status.value,
                    "embedding_version": chunk.embedding_version,
                }
            )
        await self._call(self.client.upsert, collection_name=self.collection, data=data)

    async def search(
        self, tenant_id: str, vector: Sequence[float], *, top_k: int
    ) -> list[SearchCandidate]:
        filter_expression = (
            f'tenant_id == {json.dumps(tenant_id)} and status == "active" and '
            f"embedding_version == {json.dumps(self.embedding_version)}"
        )
        rows = await self._call(
            self.client.search,
            collection_name=self.collection,
            data=[list(vector)],
            limit=top_k,
            filter=filter_expression,
            output_fields=["chunk_id"],
            search_params={"metric_type": "COSINE", "params": {"ef": max(64, top_k)}},
            consistency_level="Strong",
        )
        return [
            SearchCandidate(
                chunk_id=str(item["entity"]["chunk_id"]),
                score=float(item["distance"]),
                source="dense",
                rank=rank,
            )
            for rank, item in enumerate(rows[0], start=1)
        ]

    async def delete(self, tenant_id: str, chunk_ids: Sequence[str]) -> None:
        if not chunk_ids:
            return
        safe_ids = ",".join(json.dumps(item) for item in chunk_ids)
        await self._call(
            self.client.delete,
            collection_name=self.collection,
            filter=f"tenant_id == {json.dumps(tenant_id)} and chunk_id in [{safe_ids}]",
        )

    async def list_ids(self, tenant_id: str) -> set[str]:
        rows = await self._call(
            self.client.query,
            collection_name=self.collection,
            filter=f"tenant_id == {json.dumps(tenant_id)}",
            output_fields=["chunk_id"],
            limit=16384,
            consistency_level="Strong",
        )
        return {str(item["chunk_id"]) for item in rows}

    async def close(self) -> None:
        await self._call(self.client.close)
