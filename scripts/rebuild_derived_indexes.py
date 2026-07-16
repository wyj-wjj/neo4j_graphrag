"""Rebuild one tenant's Milvus and Neo4j indexes from the MySQL truth source."""

from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

from graphrag.application.index_workers import GraphIndexEventHandler, VectorIndexEventHandler
from graphrag.config import Settings
from graphrag.domain.events import EventEnvelope
from graphrag.domain.ids import new_id
from graphrag.domain.models import DocumentStatus
from graphrag.infrastructure.database import Database, SQLKnowledgeRepository
from graphrag.infrastructure.milvus_adapter import MilvusVectorStore
from graphrag.infrastructure.model_adapter import OpenAICompatibleProvider
from graphrag.infrastructure.neo4j_adapter import Neo4jGraphStore


async def _active_events(
    repository: SQLKnowledgeRepository,
    tenant_id: str,
) -> tuple[list[EventEnvelope], set[str]]:
    events: list[EventEnvelope] = []
    chunk_ids: set[str] = set()
    offset = 0
    while True:
        documents = await repository.list_documents(tenant_id, offset=offset, limit=100)
        if not documents:
            break
        for document in documents:
            if document.status is not DocumentStatus.ACTIVE:
                continue
            versions = await repository.list_versions(tenant_id, document.document_id)
            active_versions = [item for item in versions if item.valid_until is None]
            if len(active_versions) != 1:
                raise RuntimeError(
                    f"active document {document.document_id} has "
                    f"{len(active_versions)} active versions"
                )
            version = active_versions[0]
            chunks = [
                item
                for item in await repository.list_chunks_for_version(tenant_id, version.version_id)
                if item.chunk_kind == "child"
            ]
            if not chunks:
                raise RuntimeError(f"active version {version.version_id} has no child chunks")
            chunk_ids.update(item.chunk_id for item in chunks)
            events.append(
                EventEnvelope(
                    event_type="document.chunked",
                    aggregate_version=version.version,
                    tenant_id=tenant_id,
                    aggregate_id=document.document_id,
                    trace_id=new_id(),
                    payload_summary={
                        "version_id": version.version_id,
                        "version": version.version,
                        "chunk_count": len(chunks),
                    },
                )
            )
        offset += len(documents)
    return events, chunk_ids


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    settings = Settings()
    if settings.use_fake_external_clients:
        raise RuntimeError("derived-index rebuild requires SQL-backed real adapters")
    if settings.llm_api_key is None or settings.neo4j_password is None:
        raise RuntimeError("derived-index rebuild requires model and Neo4j credentials")

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
    vector = MilvusVectorStore(
        uri=settings.milvus_uri,
        token=settings.milvus_token.get_secret_value() if settings.milvus_token else None,
        collection=settings.milvus_collection,
        dimension=settings.embedding_dimension,
        embedding_version=settings.embedding_version,
    )
    graph = Neo4jGraphStore(
        settings.neo4j_uri,
        settings.neo4j_username,
        settings.neo4j_password.get_secret_value(),
        settings.neo4j_database,
    )
    try:
        await database.health()
        await vector.ensure_schema()
        await graph.ensure_schema()
        events, expected_ids = await _active_events(repository, args.tenant_id)
        report: dict[str, Any] = {
            "schema_version": "derived-index-rebuild-v1",
            "tenant_id": args.tenant_id,
            "active_documents": len(events),
            "expected_child_chunks": len(expected_ids),
        }
        if not args.execute:
            report["status"] = "dry_run"
            return report
        if args.confirm_tenant_id != args.tenant_id:
            raise RuntimeError("--confirm-tenant-id must exactly match --tenant-id")

        await vector.delete_tenant(args.tenant_id)
        await graph.delete_tenant(args.tenant_id)
        vector_handler = VectorIndexEventHandler(
            repository=repository,
            embedding=provider,
            vector_store=vector,
            timeout_seconds=settings.embedding_timeout_seconds,
        )
        graph_handler = GraphIndexEventHandler(
            repository=repository,
            extractor=provider,
            graph_store=graph,
            timeout_seconds=settings.generation_timeout_seconds,
            concurrency=settings.ingestion_concurrency,
        )
        for event in events:
            await vector_handler(event)
            await graph_handler(event)

        vector_ids = await vector.list_ids(args.tenant_id)
        graph_ids = await graph.list_chunk_ids(args.tenant_id)
        report.update(
            {
                "status": "completed"
                if vector_ids == expected_ids and graph_ids == expected_ids
                else "verification_failed",
                "vector_ids": len(vector_ids),
                "graph_ids": len(graph_ids),
                "vector_missing": sorted(expected_ids - vector_ids)[:20],
                "vector_unexpected": sorted(vector_ids - expected_ids)[:20],
                "graph_missing": sorted(expected_ids - graph_ids)[:20],
                "graph_unexpected": sorted(graph_ids - expected_ids)[:20],
            }
        )
        if report["status"] != "completed":
            raise RuntimeError(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return report
    finally:
        await graph.close()
        await vector.close()
        await provider.close()
        await database.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm-tenant-id")
    args = parser.parse_args()
    report = asyncio.run(_run(args))
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
