"""Tenant-bounded Neo4j graph adapter with fixed relationship types and hop limits."""

from __future__ import annotations

from collections.abc import Sequence

from neo4j import AsyncDriver, AsyncGraphDatabase, AsyncManagedTransaction

from graphrag.domain.models import ChunkRecord, GraphExtraction, SearchCandidate


class Neo4jGraphStore:
    def __init__(self, uri: str, username: str, password: str, database: str) -> None:
        self.driver: AsyncDriver = AsyncGraphDatabase.driver(uri, auth=(username, password))
        self.database = database

    async def ensure_schema(self) -> None:
        statements = (
            "CREATE CONSTRAINT document_identity IF NOT EXISTS FOR (n:Document) "
            "REQUIRE (n.tenant_id, n.document_id) IS UNIQUE",
            "CREATE CONSTRAINT chunk_identity IF NOT EXISTS FOR (n:Chunk) "
            "REQUIRE (n.tenant_id, n.chunk_id) IS UNIQUE",
            "CREATE CONSTRAINT entity_identity IF NOT EXISTS FOR (n:Entity) "
            "REQUIRE (n.tenant_id, n.entity_id) IS UNIQUE",
            "CREATE INDEX entity_name IF NOT EXISTS FOR (n:Entity) "
            "ON (n.tenant_id, n.normalized_name)",
        )
        async with self.driver.session(database=self.database) as session:
            for statement in statements:
                await session.run(statement)

    async def upsert(
        self, chunks: Sequence[ChunkRecord], extractions: Sequence[GraphExtraction]
    ) -> None:
        if len(chunks) != len(extractions):
            raise ValueError("chunks and extractions must have equal length")
        async with self.driver.session(database=self.database) as session:
            for chunk, extraction in zip(chunks, extractions, strict=True):
                await session.execute_write(self._upsert_one, chunk, extraction)

    @staticmethod
    async def _upsert_one(
        tx: AsyncManagedTransaction, chunk: ChunkRecord, extraction: GraphExtraction
    ) -> None:
        query = """
        MERGE (d:Document {tenant_id: $tenant_id, document_id: $document_id})
        MERGE (c:Chunk {tenant_id: $tenant_id, chunk_id: $chunk_id})
        SET c.version_id = $version_id, c.source_chunk_id = $chunk_id,
            c.extractor_version = $extractor_version, c.valid_until = $valid_until
        MERGE (d)-[:HAS_CHUNK {tenant_id: $tenant_id}]->(c)
        WITH c
        UNWIND $entities AS entity
        MERGE (e:Entity {tenant_id: $tenant_id, entity_id: entity.entity_id})
        SET e.name = entity.name, e.normalized_name = entity.normalized_name,
            e.entity_type = entity.entity_type, e.aliases = entity.aliases
        MERGE (c)-[m:MENTIONS {tenant_id: $tenant_id, source_chunk_id: $chunk_id}]->(e)
        SET m.confidence = entity.confidence, m.extractor_version = $extractor_version
        """
        await tx.run(
            query,
            tenant_id=chunk.tenant_id,
            document_id=chunk.document_id,
            chunk_id=chunk.chunk_id,
            version_id=chunk.version_id,
            extractor_version=extraction.extractor_version,
            valid_until=chunk.valid_until,
            entities=[item.model_dump(mode="json") for item in extraction.entities],
        )
        id_to_entity = {item.entity_id: item for item in extraction.entities}
        for relation in extraction.relations:
            if (
                relation.source_entity_id not in id_to_entity
                or relation.target_entity_id not in id_to_entity
            ):
                continue
            await tx.run(
                """
                MATCH (a:Entity {tenant_id: $tenant_id, entity_id: $source_id})
                MATCH (b:Entity {tenant_id: $tenant_id, entity_id: $target_id})
                MERGE (a)-[r:RELATED_TO {tenant_id: $tenant_id, source_chunk_id: $chunk_id,
                    predicate: $predicate}]->(b)
                SET r.confidence = $confidence, r.extractor_version = $extractor_version
                """,
                tenant_id=chunk.tenant_id,
                source_id=relation.source_entity_id,
                target_id=relation.target_entity_id,
                chunk_id=chunk.chunk_id,
                predicate=relation.predicate,
                confidence=relation.confidence,
                extractor_version=extraction.extractor_version,
            )

    async def search(
        self, tenant_id: str, entities: Sequence[str], *, max_hops: int, top_k: int
    ) -> list[SearchCandidate]:
        if not 1 <= max_hops <= 2:
            raise ValueError("graph max_hops must be one or two")
        query = f"""
        MATCH (e:Entity) WHERE e.tenant_id = $tenant_id
          AND (e.normalized_name IN $entities
            OR any(alias IN e.aliases WHERE toLower(alias) IN $entities))
        MATCH path=(e)-[:RELATED_TO*0..{max_hops}]-(related:Entity)<-[:MENTIONS]-(c:Chunk)
        WHERE related.tenant_id = $tenant_id AND c.tenant_id = $tenant_id
        RETURN DISTINCT c.chunk_id AS chunk_id, length(path) AS hops,
          [node IN nodes(path) | coalesce(node.normalized_name, node.chunk_id)] AS path_names
        ORDER BY hops ASC, chunk_id ASC LIMIT $top_k
        """
        async with self.driver.session(database=self.database) as session:
            result = await session.run(
                query,
                tenant_id=tenant_id,
                entities=[item.casefold() for item in entities],
                top_k=top_k,
            )
            rows = [record async for record in result]
        return [
            SearchCandidate(
                chunk_id=str(row["chunk_id"]),
                score=1.0 / (int(row["hops"]) + 1),
                source="graph",
                rank=rank,
                path=tuple(str(item) for item in row["path_names"]),
            )
            for rank, row in enumerate(rows, start=1)
        ]

    async def delete_document_version(self, tenant_id: str, version_id: str) -> None:
        async with self.driver.session(database=self.database) as session:
            await session.run(
                """
                MATCH (c:Chunk {tenant_id: $tenant_id, version_id: $version_id})
                DETACH DELETE c
                WITH 1 AS ignored
                MATCH (e:Entity {tenant_id: $tenant_id})
                WHERE NOT (e)<-[:MENTIONS]-(:Chunk)
                DETACH DELETE e
                """,
                tenant_id=tenant_id,
                version_id=version_id,
            )

    async def list_chunk_ids(self, tenant_id: str) -> set[str]:
        async with self.driver.session(database=self.database) as session:
            result = await session.run(
                "MATCH (c:Chunk {tenant_id: $tenant_id}) RETURN c.chunk_id AS chunk_id",
                tenant_id=tenant_id,
            )
            return {str(row["chunk_id"]) async for row in result}

    async def close(self) -> None:
        await self.driver.close()
