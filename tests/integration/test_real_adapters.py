"""Opt-in checks against the pinned Docker Compose infrastructure."""

from __future__ import annotations

import hashlib
import os

import boto3
import pytest

from graphrag.domain.errors import NotFoundError
from graphrag.domain.ids import new_id
from graphrag.domain.models import ChunkRecord, Entity, GraphExtraction
from graphrag.domain.state import AgentState
from graphrag.infrastructure.database import Database
from graphrag.infrastructure.milvus_adapter import MilvusVectorStore
from graphrag.infrastructure.neo4j_adapter import Neo4jGraphStore
from graphrag.infrastructure.object_store import S3ObjectStore
from graphrag.infrastructure.redis_adapter import RedisAdapter

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_REAL_INTEGRATION") != "1",
    reason="set RUN_REAL_INTEGRATION=1 with the Compose dependencies running",
)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pinned_mysql_redis_milvus_and_neo4j_adapters() -> None:
    database = Database(os.environ["TEST_DATABASE_URL"])
    redis = RedisAdapter(os.environ["TEST_REDIS_URL"])
    collection = f"integration_{new_id().replace('-', '_')}"
    vector = MilvusVectorStore(
        uri=os.environ["TEST_MILVUS_URI"],
        token=None,
        collection=collection,
        dimension=4,
        embedding_version="integration-v1",
    )
    graph = Neo4jGraphStore(
        os.environ["TEST_NEO4J_URI"],
        "neo4j",
        os.environ["TEST_NEO4J_PASSWORD"],
        "neo4j",
    )
    try:
        assert await database.health()
        capabilities = await redis.verify_capabilities()
        assert str(capabilities["version"]).startswith("8.")
        state = AgentState(
            request_id=new_id(),
            run_id=new_id(),
            session_id=new_id(),
            tenant_id="integration",
            user_id="tester",
            roles=frozenset({"user"}),
            original_query="integration",
            query="integration",
        )
        await redis.put(state, ttl_seconds=60)
        assert await redis.get("integration", state.session_id) == state
        assert await redis.acquire_lock("lock:integration", "owner", lease_seconds=10)
        assert not await redis.release_lock("lock:integration", "other")
        assert await redis.release_lock("lock:integration", "owner")

        chunk = ChunkRecord(
            tenant_id="integration",
            document_id=new_id(),
            version_id=new_id(),
            ordinal=0,
            content="integration entity policy",
            content_hash=hashlib.sha256(b"integration entity policy").hexdigest(),
            embedding_model="integration",
            embedding_dimension=4,
            embedding_version="integration-v1",
        )
        await vector.ensure_schema()
        await vector.upsert([chunk], [(1.0, 0.0, 0.0, 0.0)])
        assert (await vector.search("integration", (1.0, 0.0, 0.0, 0.0), top_k=1))[
            0
        ].chunk_id == chunk.chunk_id
        assert chunk.chunk_id in await vector.list_ids("integration")

        await graph.ensure_schema()
        entity = Entity(
            name="Integration Entity",
            normalized_name="integration entity",
            entity_type="Other",
            confidence=1.0,
        )
        await graph.upsert(
            [chunk], [GraphExtraction(entities=(entity,), extractor_version="integration-v1")]
        )
        assert (await graph.search("integration", ["integration entity"], max_hops=1, top_k=1))[
            0
        ].chunk_id == chunk.chunk_id
        await graph.delete_document_version("integration", chunk.version_id)
        assert chunk.chunk_id not in await graph.list_chunk_ids("integration")
    finally:
        await graph.close()
        await vector.close()
        await redis.close()
        await database.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_isolated_s3_compatible_original_file_store() -> None:
    bucket = f"integration-{new_id()}"
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["TEST_S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["TEST_S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["TEST_S3_SECRET_ACCESS_KEY"],
        region_name="us-east-1",
    )
    client.create_bucket(Bucket=bucket)
    store = S3ObjectStore(
        bucket=bucket,
        region="us-east-1",
        endpoint_url=os.environ["TEST_S3_ENDPOINT_URL"],
        access_key_id=os.environ["TEST_S3_ACCESS_KEY_ID"],
        secret_access_key=os.environ["TEST_S3_SECRET_ACCESS_KEY"],
        session_token=None,
        force_path_style=True,
        verify_tls=True,
        sse_algorithm=None,
        kms_key_id=None,
        client=client,
    )
    try:
        key, _ = await store.save("integration", new_id(), "policy.txt", b"integration object")
        assert await store.read("integration", key) == b"integration object"
        with pytest.raises(NotFoundError):
            await store.read("other-tenant", key)
        await store.delete("integration", key)
        await store.delete("integration", key)
    finally:
        await store.close()
        cleanup = boto3.client(
            "s3",
            endpoint_url=os.environ["TEST_S3_ENDPOINT_URL"],
            aws_access_key_id=os.environ["TEST_S3_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["TEST_S3_SECRET_ACCESS_KEY"],
            region_name="us-east-1",
        )
        cleanup.delete_bucket(Bucket=bucket)
        cleanup.close()
