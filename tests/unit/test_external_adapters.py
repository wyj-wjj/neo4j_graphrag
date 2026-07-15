from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any

import pytest

from graphrag.domain.errors import ValidationError
from graphrag.domain.models import (
    ChunkRecord,
    Entity,
    GraphExtraction,
)
from graphrag.domain.state import AgentState
from graphrag.infrastructure.milvus_adapter import MilvusVectorStore
from graphrag.infrastructure.model_adapter import (
    BailianOCR,
    BailianReranker,
    OpenAICompatibleProvider,
)
from graphrag.infrastructure.neo4j_adapter import Neo4jGraphStore
from graphrag.infrastructure.redis_adapter import RedisAdapter


def chunk(chunk_id: str = "chunk-1") -> ChunkRecord:
    return ChunkRecord(
        chunk_id=chunk_id,
        tenant_id="default",
        document_id="document-1",
        version_id="version-1",
        ordinal=0,
        content="warranty policy",
        content_hash="a" * 64,
        embedding_model="fake",
        embedding_dimension=4,
        embedding_version="v1",
    )


class FakeSchema:
    def __init__(self) -> None:
        self.fields: list[dict[str, Any]] = []

    def add_field(self, name: str, _type: Any, **params: Any) -> None:
        self.fields.append({"name": name, "params": params})


class FakeIndex:
    def add_index(self, **_kwargs: Any) -> None:
        return None


class FakeMilvusClient:
    def __init__(self) -> None:
        self.exists = False
        self.schema = FakeSchema()
        self.upserts: list[dict[str, Any]] = []
        self.last_search_kwargs: dict[str, Any] = {}
        self.last_query_kwargs: dict[str, Any] = {}
        self.closed = False

    def has_collection(self, _name: str) -> bool:
        return self.exists

    def create_schema(self, **_kwargs: Any) -> FakeSchema:
        return self.schema

    def prepare_index_params(self) -> FakeIndex:
        return FakeIndex()

    def create_collection(self, **_kwargs: Any) -> None:
        self.exists = True

    def describe_collection(self, _name: str) -> dict[str, Any]:
        return {"fields": self.schema.fields}

    def upsert(self, *, collection_name: str, data: list[dict[str, Any]]) -> None:
        assert collection_name == "chunks-v1"
        self.upserts.extend(data)

    def search(self, **kwargs: Any) -> list[list[dict[str, Any]]]:
        self.last_search_kwargs = kwargs
        return [[{"entity": {"chunk_id": "chunk-1"}, "distance": 0.9}]]

    def delete(self, **_kwargs: Any) -> None:
        return None

    def query(self, **kwargs: Any) -> list[dict[str, str]]:
        self.last_query_kwargs = kwargs
        return [{"chunk_id": "chunk-1"}]

    def close(self) -> None:
        self.closed = True


def milvus_store(client: FakeMilvusClient) -> MilvusVectorStore:
    store = object.__new__(MilvusVectorStore)
    store.client = client  # type: ignore[assignment]
    store.collection = "chunks-v1"
    store.dimension = 4
    store.embedding_version = "v1"
    store._semaphore = asyncio.Semaphore(2)
    return store


@pytest.mark.asyncio
async def test_milvus_schema_crud_and_dimension_guard() -> None:
    client = FakeMilvusClient()
    store = milvus_store(client)
    await store.ensure_schema()
    await store.ensure_schema()
    await store.upsert([chunk()], [(1.0, 0.0, 0.0, 0.0)])
    result = await store.search("default", (1.0, 0.0, 0.0, 0.0), top_k=2)
    assert result[0].chunk_id == "chunk-1"
    assert await store.list_ids("default") == {"chunk-1"}
    assert client.last_search_kwargs["consistency_level"] == "Strong"
    assert client.last_query_kwargs["consistency_level"] == "Strong"
    await store.delete("default", ["chunk-1"])
    await store.delete("default", [])
    await store.close()
    assert client.closed
    with pytest.raises(ValueError):
        await store.upsert([chunk()], [(1.0,)])


class FakePipeline:
    def __init__(self) -> None:
        self.commands: list[tuple[str, tuple[Any, ...]]] = []

    def incr(self, *args: Any) -> None:
        self.commands.append(("incr", args))

    def expire(self, *args: Any, **_kwargs: Any) -> None:
        self.commands.append(("expire", args))

    async def execute(self) -> list[int]:
        return [1, 1]


class FakeRedisClient:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.closed = False

    async def info(self, _section: str) -> dict[str, str]:
        return {"redis_version": "8.2.0"}

    async def module_list(self) -> list[dict[str, str]]:
        return []

    async def set(self, key: str, value: str, **_kwargs: Any) -> bool:
        self.values[key] = value
        return True

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def delete(self, key: str) -> int:
        return int(self.values.pop(key, None) is not None)

    async def eval(self, _script: str, _keys: int, key: str, owner: str) -> int:
        if self.values.get(key) == owner:
            self.values.pop(key)
            return 1
        return 0

    def pipeline(self, **_kwargs: Any) -> FakePipeline:
        return FakePipeline()

    async def aclose(self) -> None:
        self.closed = True


def redis_adapter(client: FakeRedisClient) -> RedisAdapter:
    adapter = object.__new__(RedisAdapter)
    adapter.client = client  # type: ignore[assignment]
    return adapter


@pytest.mark.asyncio
async def test_redis_checkpoint_cache_lock_rate_and_capability() -> None:
    client = FakeRedisClient()
    adapter = redis_adapter(client)
    assert (await adapter.verify_capabilities())["version"] == "8.2.0"
    state = AgentState(
        request_id="request-1",
        run_id="run-1",
        session_id="session-1",
        tenant_id="default",
        user_id="user-1",
        roles=frozenset({"user"}),
        original_query="q",
        query="q",
    )
    await adapter.put(state, ttl_seconds=60)
    assert await adapter.get("default", "session-1") == state
    await adapter.cache_set("cache", {"v": 1}, ttl_seconds=60)
    assert await adapter.cache_get("cache") == {"v": 1}
    assert await adapter.acquire_lock("lock", "owner", lease_seconds=10)
    assert await adapter.release_lock("lock", "owner")
    assert await adapter.allow("rate", limit=1, window_seconds=60)
    await adapter.delete("default", "session-1")
    assert await adapter.get("default", "session-1") is None
    await adapter.close()
    assert client.closed


class AsyncRows:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        async def iterate() -> AsyncIterator[dict[str, Any]]:
            for row in self.rows:
                yield row

        return iterate()


class FakeNeo4jSession:
    def __init__(self) -> None:
        self.queries: list[str] = []

    async def __aenter__(self) -> FakeNeo4jSession:
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    async def run(self, query: str, **_kwargs: Any) -> AsyncRows:
        self.queries.append(query)
        if "length(path)" in query:
            return AsyncRows([{"chunk_id": "chunk-1", "hops": 1, "path_names": ["policy"]}])
        if "RETURN c.chunk_id" in query:
            return AsyncRows([{"chunk_id": "chunk-1"}])
        return AsyncRows([])

    async def execute_write(self, operation: Any, *args: Any) -> Any:
        return await operation(self, *args)


class FakeNeo4jDriver:
    def __init__(self) -> None:
        self.session_instance = FakeNeo4jSession()
        self.closed = False

    def session(self, **_kwargs: Any) -> FakeNeo4jSession:
        return self.session_instance

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_neo4j_schema_upsert_bounded_search_delete_and_list() -> None:
    driver = FakeNeo4jDriver()
    store = object.__new__(Neo4jGraphStore)
    store.driver = driver  # type: ignore[assignment]
    store.database = "neo4j"
    entity = Entity(
        entity_id="entity-1",
        name="Policy",
        normalized_name="policy",
        entity_type="Policy",
        confidence=1.0,
    )
    extraction = GraphExtraction(entities=(entity,), extractor_version="v1")
    await store.ensure_schema()
    await store.upsert([chunk()], [extraction])
    result = await store.search("default", ["policy"], max_hops=2, top_k=5)
    assert result[0].path == ("policy",)
    with pytest.raises(ValueError):
        await store.search("default", ["policy"], max_hops=3, top_k=5)
    await store.delete_document_version("default", "version-1")
    assert await store.list_chunk_ids("default") == {"chunk-1"}
    await store.close()
    assert driver.closed


class FakeChatCompletions:
    async def create(self, **kwargs: Any) -> Any:
        if kwargs.get("stream"):

            async def chunks() -> AsyncIterator[Any]:
                yield SimpleNamespace(
                    choices=[SimpleNamespace(delta=SimpleNamespace(content="part"))]
                )

            return chunks()
        return SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content='{"entities":[],"relations":[],"extractor_version":"v1"}'
                    ),
                    finish_reason="stop",
                )
            ],
            usage=SimpleNamespace(prompt_tokens=2, completion_tokens=3),
            model="fake-chat",
        )


class FakeEmbeddings:
    async def create(self, **_kwargs: Any) -> Any:
        return SimpleNamespace(
            data=[SimpleNamespace(embedding=[1.0, 0.0, 0.0, 0.0])],
            model="fake-embedding",
        )


@pytest.mark.asyncio
async def test_openai_compatible_chat_stream_structured_embedding_and_ocr() -> None:
    provider = object.__new__(OpenAICompatibleProvider)
    provider.embedding_model = "embed"
    provider.embedding_dimension = 4
    provider.embedding_version = "v1"
    provider.extractor_model = "extract"
    provider.client = SimpleNamespace(
        chat=SimpleNamespace(completions=FakeChatCompletions()),
        embeddings=FakeEmbeddings(),
        close=lambda: None,
    )
    completion = await provider.complete(
        [{"role": "user", "content": "q"}], model="chat", timeout=1
    )
    assert completion.usage.output_tokens == 3
    streamed = [
        item
        async for item in provider.stream(
            [{"role": "user", "content": "q"}], model="chat", timeout=1
        )
    ]
    assert streamed[-1].done
    structured = await provider.extract("policy", timeout=1)
    assert structured.extractor_version == "v1"
    embedded = await provider.embed(["q"], input_type="query", timeout=1)
    assert embedded.dimension == 4
    with pytest.raises(ValidationError):
        await provider.embed(["q"], input_type="bad", timeout=1)

    ocr = object.__new__(BailianOCR)
    ocr.model = "ocr"
    ocr.client = SimpleNamespace(chat=SimpleNamespace(completions=FakeChatCompletions()))
    assert await ocr.recognize(b"image", page_number=1, timeout=1)


class FakeHTTPResponse:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return {"output": {"results": [{"index": 0, "relevance_score": 0.8}]}}


class FakeHTTPClient:
    async def post(self, *_args: Any, **_kwargs: Any) -> FakeHTTPResponse:
        return FakeHTTPResponse()

    async def aclose(self) -> None:
        return None


@pytest.mark.asyncio
async def test_bailian_reranker_maps_indices() -> None:
    reranker = object.__new__(BailianReranker)
    reranker.api_key = "not-real"
    reranker.endpoint = "https://example.invalid"
    reranker.model = "r"
    reranker.client = FakeHTTPClient()  # type: ignore[assignment]
    result = await reranker.rerank("q", [("c1", "text")], timeout=1)
    assert result[0].candidate_id == "c1"
    await reranker.close()
