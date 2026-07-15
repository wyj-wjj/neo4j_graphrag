"""Application composition root for Fake and production adapters."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from graphrag.agents.orchestrator import AgentOrchestrator, default_faqs
from graphrag.application.sessions import InMemorySessionService
from graphrag.config import AppEnvironment, Settings
from graphrag.domain.ports import (
    AuditPort,
    ChatModelPort,
    CheckpointStorePort,
    EmbeddingPort,
    EntityExtractorPort,
    EventPublisherPort,
    GraphStorePort,
    KnowledgeRepositoryPort,
    OCRPort,
    RerankerPort,
    TracePort,
    VectorStorePort,
)
from graphrag.infrastructure.database import Base, Database, SQLKnowledgeRepository
from graphrag.infrastructure.fakes import FakeBusinessServices, FakeModelProvider, FakeOCR
from graphrag.infrastructure.memory import (
    InMemoryAudit,
    InMemoryCheckpointStore,
    InMemoryEventPublisher,
    InMemoryGraphStore,
    InMemoryKnowledgeRepository,
    InMemoryRateLimiter,
    InMemoryVectorStore,
)
from graphrag.infrastructure.milvus_adapter import MilvusVectorStore
from graphrag.infrastructure.model_adapter import (
    BailianOCR,
    BailianReranker,
    OpenAICompatibleProvider,
)
from graphrag.infrastructure.neo4j_adapter import Neo4jGraphStore
from graphrag.infrastructure.object_store import LocalObjectStore
from graphrag.infrastructure.redis_adapter import RedisAdapter
from graphrag.infrastructure.sql_services import SQLAudit, SQLSessionService
from graphrag.ingestion.chunker import StructureAwareChunker
from graphrag.ingestion.parsers import ParserRegistry
from graphrag.ingestion.service import IngestionCoordinator, IngestionWorker
from graphrag.ingestion.validation import UploadValidator
from graphrag.observability.tracing import LangfuseTrace, NoopTrace
from graphrag.retrieval.pipeline import GraphRAGPipeline
from graphrag.tools.executor import ToolExecutor
from graphrag.tools.registry import ToolRegistry, build_default_registry


@dataclass(slots=True)
class Runtime:
    settings: Settings
    repository: KnowledgeRepositoryPort
    vector_store: VectorStorePort
    graph_store: GraphStorePort
    checkpoint: CheckpointStorePort
    chat_model: ChatModelPort
    embedding: EmbeddingPort
    extractor: EntityExtractorPort
    reranker: RerankerPort
    publisher: EventPublisherPort
    audit: AuditPort
    rate_limiter: InMemoryRateLimiter | RedisAdapter
    trace: TracePort
    sessions: InMemorySessionService | SQLSessionService
    ingestion: IngestionCoordinator
    worker: IngestionWorker
    retrieval: GraphRAGPipeline
    orchestrator: AgentOrchestrator
    tools: ToolRegistry
    database: Database | None = None
    redis: RedisAdapter | None = None
    closeables: list[Any] = field(default_factory=list)
    approval_callbacks: set[str] = field(default_factory=set)
    langgraph_checkpointer: Any = None

    async def start(self) -> None:
        if self.database is not None and self.settings.app_env in {
            AppEnvironment.DEV,
            AppEnvironment.TEST,
        }:
            async with self.database.engine.begin() as connection:
                await connection.run_sync(Base.metadata.create_all)
        await self.vector_store.ensure_schema()
        await self.graph_store.ensure_schema()
        setup = getattr(self.langgraph_checkpointer, "asetup", None)
        if setup is not None:
            await setup()
        await self.worker.start()

    async def close(self) -> None:
        await self.worker.stop()
        exit_checkpointer = getattr(self.langgraph_checkpointer, "__aexit__", None)
        if exit_checkpointer is not None:
            await exit_checkpointer(None, None, None)
        for resource in reversed(self.closeables):
            close = getattr(resource, "close", None)
            if close is not None:
                await close()

    async def readiness(self) -> dict[str, dict[str, Any]]:
        if self.settings.use_fake_external_clients:
            return {
                name: {"ready": True, "mode": "fake"}
                for name in ("mysql", "redis", "milvus", "neo4j", "model")
            }
        checks: dict[str, dict[str, Any]] = {}
        checks["mysql"] = await self._health_call(
            self.database.health if self.database is not None else None
        )
        checks["redis"] = await self._health_call(
            self.redis.verify_capabilities if self.redis is not None else None
        )
        checks["milvus"] = await self._health_call(self.vector_store.ensure_schema)
        checks["neo4j"] = await self._health_call(self.graph_store.ensure_schema)
        checks["model"] = {
            "ready": self.settings.llm_api_key is not None,
            "mode": "configured",
        }
        return checks

    @staticmethod
    async def _health_call(operation: Any) -> dict[str, Any]:
        if operation is None:
            return {"ready": False, "error": "not_configured"}
        try:
            detail = await operation()
            return {"ready": True, "detail": detail if isinstance(detail, dict) else None}
        except Exception as exc:
            return {"ready": False, "error": type(exc).__name__}


def build_runtime(settings: Settings) -> Runtime:
    """Build without I/O; Runtime.start owns all initialization side effects."""

    database: Database | None = None
    redis: RedisAdapter | None = None
    closeables: list[Any] = []
    ocr: OCRPort
    trace: TracePort = NoopTrace()
    if settings.langfuse_enabled:
        from langfuse import Langfuse

        trace_client = Langfuse(
            public_key=settings.langfuse_public_key.get_secret_value()
            if settings.langfuse_public_key
            else None,
            secret_key=settings.langfuse_secret_key.get_secret_value()
            if settings.langfuse_secret_key
            else None,
            host=str(settings.langfuse_host),
            environment=settings.app_env.value,
        )
        trace = LangfuseTrace(trace_client)
        closeables.append(trace)

    if settings.use_fake_external_clients:
        from langgraph.checkpoint.memory import InMemorySaver

        langgraph_checkpointer: Any = InMemorySaver()
    else:
        from langgraph.checkpoint.redis.aio import AsyncRedisSaver

        langgraph_checkpointer = AsyncRedisSaver(
            redis_url=settings.redis_url,
            ttl={
                "default_ttl": settings.checkpoint_ttl_seconds / 60,
                "refresh_on_read": True,
            },
            checkpoint_prefix="agent_checkpoint",
            checkpoint_write_prefix="agent_checkpoint_write",
        )

    if settings.use_fake_external_clients:
        repository: KnowledgeRepositoryPort = InMemoryKnowledgeRepository()
        vector_store: VectorStorePort = InMemoryVectorStore(
            dimension=settings.embedding_dimension, version=settings.embedding_version
        )
        graph_store: GraphStorePort = InMemoryGraphStore()
        checkpoint: CheckpointStorePort = InMemoryCheckpointStore()
        provider = FakeModelProvider(
            dimension=settings.embedding_dimension, version=settings.embedding_version
        )
        chat_model: ChatModelPort = provider
        embedding: EmbeddingPort = provider
        extractor: EntityExtractorPort = provider
        reranker: RerankerPort = provider
        publisher: EventPublisherPort = InMemoryEventPublisher()
        audit: AuditPort = InMemoryAudit()
        rate_limiter: InMemoryRateLimiter | RedisAdapter = InMemoryRateLimiter()
        sessions: InMemorySessionService | SQLSessionService = InMemorySessionService()
        ocr = FakeOCR()
    else:
        database = Database(settings.database_url, pool_size=settings.database_pool_size)
        repository = SQLKnowledgeRepository(database)
        vector_store = MilvusVectorStore(
            uri=settings.milvus_uri,
            token=settings.milvus_token.get_secret_value() if settings.milvus_token else None,
            collection=settings.milvus_collection,
            dimension=settings.embedding_dimension,
            embedding_version=settings.embedding_version,
        )
        graph_store = Neo4jGraphStore(
            settings.neo4j_uri,
            settings.neo4j_username,
            settings.neo4j_password.get_secret_value() if settings.neo4j_password else "",
            settings.neo4j_database,
        )
        redis = RedisAdapter(settings.redis_url)
        checkpoint = redis
        api_key = settings.llm_api_key.get_secret_value() if settings.llm_api_key else ""
        model = OpenAICompatibleProvider(
            api_key=api_key,
            base_url=str(settings.llm_base_url),
            embedding_model=settings.embedding_model,
            embedding_dimension=settings.embedding_dimension,
            embedding_version=settings.embedding_version,
            extractor_model=settings.router_model,
        )
        chat_model = model
        embedding = model
        extractor = model
        rerank_client = BailianReranker(
            api_key=api_key,
            endpoint=str(settings.rerank_endpoint),
            model=settings.rerank_model,
        )
        reranker = rerank_client
        publisher = InMemoryEventPublisher()
        audit = SQLAudit(database, default_tenant_id=settings.default_tenant_id)
        rate_limiter = redis
        sessions = SQLSessionService(database, model_version=settings.chat_model)
        ocr = BailianOCR(
            api_key=api_key,
            base_url=str(settings.llm_base_url),
            model=settings.ocr_model,
        )
        closeables.extend([rerank_client, ocr, model, vector_store, graph_store, redis, database])

    object_store = LocalObjectStore(settings.upload_dir)
    parsers = ParserRegistry(
        max_pages=settings.upload_max_pages,
        ocr=ocr,
        ocr_timeout=settings.ocr_page_timeout_seconds,
    )
    ingestion = IngestionCoordinator(
        settings=settings,
        repository=repository,
        object_store=object_store,
        vector_store=vector_store,
        graph_store=graph_store,
        embedding=embedding,
        extractor=extractor,
        publisher=publisher,
        validator=UploadValidator(
            allowed_extensions=settings.allowed_extensions,
            max_bytes=settings.upload_max_bytes,
        ),
        parsers=parsers,
        chunker=StructureAwareChunker(
            target_chars=settings.chunk_target_chars,
            max_chars=settings.chunk_max_chars,
            overlap_ratio=settings.chunk_overlap_ratio,
        ),
    )
    worker = IngestionWorker(ingestion, concurrency=settings.ingestion_concurrency)
    retrieval = GraphRAGPipeline(
        settings=settings,
        repository=repository,
        vector_store=vector_store,
        graph_store=graph_store,
        embedding=embedding,
        extractor=extractor,
        reranker=reranker,
        chat_model=chat_model,
        trace=trace,
    )
    business = FakeBusinessServices(repository)
    tools = build_default_registry(settings.tool_timeout_seconds)
    orchestrator = AgentOrchestrator(
        settings=settings,
        repository=repository,
        checkpoint=checkpoint,
        retrieval=retrieval,
        business=business,
        faqs=default_faqs(settings.default_tenant_id),
        trace=trace,
        graph_checkpointer=langgraph_checkpointer,
        tool_executor=ToolExecutor(registry=tools, audit=audit, trace=trace),
        audit=audit,
    )
    return Runtime(
        settings=settings,
        repository=repository,
        vector_store=vector_store,
        graph_store=graph_store,
        checkpoint=checkpoint,
        chat_model=chat_model,
        embedding=embedding,
        extractor=extractor,
        reranker=reranker,
        publisher=publisher,
        audit=audit,
        rate_limiter=rate_limiter,
        trace=trace,
        sessions=sessions,
        ingestion=ingestion,
        worker=worker,
        retrieval=retrieval,
        orchestrator=orchestrator,
        tools=tools,
        database=database,
        redis=redis,
        closeables=closeables,
        langgraph_checkpointer=langgraph_checkpointer,
    )
