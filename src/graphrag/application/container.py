"""Application composition root for Fake and production adapters."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from graphrag.agents.orchestrator import AgentOrchestrator, default_faqs
from graphrag.agents.router import DeterministicRouter
from graphrag.application.context import (
    ContextAssembler,
    ContextPolicy,
    ConversationMemoryManager,
    HeuristicTokenEstimator,
)
from graphrag.application.knowledge_quality import KnowledgeQualityService
from graphrag.application.memory_governance import LongTermMemoryService
from graphrag.application.outbox import OutboxRelay
from graphrag.application.prompts import PromptRegistry
from graphrag.application.safety import RuleBasedSafety
from graphrag.application.sessions import InMemorySessionService
from graphrag.config import (
    AppEnvironment,
    BusinessAdapterMode,
    ObjectStoreBackend,
    Settings,
)
from graphrag.domain.ports import (
    AuditPort,
    BusinessServicesPort,
    ChatModelPort,
    CheckpointStorePort,
    EmbeddingPort,
    EntityExtractorPort,
    EventPublisherPort,
    GraphStorePort,
    KnowledgeRepositoryPort,
    LongTermMemoryPort,
    ObjectStorePort,
    OCRPort,
    RerankerPort,
    RouterPort,
    SafeResumeStorePort,
    SafetyPort,
    TracePort,
    VectorStorePort,
)
from graphrag.infrastructure.business_http_adapter import SyntheticBusinessHTTPAdapter
from graphrag.infrastructure.database import Base, Database, SQLKnowledgeRepository
from graphrag.infrastructure.fakes import FakeBusinessServices, FakeModelProvider, FakeOCR
from graphrag.infrastructure.kafka_adapter import KafkaEventPublisher
from graphrag.infrastructure.memory import (
    InMemoryAudit,
    InMemoryCheckpointStore,
    InMemoryEventPublisher,
    InMemoryGraphStore,
    InMemoryKnowledgeRepository,
    InMemoryLongTermMemoryStore,
    InMemoryRateLimiter,
    InMemorySafeResumeStore,
    InMemoryVectorStore,
)
from graphrag.infrastructure.milvus_adapter import MilvusVectorStore
from graphrag.infrastructure.model_adapter import (
    BailianOCR,
    BailianReranker,
    OpenAICompatibleProvider,
)
from graphrag.infrastructure.neo4j_adapter import Neo4jGraphStore
from graphrag.infrastructure.object_store import LocalObjectStore, S3ObjectStore
from graphrag.infrastructure.outbox import SQLOutboxStore
from graphrag.infrastructure.redis_adapter import RedisAdapter
from graphrag.infrastructure.sql_services import (
    SQLAudit,
    SQLLongTermMemoryStore,
    SQLSafeResumeStore,
    SQLSessionService,
)
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
    safe_resume: SafeResumeStorePort
    memories: LongTermMemoryService
    knowledge_quality: KnowledgeQualityService
    safety: SafetyPort
    business: BusinessServicesPort
    object_store: ObjectStorePort
    ingestion: IngestionCoordinator
    worker: IngestionWorker
    retrieval: GraphRAGPipeline
    orchestrator: AgentOrchestrator
    tools: ToolRegistry
    database: Database | None = None
    redis: RedisAdapter | None = None
    kafka: KafkaEventPublisher | None = None
    outbox_relay: OutboxRelay | None = None
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
        if self.outbox_relay is not None:
            await self.outbox_relay.start()

    async def close(self) -> None:
        if self.outbox_relay is not None:
            await self.outbox_relay.stop()
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
            fake_checks = {
                name: {"ready": True, "mode": "fake"}
                for name in ("mysql", "redis", "milvus", "neo4j", "model")
            }
            fake_checks["object_store"] = await self._object_store_health()
            fake_checks["business"] = await self._business_health()
            return fake_checks
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
        if self.settings.kafka_enabled:
            checks["kafka"] = await self._health_call(
                self.kafka.health if self.kafka is not None else None
            )
        checks["object_store"] = await self._object_store_health()
        checks["business"] = await self._business_health()
        return checks

    async def _object_store_health(self) -> dict[str, Any]:
        health = getattr(self.object_store, "health", None)
        if health is None:
            return {"ready": True, "mode": "local"}
        return await self._health_call(health)

    async def _business_health(self) -> dict[str, Any]:
        health = getattr(self.business, "health", None)
        if health is None:
            return {"ready": True, "mode": "fake"}
        return await self._health_call(health)

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
    kafka: KafkaEventPublisher | None = None
    outbox_relay: OutboxRelay | None = None
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

    prompt_registry = PromptRegistry()

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
        safe_resume: SafeResumeStorePort = InMemorySafeResumeStore()
        memory_store: LongTermMemoryPort = InMemoryLongTermMemoryStore()
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
        sessions = SQLSessionService(
            database,
            model_version=settings.chat_model,
            prompt_version=prompt_registry.bundle_version,
        )
        safe_resume = SQLSafeResumeStore(database)
        memory_store = SQLLongTermMemoryStore(database)
        ocr = BailianOCR(
            api_key=api_key,
            base_url=str(settings.llm_base_url),
            model=settings.ocr_model,
        )
        closeables.extend([rerank_client, ocr, model, vector_store, graph_store, redis, database])

    if settings.kafka_enabled:
        producer_config: dict[str, object] = {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": settings.kafka_client_id,
            "security.protocol": settings.kafka_security_protocol.value,
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
            "max.in.flight.requests.per.connection": 5,
        }
        if settings.kafka_sasl_username and settings.kafka_sasl_password:
            producer_config.update(
                {
                    "sasl.mechanism": settings.kafka_sasl_mechanism,
                    "sasl.username": settings.kafka_sasl_username,
                    "sasl.password": settings.kafka_sasl_password.get_secret_value(),
                }
            )
        kafka = KafkaEventPublisher(
            producer_config,
            knowledge_topic=settings.kafka_knowledge_topic,
            action_topic=settings.kafka_action_topic,
            publish_timeout_seconds=settings.kafka_publish_timeout_seconds,
        )
        closeables.append(kafka)
        if settings.outbox_relay_enabled:
            if database is None:
                raise RuntimeError("Outbox Relay requires a SQL database")
            outbox_relay = OutboxRelay(
                store=SQLOutboxStore(database),
                publisher=kafka,
                batch_size=settings.outbox_batch_size,
                lease_seconds=settings.outbox_lease_seconds,
                poll_interval_seconds=settings.outbox_poll_interval_seconds,
                retry_base_seconds=settings.outbox_retry_base_seconds,
                retry_max_seconds=settings.outbox_retry_max_seconds,
            )

    object_store: ObjectStorePort
    if settings.object_store_backend is ObjectStoreBackend.S3:
        object_store = S3ObjectStore(
            bucket=settings.s3_bucket,
            region=settings.s3_region,
            endpoint_url=str(settings.s3_endpoint_url) if settings.s3_endpoint_url else None,
            access_key_id=settings.s3_access_key_id,
            secret_access_key=(
                settings.s3_secret_access_key.get_secret_value()
                if settings.s3_secret_access_key
                else None
            ),
            session_token=(
                settings.s3_session_token.get_secret_value() if settings.s3_session_token else None
            ),
            force_path_style=settings.s3_force_path_style,
            verify_tls=settings.s3_verify_tls,
            sse_algorithm=settings.s3_sse_algorithm,
            kms_key_id=settings.s3_kms_key_id,
        )
        closeables.append(object_store)
    else:
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
    token_estimator = HeuristicTokenEstimator()
    context_assembler = ContextAssembler(
        policy=ContextPolicy(
            version=settings.context_policy_version,
            model_context_window_tokens=settings.model_context_window_tokens,
            reserved_output_tokens=settings.context_reserved_output_tokens,
            recent_history_ratio=settings.context_recent_history_ratio,
            working_memory_ratio=settings.context_working_memory_ratio,
            external_evidence_ratio=settings.context_external_evidence_ratio,
            fixed_context_ratio=settings.context_fixed_ratio,
        ),
        estimator=token_estimator,
    )
    memory_manager = ConversationMemoryManager(
        estimator=token_estimator,
        summary_trigger_tokens=settings.context_summary_trigger_tokens,
        summary_max_tokens=settings.context_summary_max_tokens,
    )
    safety: SafetyPort = RuleBasedSafety()
    memories = LongTermMemoryService(
        store=memory_store,
        audit=audit,
        auto_write_enabled=settings.long_term_memory_auto_write_enabled,
    )
    knowledge_quality = KnowledgeQualityService(repository=repository)
    router: RouterPort = DeterministicRouter(
        threshold=settings.router_confidence_threshold,
        max_experts=2,
    )
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
        context_assembler=context_assembler,
        prompt_registry=prompt_registry,
    )
    business: BusinessServicesPort
    if settings.business_adapter_mode is BusinessAdapterMode.SYNTHETIC_HTTP:
        if settings.synthetic_business_base_url is None:
            raise RuntimeError("Synthetic business base URL is required")
        business = SyntheticBusinessHTTPAdapter(
            base_url=str(settings.synthetic_business_base_url),
            repository=repository,
            timeout_seconds=settings.business_timeout_seconds,
            read_max_attempts=settings.business_read_max_attempts,
        )
        closeables.append(business)
    else:
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
        context_assembler=context_assembler,
        memory_manager=memory_manager,
        safe_resume=safe_resume,
        prompt_registry=prompt_registry,
        router=router,
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
        safe_resume=safe_resume,
        memories=memories,
        knowledge_quality=knowledge_quality,
        safety=safety,
        business=business,
        object_store=object_store,
        ingestion=ingestion,
        worker=worker,
        retrieval=retrieval,
        orchestrator=orchestrator,
        tools=tools,
        database=database,
        redis=redis,
        kafka=kafka,
        outbox_relay=outbox_relay,
        closeables=closeables,
        langgraph_checkpointer=langgraph_checkpointer,
    )
