"""Validated runtime configuration with safe environment boundaries."""

from __future__ import annotations

from enum import StrEnum
from functools import lru_cache
from pathlib import Path

from pydantic import AnyHttpUrl, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppEnvironment(StrEnum):
    DEV = "dev"
    TEST = "test"
    STAGING = "staging"
    PROD = "prod"


class KafkaSecurityProtocol(StrEnum):
    PLAINTEXT = "PLAINTEXT"
    SSL = "SSL"
    SASL_PLAINTEXT = "SASL_PLAINTEXT"
    SASL_SSL = "SASL_SSL"


class ObjectStoreBackend(StrEnum):
    LOCAL = "local"
    S3 = "s3"


class BusinessAdapterMode(StrEnum):
    FAKE = "fake"
    SYNTHETIC_HTTP = "synthetic_http"


class Settings(BaseSettings):
    """All configurable behaviour. No model, threshold or endpoint is hard-coded elsewhere."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_env: AppEnvironment = AppEnvironment.DEV
    app_name: str = "Neo4j GraphRAG Agent"
    log_level: str = "INFO"
    default_tenant_id: str = "default"
    use_fake_external_clients: bool = True

    jwt_algorithm: str = "HS256"
    jwt_dev_secret: SecretStr | None = None
    jwt_issuer: str = "neo4j-graphrag-local"
    jwt_audience: str = "neo4j-graphrag-api"
    jwt_jwks_url: AnyHttpUrl | None = None

    database_url: str = "sqlite+aiosqlite:///./data/dev.db"
    database_pool_size: int = Field(default=10, ge=1, le=100)
    redis_url: str = "redis://127.0.0.1:6379/0"
    milvus_uri: str = "http://127.0.0.1:19530"
    milvus_token: SecretStr | None = None
    milvus_collection: str = "weview_content_chunks_v1"
    neo4j_uri: str = "bolt://127.0.0.1:7687"
    neo4j_username: str = "neo4j"
    neo4j_password: SecretStr | None = None
    neo4j_database: str = "neo4j"

    kafka_enabled: bool = False
    outbox_relay_enabled: bool = False
    kafka_bootstrap_servers: str = ""
    kafka_client_id: str = "neo4j-graphrag-outbox"
    kafka_security_protocol: KafkaSecurityProtocol = KafkaSecurityProtocol.PLAINTEXT
    kafka_sasl_mechanism: str = "PLAIN"
    kafka_sasl_username: str | None = None
    kafka_sasl_password: SecretStr | None = None
    kafka_knowledge_topic: str = "knowledge.events.v1"
    kafka_action_topic: str = "action.events.v1"
    kafka_publish_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    outbox_batch_size: int = Field(default=100, ge=1, le=1000)
    outbox_lease_seconds: int = Field(default=30, ge=1, le=600)
    outbox_poll_interval_seconds: float = Field(default=0.5, gt=0, le=60)
    outbox_retry_base_seconds: float = Field(default=1.0, gt=0, le=300)
    outbox_retry_max_seconds: float = Field(default=300.0, gt=0, le=3600)
    kafka_max_event_bytes: int = Field(default=1024 * 1024, ge=1024, le=10 * 1024 * 1024)
    kafka_vector_consumer_group: str = "graphrag-vector-index-v1"
    kafka_graph_consumer_group: str = "graphrag-graph-index-v1"
    kafka_metadata_timeout_seconds: float = Field(default=5.0, gt=0, le=120)
    kafka_consumer_poll_timeout_seconds: float = Field(default=1.0, gt=0, le=60)
    kafka_consumer_retry_pause_seconds: float = Field(default=1.0, gt=0, le=300)
    inbox_lease_seconds: int = Field(default=180, ge=1, le=3600)
    consumer_handler_timeout_seconds: float = Field(default=120.0, gt=0, le=1800)
    consumer_max_attempts: int = Field(default=5, ge=1, le=100)
    consumer_retry_base_seconds: float = Field(default=1.0, gt=0, le=300)
    consumer_retry_max_seconds: float = Field(default=300.0, gt=0, le=3600)

    llm_base_url: AnyHttpUrl = AnyHttpUrl("https://dashscope.aliyuncs.com/compatible-mode/v1")
    llm_api_key: SecretStr | None = None
    router_model: str = "qwen-turbo"
    chat_model: str = "qwen-plus"
    embedding_model: str = "text-embedding-v4"
    embedding_dimension: int = Field(default=1024, ge=1, le=65536)
    embedding_version: str = "v1"
    rerank_model: str = "qwen3-rerank"
    rerank_endpoint: AnyHttpUrl = AnyHttpUrl(
        "https://dashscope.aliyuncs.com/api/v1/services/rerank/text-rerank/text-rerank"
    )
    rerank_enabled: bool = True
    ocr_model: str = "qwen-vl-ocr-2025-11-20"

    rag_retrieval_top_k: int = Field(default=20, ge=1, le=200)
    rag_final_top_k: int = Field(default=5, ge=1, le=50)
    rag_rrf_k: int = Field(default=60, ge=1, le=1000)
    rag_graph_max_hops: int = Field(default=2, ge=1, le=2)
    rag_min_rerank_score: float = Field(default=0.08, ge=0.0, le=1.0)
    rag_rerank_relative_threshold: float = Field(default=0.80, ge=0.0, le=1.0)
    faq_similarity_threshold: float = Field(default=0.88, ge=0.0, le=1.0)
    router_confidence_threshold: float = Field(default=0.75, ge=0.0, le=1.0)
    chunk_target_chars: int = Field(default=450, ge=50, le=10000)
    chunk_max_chars: int = Field(default=600, ge=100, le=20000)
    chunk_overlap_ratio: float = Field(default=0.12, ge=0.0, lt=0.5)
    upload_dir: Path = Path("./data/uploads")
    upload_max_bytes: int = Field(default=25 * 1024 * 1024, ge=1)
    upload_max_pages: int = Field(default=200, ge=1, le=5000)
    allowed_upload_types: str = ".pdf,.docx,.xlsx,.pptx,.html,.htm,.txt,.md,.png,.jpg,.jpeg"
    ingestion_concurrency: int = Field(default=2, ge=1, le=32)
    embedding_batch_size: int = Field(default=16, ge=1, le=128)

    object_store_backend: ObjectStoreBackend = ObjectStoreBackend.LOCAL
    s3_endpoint_url: AnyHttpUrl | None = None
    s3_bucket: str = ""
    s3_region: str = "us-east-1"
    s3_access_key_id: str | None = None
    s3_secret_access_key: SecretStr | None = None
    s3_session_token: SecretStr | None = None
    s3_force_path_style: bool = False
    s3_verify_tls: bool = True
    s3_sse_algorithm: str | None = None
    s3_kms_key_id: str | None = None

    business_adapter_mode: BusinessAdapterMode = BusinessAdapterMode.FAKE
    synthetic_business_base_url: AnyHttpUrl | None = None
    business_timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    business_read_max_attempts: int = Field(default=2, ge=1, le=3)

    dependency_timeout_seconds: float = Field(default=5.0, gt=0, le=120)
    router_timeout_seconds: float = Field(default=8.0, gt=0, le=120)
    generation_timeout_seconds: float = Field(default=60.0, gt=0, le=600)
    embedding_timeout_seconds: float = Field(default=30.0, gt=0, le=300)
    rerank_timeout_seconds: float = Field(default=15.0, gt=0, le=300)
    ocr_page_timeout_seconds: float = Field(default=60.0, gt=0, le=600)
    tool_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    checkpoint_ttl_seconds: int = Field(default=86400, ge=60)
    context_policy_version: str = "context-policy-v1"
    model_context_window_tokens: int = Field(default=32768, ge=1024, le=2_000_000)
    context_reserved_output_tokens: int = Field(default=4096, ge=128, le=500_000)
    context_recent_history_ratio: float = Field(default=0.30, ge=0.0, le=1.0)
    context_working_memory_ratio: float = Field(default=0.20, ge=0.0, le=1.0)
    context_external_evidence_ratio: float = Field(default=0.40, ge=0.0, le=1.0)
    context_fixed_ratio: float = Field(default=0.10, ge=0.0, le=1.0)
    context_summary_trigger_tokens: int = Field(default=6000, ge=128)
    context_summary_max_tokens: int = Field(default=1500, ge=64)
    long_term_memory_auto_write_enabled: bool = False
    chat_rate_limit_per_minute: int = Field(default=30, ge=1, le=10000)
    upload_rate_limit_per_minute: int = Field(default=10, ge=1, le=10000)
    fake_approval_secret: SecretStr | None = None

    langfuse_enabled: bool = False
    langfuse_public_key: SecretStr | None = None
    langfuse_secret_key: SecretStr | None = None
    langfuse_host: AnyHttpUrl = AnyHttpUrl("https://cloud.langfuse.com")
    otel_exporter_otlp_endpoint: AnyHttpUrl | None = None

    @field_validator("default_tenant_id")
    @classmethod
    def validate_tenant(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned or not cleaned.replace("-", "").replace("_", "").isalnum():
            msg = "DEFAULT_TENANT_ID must be a non-empty safe identifier"
            raise ValueError(msg)
        return cleaned

    @model_validator(mode="after")
    def validate_boundaries(self) -> Settings:
        if self.chunk_target_chars > self.chunk_max_chars:
            msg = "CHUNK_TARGET_CHARS must not exceed CHUNK_MAX_CHARS"
            raise ValueError(msg)
        if self.rag_final_top_k > self.rag_retrieval_top_k:
            msg = "RAG_FINAL_TOP_K must not exceed RAG_RETRIEVAL_TOP_K"
            raise ValueError(msg)
        if self.context_reserved_output_tokens >= self.model_context_window_tokens:
            msg = "CONTEXT_RESERVED_OUTPUT_TOKENS must be smaller than the model context window"
            raise ValueError(msg)
        context_ratio = (
            self.context_recent_history_ratio
            + self.context_working_memory_ratio
            + self.context_external_evidence_ratio
            + self.context_fixed_ratio
        )
        if abs(context_ratio - 1.0) > 1e-9:
            msg = "context budget ratios must sum to 1.0"
            raise ValueError(msg)
        if self.long_term_memory_auto_write_enabled:
            msg = "phase 1.5 forbids automatic long-term memory writes"
            raise ValueError(msg)
        if self.outbox_relay_enabled and not self.kafka_enabled:
            msg = "OUTBOX_RELAY_ENABLED requires KAFKA_ENABLED"
            raise ValueError(msg)
        if self.outbox_relay_enabled and self.use_fake_external_clients:
            msg = "Outbox Relay requires the SQL-backed real adapter mode"
            raise ValueError(msg)
        if self.kafka_enabled and not self.kafka_bootstrap_servers.strip():
            msg = "KAFKA_BOOTSTRAP_SERVERS is required when Kafka is enabled"
            raise ValueError(msg)
        if self.outbox_lease_seconds <= self.kafka_publish_timeout_seconds:
            msg = "OUTBOX_LEASE_SECONDS must exceed KAFKA_PUBLISH_TIMEOUT_SECONDS"
            raise ValueError(msg)
        if self.outbox_retry_max_seconds < self.outbox_retry_base_seconds:
            msg = "OUTBOX_RETRY_MAX_SECONDS must not be smaller than the base delay"
            raise ValueError(msg)
        if self.inbox_lease_seconds <= self.consumer_handler_timeout_seconds:
            msg = "INBOX_LEASE_SECONDS must exceed CONSUMER_HANDLER_TIMEOUT_SECONDS"
            raise ValueError(msg)
        if self.consumer_retry_max_seconds < self.consumer_retry_base_seconds:
            msg = "CONSUMER_RETRY_MAX_SECONDS must not be smaller than the base delay"
            raise ValueError(msg)
        if (
            self.kafka_enabled
            and self.kafka_security_protocol
            in {
                KafkaSecurityProtocol.SASL_PLAINTEXT,
                KafkaSecurityProtocol.SASL_SSL,
            }
            and (not self.kafka_sasl_username or self.kafka_sasl_password is None)
        ):
            msg = "SASL Kafka requires KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD"
            raise ValueError(msg)
        if self.object_store_backend is ObjectStoreBackend.S3:
            if not self.s3_bucket.strip():
                msg = "S3_BUCKET is required for the S3 object store"
                raise ValueError(msg)
            if bool(self.s3_access_key_id) != bool(self.s3_secret_access_key):
                msg = "S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY must be configured together"
                raise ValueError(msg)
            if self.s3_sse_algorithm not in {None, "AES256", "aws:kms"}:
                msg = "S3_SSE_ALGORITHM must be AES256 or aws:kms"
                raise ValueError(msg)
            if self.s3_sse_algorithm == "aws:kms" and not self.s3_kms_key_id:
                msg = "S3_KMS_KEY_ID is required for aws:kms encryption"
                raise ValueError(msg)
        if (
            self.business_adapter_mode is BusinessAdapterMode.SYNTHETIC_HTTP
            and self.synthetic_business_base_url is None
        ):
            msg = "SYNTHETIC_BUSINESS_BASE_URL is required for the synthetic HTTP adapter"
            raise ValueError(msg)
        if self.app_env in {AppEnvironment.STAGING, AppEnvironment.PROD}:
            if self.use_fake_external_clients:
                msg = "Fake external clients are forbidden outside dev/test"
                raise ValueError(msg)
            if self.object_store_backend is not ObjectStoreBackend.S3:
                msg = "staging/prod require the S3-compatible object store"
                raise ValueError(msg)
            if not self.s3_verify_tls:
                msg = "staging/prod forbid disabled S3 TLS verification"
                raise ValueError(msg)
            if self.business_adapter_mode in {
                BusinessAdapterMode.FAKE,
                BusinessAdapterMode.SYNTHETIC_HTTP,
            }:
                msg = "staging/prod require a real business contract adapter"
                raise ValueError(msg)
            if self.jwt_algorithm == "HS256" or self.jwt_jwks_url is None:
                msg = "staging/prod require asymmetric JWT verification through JWKS"
                raise ValueError(msg)
            if "sqlite" in self.database_url or "root" in self.database_url.lower():
                msg = "staging/prod require a non-root MySQL application account"
                raise ValueError(msg)
        if self.langfuse_enabled and (
            self.langfuse_public_key is None or self.langfuse_secret_key is None
        ):
            msg = "Langfuse keys are required when LANGFUSE_ENABLED=true"
            raise ValueError(msg)
        if self.jwt_dev_secret is not None and len(self.jwt_dev_secret.get_secret_value()) < 32:
            msg = "JWT_DEV_SECRET must contain at least 32 characters"
            raise ValueError(msg)
        if (
            self.fake_approval_secret is not None
            and len(self.fake_approval_secret.get_secret_value()) < 32
        ):
            msg = "FAKE_APPROVAL_SECRET must contain at least 32 characters"
            raise ValueError(msg)
        if not self.use_fake_external_clients and (
            self.llm_api_key is None or self.neo4j_password is None
        ):
            msg = "real adapters require LLM_API_KEY and NEO4J_PASSWORD"
            raise ValueError(msg)
        return self

    @property
    def allowed_extensions(self) -> frozenset[str]:
        return frozenset(item.strip().lower() for item in self.allowed_upload_types.split(","))


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
