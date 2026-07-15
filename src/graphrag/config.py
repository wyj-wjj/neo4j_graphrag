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

    dependency_timeout_seconds: float = Field(default=5.0, gt=0, le=120)
    router_timeout_seconds: float = Field(default=8.0, gt=0, le=120)
    generation_timeout_seconds: float = Field(default=60.0, gt=0, le=600)
    embedding_timeout_seconds: float = Field(default=30.0, gt=0, le=300)
    rerank_timeout_seconds: float = Field(default=15.0, gt=0, le=300)
    ocr_page_timeout_seconds: float = Field(default=60.0, gt=0, le=600)
    tool_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    checkpoint_ttl_seconds: int = Field(default=86400, ge=60)
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
        if self.app_env in {AppEnvironment.STAGING, AppEnvironment.PROD}:
            if self.use_fake_external_clients:
                msg = "Fake external clients are forbidden outside dev/test"
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
