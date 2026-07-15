"""Infrastructure-agnostic asynchronous Ports."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from typing import Any, Protocol, TypeVar, runtime_checkable

from pydantic import BaseModel

from graphrag.domain.events import EventEnvelope
from graphrag.domain.models import (
    ActionDraft,
    AgentExecutionPlan,
    AgentOutcome,
    ChatCompletion,
    ChatDelta,
    ChunkRecord,
    ConsolidatedOutcome,
    DocumentRecord,
    DocumentVersion,
    EmbeddingResult,
    GraphExtraction,
    IdentityContext,
    IngestionTask,
    LogisticsInfo,
    LongTermMemory,
    MemoryCategory,
    MemorySettings,
    OrderInfo,
    ParsedDocument,
    RefundQuote,
    RerankItem,
    RouteDecision,
    SafeResumeSnapshot,
    SafetyAssessment,
    SearchCandidate,
    TrustDomain,
)
from graphrag.domain.state import AgentState

StructuredT = TypeVar("StructuredT", bound=BaseModel)


@runtime_checkable
class KnowledgeRepositoryPort(Protocol):
    async def create_document(
        self,
        document: DocumentRecord,
        version: DocumentVersion,
        task: IngestionTask,
        event: EventEnvelope,
    ) -> None: ...

    async def create_version(
        self, version: DocumentVersion, task: IngestionTask, event: EventEnvelope
    ) -> None: ...

    async def list_documents(
        self, tenant_id: str, *, offset: int, limit: int
    ) -> list[DocumentRecord]: ...

    async def list_versions(self, tenant_id: str, document_id: str) -> list[DocumentVersion]: ...

    async def activate_version(self, tenant_id: str, document_id: str, version_id: str) -> None: ...

    async def inactivate_document(
        self, tenant_id: str, document_id: str, event: EventEnvelope
    ) -> None: ...

    async def save_chunks(self, chunks: Sequence[ChunkRecord], event: EventEnvelope) -> None: ...

    async def update_task(self, task: IngestionTask) -> None: ...

    async def get_task(self, tenant_id: str, task_id: str) -> IngestionTask | None: ...

    async def retry_task(self, tenant_id: str, task_id: str) -> IngestionTask: ...

    async def get_version(self, tenant_id: str, version_id: str) -> DocumentVersion | None: ...

    async def get_document(self, tenant_id: str, document_id: str) -> DocumentRecord | None: ...

    async def claim_pending_tasks(self, *, limit: int) -> list[IngestionTask]: ...

    async def list_chunks_for_version(
        self, tenant_id: str, version_id: str
    ) -> list[ChunkRecord]: ...

    async def authorize_chunks(
        self,
        identity: IdentityContext,
        chunk_ids: Sequence[str],
    ) -> list[ChunkRecord]: ...

    async def keyword_search(
        self, identity: IdentityContext, query: str, *, top_k: int
    ) -> list[SearchCandidate]: ...

    async def set_index_status(
        self,
        tenant_id: str,
        chunk_ids: Sequence[str],
        *,
        index: str,
        succeeded: bool,
        error: str | None = None,
    ) -> None: ...

    async def save_draft(self, draft: ActionDraft) -> ActionDraft: ...

    async def get_draft(self, tenant_id: str, draft_id: str) -> ActionDraft | None: ...


@runtime_checkable
class VectorStorePort(Protocol):
    async def ensure_schema(self) -> None: ...

    async def upsert(
        self, chunks: Sequence[ChunkRecord], vectors: Sequence[Sequence[float]]
    ) -> None: ...

    async def search(
        self, tenant_id: str, vector: Sequence[float], *, top_k: int
    ) -> list[SearchCandidate]: ...

    async def delete(self, tenant_id: str, chunk_ids: Sequence[str]) -> None: ...

    async def list_ids(self, tenant_id: str) -> set[str]: ...


@runtime_checkable
class GraphStorePort(Protocol):
    async def ensure_schema(self) -> None: ...

    async def upsert(
        self, chunks: Sequence[ChunkRecord], extractions: Sequence[GraphExtraction]
    ) -> None: ...

    async def search(
        self, tenant_id: str, entities: Sequence[str], *, max_hops: int, top_k: int
    ) -> list[SearchCandidate]: ...

    async def delete_document_version(self, tenant_id: str, version_id: str) -> None: ...

    async def list_chunk_ids(self, tenant_id: str) -> set[str]: ...


@runtime_checkable
class CheckpointStorePort(Protocol):
    async def put(self, state: AgentState, *, ttl_seconds: int) -> None: ...

    async def get(self, tenant_id: str, session_id: str) -> AgentState | None: ...

    async def delete(self, tenant_id: str, session_id: str) -> None: ...


@runtime_checkable
class RouterPort(Protocol):
    @property
    def version(self) -> str: ...

    def route(self, query: str) -> RouteDecision: ...

    def plan(self, query: str) -> AgentExecutionPlan: ...


@runtime_checkable
class ResultConsolidatorPort(Protocol):
    def consolidate(self, outcomes: Sequence[AgentOutcome]) -> ConsolidatedOutcome: ...


@runtime_checkable
class SafeResumeStorePort(Protocol):
    async def save(self, snapshot: SafeResumeSnapshot) -> SafeResumeSnapshot: ...

    async def get_by_draft(self, tenant_id: str, draft_id: str) -> SafeResumeSnapshot | None: ...

    async def mark_resumed(
        self,
        tenant_id: str,
        snapshot_id: str,
        *,
        decision: str,
        result_hash: str,
        final_answer: str,
        recovery_source: str,
    ) -> SafeResumeSnapshot: ...


@runtime_checkable
class LongTermMemoryPort(Protocol):
    async def settings(self, identity: IdentityContext) -> MemorySettings: ...

    async def set_enabled(self, identity: IdentityContext, *, enabled: bool) -> MemorySettings: ...

    async def create_confirmed(
        self,
        identity: IdentityContext,
        *,
        category: MemoryCategory,
        key: str,
        value: str,
        source_turn_id: str,
        expires_at: datetime | None,
    ) -> LongTermMemory: ...

    async def list_active(self, identity: IdentityContext) -> list[LongTermMemory]: ...

    async def correct(
        self,
        identity: IdentityContext,
        memory_id: str,
        *,
        value: str,
        source_turn_id: str,
        expires_at: datetime | None,
    ) -> LongTermMemory: ...

    async def delete(self, identity: IdentityContext, memory_id: str) -> None: ...


@runtime_checkable
class SafetyPort(Protocol):
    async def inspect(self, text: str, *, trust_domain: TrustDomain) -> SafetyAssessment: ...

    async def validate_answer(
        self,
        result: Any,
        *,
        trust_domain: TrustDomain = TrustDomain.MODEL_OUTPUT,
    ) -> SafetyAssessment: ...


@runtime_checkable
class ObjectStorePort(Protocol):
    async def save(
        self, tenant_id: str, document_id: str, filename: str, content: bytes
    ) -> tuple[str, str]: ...

    async def read(self, tenant_id: str, object_key: str) -> bytes: ...

    async def delete(self, tenant_id: str, object_key: str) -> None: ...


@runtime_checkable
class ChatModelPort(Protocol):
    async def complete(
        self, messages: Sequence[dict[str, str]], *, model: str, timeout: float
    ) -> ChatCompletion: ...

    def stream(
        self, messages: Sequence[dict[str, str]], *, model: str, timeout: float
    ) -> AsyncIterator[ChatDelta]: ...

    async def structured(
        self,
        messages: Sequence[dict[str, str]],
        *,
        model: str,
        response_model: type[StructuredT],
        timeout: float,
    ) -> StructuredT: ...


@runtime_checkable
class EmbeddingPort(Protocol):
    async def embed(
        self, texts: Sequence[str], *, input_type: str, timeout: float
    ) -> EmbeddingResult: ...


@runtime_checkable
class EntityExtractorPort(Protocol):
    async def extract(self, text: str, *, timeout: float) -> GraphExtraction: ...

    async def query_entities(self, query: str, *, timeout: float) -> list[str]: ...


@runtime_checkable
class RerankerPort(Protocol):
    async def rerank(
        self, query: str, candidates: Sequence[tuple[str, str]], *, timeout: float
    ) -> list[RerankItem]: ...


@runtime_checkable
class DocumentParserPort(Protocol):
    async def parse(self, filename: str, content: bytes) -> ParsedDocument: ...

    async def apply_ocr(self, parsed: ParsedDocument, content: bytes) -> ParsedDocument: ...


@runtime_checkable
class OCRPort(Protocol):
    async def recognize(self, image: bytes, *, page_number: int, timeout: float) -> str: ...


@runtime_checkable
class EventPublisherPort(Protocol):
    async def publish(self, event: EventEnvelope) -> None: ...


@runtime_checkable
class AuthorizationPort(Protocol):
    async def require_roles(self, identity: IdentityContext, *roles: str) -> None: ...


@runtime_checkable
class AuditPort(Protocol):
    async def record(self, event_type: str, fields: dict[str, Any]) -> None: ...


@runtime_checkable
class TracePort(Protocol):
    def span(self, name: str, attributes: dict[str, Any] | None = None) -> Any: ...


@runtime_checkable
class OrderServicePort(Protocol):
    async def query(self, identity: IdentityContext, order_id: str) -> OrderInfo: ...

    async def create_address_draft(
        self,
        identity: IdentityContext,
        order_id: str,
        masked_address: str,
        idempotency_key: str,
    ) -> ActionDraft: ...


@runtime_checkable
class LogisticsServicePort(Protocol):
    async def query(self, identity: IdentityContext, order_id: str) -> LogisticsInfo: ...

    async def create_urge_draft(
        self, identity: IdentityContext, order_id: str, idempotency_key: str
    ) -> ActionDraft: ...


@runtime_checkable
class RefundServicePort(Protocol):
    async def calculate(
        self, identity: IdentityContext, order_id: str, amount: str
    ) -> RefundQuote: ...

    async def create_draft(
        self,
        identity: IdentityContext,
        quote: RefundQuote,
        idempotency_key: str,
    ) -> ActionDraft: ...


@runtime_checkable
class ApprovalPort(Protocol):
    async def submit_fake(self, draft: ActionDraft) -> str: ...
