"""Versioned domain models shared across use cases and adapters."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from graphrag.domain.ids import new_id


def utc_now() -> datetime:
    return datetime.now(UTC)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class IdentitySource(StrEnum):
    DEV = "dev"
    JWKS = "jwks"


class IdentityContext(StrictModel):
    tenant_id: str = Field(min_length=1, max_length=128)
    user_id: str = Field(min_length=1, max_length=128)
    roles: frozenset[str] = Field(min_length=1)
    departments: frozenset[str] = frozenset()
    source: IdentitySource = IdentitySource.DEV

    def has_role(self, *roles: str) -> bool:
        return bool(self.roles.intersection(roles))


class DocumentStatus(StrEnum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    DELETED = "deleted"


class IndexStatus(StrEnum):
    NOT_APPLICABLE = "not_applicable"
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class IngestionStatus(StrEnum):
    RECEIVED = "received"
    PARSING = "parsing"
    CHUNKED = "chunked"
    VECTOR_PROCESSING = "vector_processing"
    GRAPH_PROCESSING = "graph_processing"
    COMPLETED = "completed"
    PARTIAL_FAILED = "partial_failed"
    FAILED = "failed"


class SourceKind(StrEnum):
    REAL = "real"
    FAKE = "fake"


class AnswerStatus(StrEnum):
    ANSWERED = "answered"
    REFUSED = "refused"
    CLARIFICATION = "clarification"
    ESCALATED = "escalated"
    FAKE_RESULT = "fake_result"


class AgentIntent(StrEnum):
    FAQ = "faq"
    KB = "kb"
    ORDER = "order"
    LOGISTICS = "logistics"
    REFUND = "refund"
    ESCALATION = "escalation"
    CLARIFY = "clarify"


class RiskLevel(StrEnum):
    READ = "read"
    LOW_WRITE = "low_write"
    CRITICAL = "critical"


class DocumentRecord(StrictModel):
    document_id: str = Field(default_factory=new_id)
    tenant_id: str
    title: str = Field(min_length=1, max_length=500)
    status: DocumentStatus = DocumentStatus.ACTIVE
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class DocumentVersion(StrictModel):
    version_id: str = Field(default_factory=new_id)
    document_id: str
    tenant_id: str
    version: int = Field(ge=1)
    object_key: str
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    mime_type: str
    size_bytes: int = Field(ge=1)
    valid_from: datetime = Field(default_factory=utc_now)
    valid_until: datetime | None = None
    created_at: datetime = Field(default_factory=utc_now)

    @model_validator(mode="after")
    def validate_range(self) -> DocumentVersion:
        if self.valid_until is not None and self.valid_until <= self.valid_from:
            msg = "valid_until must be later than valid_from"
            raise ValueError(msg)
        return self


class ChunkRecord(StrictModel):
    chunk_id: str = Field(default_factory=new_id)
    tenant_id: str
    document_id: str
    version_id: str
    parent_chunk_id: str | None = None
    chunk_kind: Literal["parent", "child"] = "child"
    ordinal: int = Field(ge=0)
    content: str = Field(min_length=1)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    title_path: tuple[str, ...] = ()
    source_location: str = ""
    status: DocumentStatus = DocumentStatus.ACTIVE
    embedding_model: str
    embedding_dimension: int = Field(gt=0)
    embedding_version: str
    vector_status: IndexStatus = IndexStatus.PENDING
    graph_status: IndexStatus = IndexStatus.PENDING
    vector_error: str | None = None
    graph_error: str | None = None
    valid_from: datetime = Field(default_factory=utc_now)
    valid_until: datetime | None = None


class AccessPolicy(StrictModel):
    chunk_id: str
    tenant_id: str
    allowed_roles: frozenset[str] = frozenset()
    allowed_departments: frozenset[str] = frozenset()
    deny: bool = False
    valid_from: datetime = Field(default_factory=utc_now)
    valid_until: datetime | None = None

    def applies_to(self, identity: IdentityContext, *, now: datetime | None = None) -> bool:
        current = now or utc_now()
        if self.tenant_id != identity.tenant_id:
            return False
        if self.valid_from > current or (
            self.valid_until is not None and self.valid_until <= current
        ):
            return False
        role_allowed = not self.allowed_roles or bool(self.allowed_roles & identity.roles)
        department_allowed = not self.allowed_departments or bool(
            self.allowed_departments & identity.departments
        )
        return role_allowed and department_allowed

    def allows(self, identity: IdentityContext, *, now: datetime | None = None) -> bool:
        return not self.deny and self.applies_to(identity, now=now)


class IngestionTask(StrictModel):
    task_id: str = Field(default_factory=new_id)
    tenant_id: str
    document_id: str
    version_id: str | None = None
    status: IngestionStatus = IngestionStatus.RECEIVED
    attempt: int = Field(default=0, ge=0)
    error_code: str | None = None
    error_message: str | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class SearchCandidate(StrictModel):
    chunk_id: str
    score: float
    source: Literal["dense", "graph", "keyword"]
    rank: int = Field(ge=1)
    path: tuple[str, ...] = ()


class Evidence(StrictModel):
    chunk_id: str
    document_id: str
    document_title: str
    document_version: int
    updated_at: datetime
    source_location: str
    content: str
    score: float
    sources: tuple[str, ...]


class Citation(StrictModel):
    citation_id: str
    chunk_id: str
    document_id: str
    document_title: str
    document_version: int
    updated_at: datetime
    source_location: str


class ChatMessage(StrictModel):
    role: Literal["user", "assistant", "system"]
    content: str = Field(min_length=1, max_length=20000)
    created_at: datetime = Field(default_factory=utc_now)


class ChatResult(StrictModel):
    request_id: str
    run_id: str
    session_id: str
    status: AnswerStatus
    answer: str
    intent: AgentIntent
    citations: tuple[Citation, ...] = ()
    source: SourceKind = SourceKind.REAL


class ModelUsage(StrictModel):
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)


class ChatCompletion(StrictModel):
    content: str
    model: str
    finish_reason: str = "stop"
    usage: ModelUsage = ModelUsage()
    trace_metadata: dict[str, str] = Field(default_factory=dict)


class ChatDelta(StrictModel):
    sequence: int = Field(ge=1)
    content: str
    done: bool = False


class EmbeddingResult(StrictModel):
    vectors: tuple[tuple[float, ...], ...]
    model: str
    dimension: int
    version: str


class RerankItem(StrictModel):
    candidate_id: str
    score: float


class Entity(StrictModel):
    entity_id: str = Field(default_factory=new_id)
    name: str = Field(min_length=1, max_length=500)
    normalized_name: str = Field(min_length=1, max_length=500)
    entity_type: Literal[
        "Product",
        "Policy",
        "Process",
        "Document",
        "Organization",
        "Location",
        "Time",
        "Condition",
        "Other",
    ]
    aliases: tuple[str, ...] = ()
    confidence: float = Field(ge=0.0, le=1.0)


class EntityRelation(StrictModel):
    source_entity_id: str
    target_entity_id: str
    predicate: str = Field(pattern=r"^[A-Z][A-Z0-9_]{1,63}$")
    confidence: float = Field(ge=0.0, le=1.0)


class GraphExtraction(StrictModel):
    entities: tuple[Entity, ...] = ()
    relations: tuple[EntityRelation, ...] = ()
    extractor_version: str


class FAQItem(StrictModel):
    faq_id: str = Field(default_factory=new_id)
    tenant_id: str
    question: str
    answer: str
    patterns: tuple[str, ...] = ()
    version: int = Field(default=1, ge=1)
    status: DocumentStatus = DocumentStatus.ACTIVE
    valid_from: datetime = Field(default_factory=utc_now)
    valid_until: datetime | None = None


class OrderInfo(StrictModel):
    order_id: str
    owner_user_id: str
    status: str
    items: tuple[str, ...]
    masked_address: str
    source: SourceKind = SourceKind.FAKE


class LogisticsInfo(StrictModel):
    order_id: str
    status: str
    events: tuple[str, ...]
    source: SourceKind = SourceKind.FAKE


class RefundQuote(StrictModel):
    order_id: str
    amount: Decimal = Field(ge=Decimal("0"))
    currency: str = "CNY"
    requires_approval: bool = True
    source: SourceKind = SourceKind.FAKE


class ActionDraft(StrictModel):
    draft_id: str = Field(default_factory=new_id)
    tenant_id: str
    user_id: str
    action_type: Literal["update_address", "urge_delivery", "refund", "ticket"]
    idempotency_key: str = Field(min_length=8, max_length=200)
    payload: dict[str, Any]
    risk_level: RiskLevel
    status: Literal["draft", "pending_approval", "expired", "cancelled"] = "draft"
    source: SourceKind = SourceKind.FAKE
    created_at: datetime = Field(default_factory=utc_now)
    expires_at: datetime


class ApprovalDecision(StrictModel):
    request_id: str
    draft_id: str
    decision: Literal["approved", "rejected"]
    actor_id: str
    decided_at: datetime = Field(default_factory=utc_now)
    signature: str


class Page(StrictModel):
    text: str
    page_number: int = Field(ge=1)
    title_path: tuple[str, ...] = ()
    block_type: Literal["paragraph", "heading", "list", "table", "image"] = "paragraph"


class ParsedDocument(StrictModel):
    pages: tuple[Page, ...]
    parser: str
    requires_ocr: bool = False

    @field_validator("pages")
    @classmethod
    def no_empty_document(cls, value: tuple[Page, ...]) -> tuple[Page, ...]:
        if not value:
            msg = "document contains no parsable pages"
            raise ValueError(msg)
        return value


Score = Annotated[float, Field(ge=0.0, le=1.0)]
