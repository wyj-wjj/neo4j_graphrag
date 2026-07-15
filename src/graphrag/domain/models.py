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


type RefusalReason = Literal[
    "insufficient_evidence",
    "conflicting_evidence",
    "safety_policy",
    "dependency_unavailable",
]


class AgentIntent(StrEnum):
    FAQ = "faq"
    KB = "kb"
    ORDER = "order"
    LOGISTICS = "logistics"
    REFUND = "refund"
    ESCALATION = "escalation"
    CLARIFY = "clarify"


class RouteCandidate(StrictModel):
    intent: AgentIntent
    score: float = Field(ge=0.0, le=1.0)
    source: Literal["rule", "embedding", "small_model", "complex_model"]


class RouteDecision(StrictModel):
    router_version: str = "rule-router-v1"
    intent: AgentIntent
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str
    candidates: tuple[RouteCandidate, ...] = ()
    degraded: bool = False


class AgentPlanStep(StrictModel):
    ordinal: int = Field(ge=1, le=3)
    intent: AgentIntent
    read_only: bool


class AgentExecutionPlan(StrictModel):
    plan_version: str = "bounded-plan-v1"
    steps: tuple[AgentPlanStep, ...]
    max_steps: int = Field(default=2, ge=1, le=3)
    requires_arbitration: bool = False

    @model_validator(mode="after")
    def validate_bound(self) -> AgentExecutionPlan:
        if len(self.steps) > self.max_steps:
            raise ValueError("Agent plan exceeds max_steps")
        return self


class RiskLevel(StrEnum):
    READ = "read"
    LOW_WRITE = "low_write"
    CRITICAL = "critical"


class TrustDomain(StrEnum):
    IMMUTABLE_IDENTITY = "immutable_identity"
    USER_INPUT = "untrusted_user_input"
    EXTERNAL_EVIDENCE = "untrusted_external_evidence"
    VALIDATED_TOOL_RESULT = "validated_tool_result"
    MODEL_OUTPUT = "untrusted_model_output"


class MemoryCategory(StrEnum):
    PREFERENCE = "preference"
    LONG_TERM_GOAL = "long_term_goal"
    CONFIRMED_FACT = "confirmed_fact"


class MemoryStatus(StrEnum):
    ACTIVE = "active"
    CORRECTED = "corrected"
    DELETED = "deleted"


class SensitivityLevel(StrEnum):
    LOW = "low"
    MODERATE = "moderate"


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
    valid_from: datetime | None = None
    valid_until: datetime | None = None
    applicability: str = "tenant_authorized"
    authority_rank: int = Field(default=100, ge=0, le=1000)
    conflict_group: str | None = None


class Citation(StrictModel):
    citation_id: str
    chunk_id: str
    document_id: str
    document_title: str
    document_version: int
    updated_at: datetime
    source_location: str


class AgentOutcome(StrictModel):
    """A bounded expert result supplied to deterministic convergence."""

    intent: AgentIntent
    answer: str = Field(min_length=1, max_length=20_000)
    authority_rank: int = Field(ge=0, le=1000)
    evidence_updated_at: datetime | None = None
    citations: tuple[Citation, ...] = ()


class ConsolidatedOutcome(StrictModel):
    """Auditable result of converging at most two expert outcomes."""

    consolidation_version: str = "deterministic-consolidator-v1"
    answer: str = Field(min_length=1, max_length=20_000)
    intents: tuple[AgentIntent, ...]
    citations: tuple[Citation, ...] = ()
    requires_clarification: bool = False
    arbitration_basis: Literal["single", "authority", "recency", "conflict", "combined"]


class ChatMessage(StrictModel):
    role: Literal["user", "assistant", "system"]
    content: str = Field(min_length=1, max_length=20000)
    created_at: datetime = Field(default_factory=utc_now)


class SessionMessage(ChatMessage):
    """Durable message metadata used for ordering and idempotency."""

    message_id: str = Field(default_factory=new_id)
    client_turn_id: str = Field(min_length=8, max_length=128)
    run_id: str
    sequence: int = Field(ge=1)
    status: Literal["committed"] = "committed"
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class ConversationConstraint(StrictModel):
    kind: Literal["amount", "permission", "region", "deadline", "negation", "approval"]
    name: str = Field(min_length=1, max_length=100)
    value: str = Field(min_length=1, max_length=500)
    source_message_id: str
    source_sequence: int = Field(ge=1)
    confirmed: bool = True
    updated_at: datetime = Field(default_factory=utc_now)


class ConversationState(StrictModel):
    state_version: int = 1
    tenant_id: str
    session_id: str
    goal: str | None = None
    task_stage: str | None = None
    constraints: tuple[ConversationConstraint, ...] = ()
    confirmed_facts: tuple[str, ...] = ()
    open_questions: tuple[str, ...] = ()
    summary: str = ""
    summary_version: int = 0
    summary_through_sequence: int = 0
    last_processed_sequence: int = 0


class ContextDrop(StrictModel):
    item_type: Literal["message", "summary", "evidence", "tool_result"]
    item_id: str
    reason: Literal["component_budget", "global_budget", "superseded", "invalid"]
    estimated_tokens: int = Field(ge=0)


class ContextManifest(StrictModel):
    manifest_version: int = 1
    manifest_id: str = Field(default_factory=new_id)
    run_id: str
    tenant_id: str
    session_id: str
    model: str
    context_policy_version: str
    model_context_window_tokens: int = Field(gt=0)
    reserved_output_tokens: int = Field(ge=0)
    input_budget_tokens: int = Field(gt=0)
    target_tokens: dict[str, int]
    actual_tokens: dict[str, int]
    actual_input_tokens: int = Field(ge=0)
    borrowed_tokens: dict[str, int] = Field(default_factory=dict)
    selected_message_ids: tuple[str, ...] = ()
    selected_evidence_ids: tuple[str, ...] = ()
    selected_tool_result_ids: tuple[str, ...] = ()
    dropped_items: tuple[ContextDrop, ...] = ()
    created_at: datetime = Field(default_factory=utc_now)


class GenerationManifest(StrictModel):
    manifest_version: int = 1
    manifest_id: str = Field(default_factory=new_id)
    run_id: str
    tenant_id: str
    prompt_bundle_version: str
    prompt_hashes: dict[str, str]
    chat_model: str
    router_version: str
    embedding_model: str
    embedding_version: str
    rerank_model: str
    state_version: int = Field(ge=1)
    context_policy_version: str
    evaluation_set_version: str
    created_at: datetime = Field(default_factory=utc_now)


class LongTermMemory(StrictModel):
    memory_id: str = Field(default_factory=new_id)
    tenant_id: str
    user_id: str
    category: MemoryCategory
    key: str = Field(min_length=1, max_length=100)
    value: str = Field(min_length=1, max_length=2000)
    source_turn_id: str
    confirmation_method: Literal["explicit_user", "authoritative_source"]
    confirmed_by: str
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    sensitivity: SensitivityLevel = SensitivityLevel.LOW
    version: int = Field(default=1, ge=1)
    status: MemoryStatus = MemoryStatus.ACTIVE
    supersedes_memory_id: str | None = None
    valid_from: datetime = Field(default_factory=utc_now)
    expires_at: datetime | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    deleted_at: datetime | None = None


class MemorySettings(StrictModel):
    tenant_id: str
    user_id: str
    enabled: bool = True
    auto_write_enabled: bool = False
    updated_at: datetime = Field(default_factory=utc_now)


class SafetyAssessment(StrictModel):
    policy_version: str = "safety-policy-v1"
    trust_domain: TrustDomain
    allowed: bool
    flags: tuple[str, ...] = ()
    action: Literal["allow", "treat_as_data", "block", "escalate"]


class KnowledgeIssue(StrictModel):
    issue_id: str = Field(default_factory=new_id)
    issue_type: Literal["duplicate", "conflict", "expired", "low_quality"]
    document_ids: tuple[str, ...]
    version_ids: tuple[str, ...] = ()
    chunk_ids: tuple[str, ...] = ()
    reason: str


class KnowledgeQualityReport(StrictModel):
    report_version: int = 1
    tenant_id: str
    issues: tuple[KnowledgeIssue, ...]
    generated_at: datetime = Field(default_factory=utc_now)


class ChatResult(StrictModel):
    request_id: str
    run_id: str
    client_turn_id: str | None = None
    session_id: str
    status: AnswerStatus
    answer: str
    intent: AgentIntent
    citations: tuple[Citation, ...] = ()
    source: SourceKind = SourceKind.REAL
    open_questions: tuple[str, ...] = ()
    actions: tuple[str, ...] = ()
    refusal_reason: RefusalReason | None = None


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


class SafeResumeSnapshot(StrictModel):
    snapshot_id: str = Field(default_factory=new_id)
    tenant_id: str
    user_id: str
    roles: frozenset[str]
    session_id: str
    run_id: str
    request_id: str
    client_turn_id: str
    draft_id: str
    safe_node: Literal["approval_interrupt"] = "approval_interrupt"
    status: Literal["pending", "resumed", "expired", "failed"] = "pending"
    decision: Literal["approved", "rejected"] | None = None
    next_action: Literal["await_approval", "none"] = "await_approval"
    completed_side_effects: tuple[str, ...] = ()
    state_version: int = 2
    checkpoint_version: str = "agent-state-v2"
    recovery_source: Literal["langgraph", "mysql_snapshot"] = "langgraph"
    resume_result_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    final_answer: str | None = Field(default=None, max_length=2000)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    expires_at: datetime


class ApprovalResumeResult(StrictModel):
    run_id: str
    draft_id: str
    decision: Literal["approved", "rejected"]
    duplicate: bool = False
    recovery_source: Literal["langgraph", "mysql_snapshot"]
    final_answer: str
    executed: bool = False


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
