"""Strict public schemas for profiles, records and manifests."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from graphrag_data_factory.constants import (
    SUPPORTED_PROFILES,
)


class StrictModel(BaseModel):
    """Reject unknown fields so contract drift fails early."""

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class ProfileCounts(StrictModel):
    tenants: int = Field(ge=1, le=100)
    users: int = Field(ge=6, le=200_000)
    products: int = Field(ge=8, le=100_000)
    orders: int = Field(ge=1, le=2_000_000)
    conversations: int = Field(ge=1, le=1_000_000)
    knowledge_documents: int = Field(ge=1, le=10_000)
    physical_files: int = Field(ge=27, le=10_000)
    events: int = Field(ge=1, le=20_000_000)
    evaluation_cases: int = Field(ge=1, le=100_000)
    security_cases: int = Field(ge=1, le=100_000)
    memory_cases: int = Field(ge=1, le=100_000)
    golden_candidates: int = Field(ge=1, le=10_000)
    event_deliveries: int = Field(ge=1, le=25_000_000)
    fault_schedules: int = Field(ge=1, le=100_000)


class DatasetProfile(StrictModel):
    profile_version: Literal["profile-v1"]
    name: str
    enabled: bool
    requires_explicit_large_flag: bool
    root_seed: int = Field(ge=1, le=2**63 - 1)
    reference_time: datetime
    batch_size: int = Field(ge=1, le=100_000)
    counts: ProfileCounts
    intent_percentages: dict[
        Literal["faq", "kb", "order", "logistics", "refund", "escalation", "multi_intent"],
        int,
    ]

    @field_validator("reference_time")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("reference_time must be timezone-aware")
        return value

    @model_validator(mode="after")
    def validate_profile(self) -> DatasetProfile:
        if self.name not in SUPPORTED_PROFILES:
            raise ValueError(f"unsupported profile: {self.name}")
        if self.name != "ci-small" and not self.requires_explicit_large_flag:
            raise ValueError("non-CI profiles must retain the explicit large-profile gate")
        if not self.enabled:
            raise ValueError(f"profile is disabled: {self.name}")
        if sum(self.intent_percentages.values()) != 100:
            raise ValueError("intent_percentages must sum to 100")
        if any(value < 0 for value in self.intent_percentages.values()):
            raise ValueError("intent percentages cannot be negative")
        return self


class SyntheticRecord(StrictModel):
    dataset_id: str = Field(min_length=8, max_length=160)
    dataset_version: Literal["synthetic-commerce-v1"]
    scenario_id: str = Field(min_length=8, max_length=160)
    tenant_id: str = Field(pattern=r"^synthetic-[a-z0-9-]+$")
    source: Literal["fake"] = "fake"
    is_synthetic: Literal[True] = True
    schema_version: Literal["synthetic-record-v1"] = "synthetic-record-v1"


class TenantRecord(SyntheticRecord):
    tenant_name: str
    tier: Literal["standard", "enterprise", "regulated"]
    timezone: Literal["Asia/Shanghai"] = "Asia/Shanghai"
    status: Literal["active", "disabled"] = "active"
    isolation_policy: Literal["logical", "dedicated"]
    quotas: dict[str, int]


class UserRecord(SyntheticRecord):
    user_id: str
    display_name: str
    email: str = Field(pattern=r"^[a-z0-9._-]+@[a-z0-9-]+\.invalid$")
    masked_phone: Literal["000-0000-0000"] = "000-0000-0000"
    role: Literal[
        "customer",
        "customer_service",
        "supervisor",
        "knowledge_admin",
        "auditor",
        "tenant_admin",
    ]
    department_id: str
    region: Literal["测试省-演示市"] = "测试省-演示市"
    active: bool = True


class AccessDecisionRecord(SyntheticRecord):
    decision_id: str
    actor_user_id: str
    resource_type: Literal["order", "knowledge", "approval", "audit"]
    resource_id: str
    resource_tenant_id: str
    relation: Literal["owner", "department", "tenant_role", "cross_tenant", "missing_policy"]
    explicit_deny: bool
    dependency_available: bool
    expected_allowed: bool
    reason_code: Literal[
        "owner_allowed",
        "role_allowed",
        "explicit_deny",
        "cross_tenant",
        "missing_policy",
        "dependency_unavailable",
    ]


class ProductRecord(SyntheticRecord):
    product_id: str
    sku: str
    name: str
    category: Literal["standard", "virtual", "presale", "bundle", "non_returnable"]
    unit_price: Decimal = Field(gt=Decimal("0"), decimal_places=2)
    tax_rate: Decimal = Field(ge=Decimal("0"), le=Decimal("0.20"), decimal_places=4)
    returnable: bool
    aliases: tuple[str, ...]


class OrderLine(StrictModel):
    line_id: str
    product_id: str
    sku: str
    quantity: int = Field(ge=1, le=20)
    unit_price: Decimal = Field(gt=Decimal("0"), decimal_places=2)
    subtotal: Decimal = Field(gt=Decimal("0"), decimal_places=2)
    returnable: bool


class StatePoint(StrictModel):
    state: str
    occurred_at: datetime

    @field_validator("occurred_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must be timezone-aware")
        return value


class OrderRecord(SyntheticRecord):
    order_id: str
    owner_user_id: str
    status: Literal[
        "created",
        "paid",
        "allocated",
        "shipped",
        "delivered",
        "closed",
        "cancelled",
        "refund_pending",
    ]
    currency: Literal["CNY"] = "CNY"
    lines: tuple[OrderLine, ...] = Field(min_length=1)
    subtotal: Decimal = Field(ge=Decimal("0"), decimal_places=2)
    discount: Decimal = Field(ge=Decimal("0"), decimal_places=2)
    shipping_fee: Decimal = Field(ge=Decimal("0"), decimal_places=2)
    tax: Decimal = Field(ge=Decimal("0"), decimal_places=2)
    total: Decimal = Field(ge=Decimal("0"), decimal_places=2)
    refunded_amount: Decimal = Field(ge=Decimal("0"), decimal_places=2)
    address_snapshot: Literal["测试省/演示市/样例区/***路***号"]
    high_value: bool
    status_history: tuple[StatePoint, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_amounts(self) -> OrderRecord:
        line_total = sum((line.subtotal for line in self.lines), Decimal("0.00"))
        if line_total != self.subtotal:
            raise ValueError("order subtotal does not equal line subtotals")
        if self.total != self.subtotal - self.discount + self.shipping_fee + self.tax:
            raise ValueError("order total formula mismatch")
        if self.refunded_amount > self.total:
            raise ValueError("refunded amount exceeds total")
        return self


class OrderOracleRecord(SyntheticRecord):
    oracle_case_id: str
    actor_user_id: str
    order_id: str
    operation: Literal["view", "update_address", "calculate_refund"]
    expected_allowed: bool
    expected_error_code: str | None
    address_requires_approval: bool
    max_refundable_amount: Decimal = Field(ge=Decimal("0"), decimal_places=2)
    oracle_version: str


class TrackingPoint(StatePoint):
    location: str
    detail: str


class PackageRecord(SyntheticRecord):
    package_id: str
    order_id: str
    tracking_number: str
    carrier: str
    status: Literal[
        "label_created",
        "picked_up",
        "in_transit",
        "exception",
        "out_for_delivery",
        "delivered",
        "returned",
    ]
    line_quantities: dict[str, int]
    tracking_events: tuple[TrackingPoint, ...] = Field(min_length=1)


class LogisticsOracleRecord(SyntheticRecord):
    oracle_case_id: str
    actor_user_id: str
    package_id: str
    anomaly: Literal[
        "none", "stale", "wrong_hub", "refused", "lost", "damaged", "returned", "timeout"
    ]
    urge_attempt: int = Field(ge=1, le=10)
    expected_allowed: bool
    expected_error_code: str | None
    expected_escalation: bool
    oracle_version: str


class RefundRecord(SyntheticRecord):
    refund_id: str
    order_id: str
    requested_by: str
    requested_amount: Decimal = Field(gt=Decimal("0"), decimal_places=2)
    max_refundable_amount: Decimal = Field(ge=Decimal("0"), decimal_places=2)
    currency: Literal["CNY"] = "CNY"
    risk_level: Literal["low", "medium", "high"]
    requires_approval: bool
    status: Literal[
        "draft",
        "approval_pending",
        "approved",
        "rejected",
        "expired",
        "reconciled",
        "compensation_required",
    ]
    idempotency_key: str
    order_snapshot_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: datetime


class ApprovalRecord(SyntheticRecord):
    approval_id: str
    refund_id: str
    draft_version: int = Field(ge=1)
    policy_version: Literal["synthetic-refund-policy-v1"]
    status: Literal["pending", "approved", "rejected", "expired", "cancelled"]
    approver_user_id: str
    approver_role: Literal["supervisor"]
    decision_reason: str
    callback_idempotency_key: str
    decided_at: datetime | None


class RefundLifecycleRecord(SyntheticRecord):
    lifecycle_id: str
    refund_id: str
    approval_id: str | None
    attempt_kind: Literal[
        "first",
        "idempotent_replay",
        "payload_conflict",
        "unknown_result",
        "reconciled",
        "compensation",
        "compensation_failed",
    ]
    expected_status: Literal[
        "blocked",
        "pending",
        "executed",
        "unknown",
        "reconciled",
        "compensated",
        "manual_intervention",
    ]
    expected_side_effect_count: int = Field(ge=0, le=1)
    expected_error_code: str | None
    oracle_version: str


class KnowledgeRecord(SyntheticRecord):
    document_id: str
    title: str
    topic: Literal[
        "product",
        "invoice",
        "membership",
        "logistics",
        "returns",
        "support",
        "privacy",
        "region",
        "channel",
        "time",
    ]
    version: int = Field(ge=1)
    status: Literal["draft", "review", "active", "expired", "inactive", "deleted"]
    authority: Literal["official", "department", "reference"]
    evidence_anchor_id: str
    relative_path: str
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    valid_from: datetime
    valid_until: datetime | None
    contains_prompt_injection: bool = False


class PhysicalKnowledgeFileRecord(SyntheticRecord):
    physical_file_id: str
    document_id: str
    evidence_anchor_id: str | None
    format: Literal["md", "txt", "html", "pdf", "docx", "xlsx", "pptx", "png", "jpeg"]
    variant: Literal["normal", "boundary", "corrupt"]
    relative_path: str
    mime_type: str
    size_bytes: int = Field(ge=1)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_outcome: Literal["parse_success", "ocr_success", "reject_corrupt"]


class ConversationTurn(StrictModel):
    sequence: int = Field(ge=1)
    role: Literal["user", "assistant"]
    text: str
    constraint_updates: dict[str, str]


class ConversationRecord(SyntheticRecord):
    conversation_id: str
    user_id: str
    channel: Literal["web", "app", "miniapp", "agent_console"]
    intent: Literal["faq", "kb", "order", "logistics", "refund", "escalation", "multi_intent"]
    scenario_family: str
    split: Literal["train", "dev", "test", "holdout"]
    order_id: str | None
    turns: tuple[ConversationTurn, ...] = Field(min_length=1)
    expected_agents: tuple[str, ...] = Field(min_length=1, max_length=2)
    allowed_tools: tuple[str, ...]
    forbidden_side_effects: tuple[str, ...]


class EventRecord(SyntheticRecord):
    event_id: str
    event_type: str
    event_version: Literal[1] = 1
    aggregate_type: Literal["order", "package", "refund", "knowledge", "conversation"]
    aggregate_id: str
    sequence: int = Field(ge=1)
    occurred_at: datetime
    trace_id: str
    payload_summary: dict[str, Any]


class EventDeliveryRecord(SyntheticRecord):
    delivery_id: str
    event_id: str | None
    delivery_ordinal: int = Field(ge=1)
    delivery_kind: Literal["normal", "duplicate", "out_of_order", "poison", "unknown_schema"]
    delivered_at: datetime
    expected_route: Literal["inbox", "duplicate_ignored", "dlq"]
    payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload: dict[str, Any]


class ReplayExpectationRecord(SyntheticRecord):
    replay_id: str
    aggregate_type: Literal["order", "package", "refund", "knowledge", "conversation", "dataset"]
    aggregate_id: str
    expected_unique_events: int = Field(ge=0)
    expected_duplicate_events: int = Field(ge=0)
    expected_dlq_events: int = Field(ge=0)
    expected_terminal_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    oracle_version: str


class DlqRepairRecord(SyntheticRecord):
    repair_id: str
    delivery_id: str
    original_payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    failure_kind: Literal["poison", "unknown_schema"]
    failure_reason: str
    repair_version: Literal[1] = 1
    operator_user_id: str
    operator_role: Literal["supervisor"] = "supervisor"
    replay_id: str
    replay_audit_id: str
    expected_replay_result: Literal["accepted"] = "accepted"
    expected_duplicate_side_effect_count: Literal[0] = 0
    oracle_version: str


class EvaluationCaseRecord(SyntheticRecord):
    case_id: str
    conversation_id: str
    split: Literal["train", "dev", "test", "holdout"]
    expected_intent: str
    expected_agents: tuple[str, ...] = Field(min_length=1, max_length=2)
    expected_tool_order: tuple[str, ...]
    evidence_anchor_ids: tuple[str, ...]
    allowed_side_effects: tuple[str, ...]
    forbidden_side_effects: tuple[str, ...]
    expected_answer_status: Literal[
        "answered", "refused", "clarification", "escalated", "fake_result"
    ]
    oracle_version: str
    prompt_version: Literal["synthetic-prompt-v1"] = "synthetic-prompt-v1"
    model_profile: Literal["fake-model-profile-v1"] = "fake-model-profile-v1"
    router_version: Literal["synthetic-router-v1"] = "synthetic-router-v1"
    state_schema_version: Literal["agent-state-v1"] = "agent-state-v1"
    context_policy_version: Literal["context-policy-v1"] = "context-policy-v1"
    tool_schema_versions: tuple[str, ...]
    event_schema_version: Literal["event-envelope-v1"] = "event-envelope-v1"
    quality_tier: Literal["silver"] = "silver"


class ContextBudget(StrictModel):
    context_window_tokens: Literal[32768] = 32768
    output_reserve_tokens: Literal[4096] = 4096
    safety_reserve_tokens: Literal[2048] = 2048
    available_input_tokens: Literal[26624] = 26624
    short_term_tokens: Literal[7987] = 7987
    working_memory_tokens: Literal[5325] = 5325
    external_evidence_tokens: Literal[10650] = 10650
    system_protocol_tokens: Literal[2662] = 2662

    @model_validator(mode="after")
    def validate_budget(self) -> ContextBudget:
        if (
            self.output_reserve_tokens + self.safety_reserve_tokens + self.available_input_tokens
            != self.context_window_tokens
        ):
            raise ValueError("context reserves and available input do not fill the window")
        if (
            self.short_term_tokens
            + self.working_memory_tokens
            + self.external_evidence_tokens
            + self.system_protocol_tokens
            != self.available_input_tokens
        ):
            raise ValueError("context partitions do not fill available input")
        return self


class MemoryCaseRecord(SyntheticRecord):
    memory_case_id: str
    conversation_id: str
    turns: int = Field(ge=4, le=32)
    expected_constraints: dict[str, str]
    expected_summary_covered_sequence: int = Field(ge=0)
    expected_duplicate_side_effects: Literal[0] = 0
    expected_cross_tenant_leakage: Literal[0] = 0
    recovery_mode: Literal["normal", "checkpoint", "mysql_snapshot"]
    context_policy_version: Literal["context-policy-v1"] = "context-policy-v1"
    target_budget: ContextBudget = Field(default_factory=ContextBudget)
    oracle_version: str


class GoldenCandidateRecord(SyntheticRecord):
    candidate_id: str
    evaluation_case_id: str
    fact_record_ids: tuple[str, ...] = Field(min_length=1)
    evidence_anchor_ids: tuple[str, ...]
    expected_behavior: str
    review_checklist: tuple[str, ...] = Field(min_length=4)
    required_independent_reviewers: Literal[2] = 2
    review_status: Literal["pending_independent_review"] = "pending_independent_review"
    reviewer_id: None = None
    reviewed_at: None = None
    promotion_eligible: Literal[False] = False
    quality_tier: Literal["golden_candidate"] = "golden_candidate"


class SecurityCaseRecord(SyntheticRecord):
    security_case_id: str
    attack_type: Literal[
        "cross_tenant",
        "role_spoofing",
        "prompt_injection",
        "secret_exfiltration",
        "fake_citation",
        "unapproved_write",
        "pii_exfiltration",
        "tool_result_injection",
    ]
    input_text: str
    expected_action: Literal["deny", "refuse", "ignore_instruction"]
    forbidden_outputs: tuple[str, ...]


class FaultScheduleRecord(SyntheticRecord):
    fault_id: str
    dependency: Literal["mysql", "milvus", "neo4j", "redis", "business_api", "event_consumer"]
    mode: Literal[
        "timeout", "rate_limit", "server_error", "disconnect", "malformed", "unknown_result"
    ]
    trigger_after_calls: int = Field(ge=1)
    duration_ms: int = Field(ge=1, le=120_000)
    expected_error_code: str
    expected_retryable: bool
    allowed_in_production: Literal[False] = False


class ValidationIssue(StrictModel):
    code: Literal[
        "schema",
        "relationship",
        "amount",
        "time",
        "tenant",
        "permission",
        "replay",
        "leakage",
        "resource",
        "determinism",
    ]
    message: str
    relative_path: str | None = None
    scenario_id: str | None = None
    entity_id: str | None = None


class FileManifest(StrictModel):
    relative_path: str = Field(min_length=1)
    media_type: str
    record_type: str
    records: int = Field(ge=0)
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("relative_path")
    @classmethod
    def validate_relative_path(cls, value: str) -> str:
        if value.startswith("/") or ".." in value.split("/"):
            raise ValueError("relative_path must stay inside the dataset directory")
        return value


class DatasetManifest(StrictModel):
    manifest_version: Literal["dataset-manifest-v1"] = "dataset-manifest-v1"
    dataset_id: str
    dataset_version: Literal["synthetic-commerce-v1"]
    spec_version: Literal["phase2-data-spec-v1"]
    generator_version: str
    schema_version: Literal["synthetic-record-v1"]
    rules_version: str
    template_version: str
    oracle_version: str
    profile: str
    profile_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    root_seed: int
    derived_seed_ids: dict[str, str]
    deterministic_created_at: datetime
    status: Literal["validated"]
    is_synthetic: Literal[True] = True
    production_slo_eligible: Literal[False] = False
    files: tuple[FileManifest, ...] = Field(min_length=1)
    record_counts: dict[str, int]
    compatibility: dict[str, str]
    canaries: tuple[Literal["CANARY_SECRET_VALUE"], ...] = ("CANARY_SECRET_VALUE",)
    validation_report: Literal["validation-report.json"] = "validation-report.json"

    @model_validator(mode="after")
    def validate_files(self) -> DatasetManifest:
        paths = [item.relative_path for item in self.files]
        if len(paths) != len(set(paths)):
            raise ValueError("manifest contains duplicate file paths")
        return self


JsonValue = Annotated[dict[str, Any], Field(description="Canonical JSON object")]
