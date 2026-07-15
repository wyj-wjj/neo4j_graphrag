"""Least-privilege Tool registry; agents only receive explicitly allowed schemas."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from graphrag.domain.errors import AuthorizationError, ConflictError, NotFoundError
from graphrag.domain.models import (
    ActionDraft,
    AnswerStatus,
    IdentityContext,
    LogisticsInfo,
    OrderInfo,
    RefundQuote,
    RiskLevel,
)
from graphrag.retrieval.pipeline import RetrievalResult
from graphrag.tools.schemas import DraftToolInputV1, QueryToolInputV1


class RetryPolicy(StrEnum):
    NONE = "none"
    IDEMPOTENT = "idempotent"


class ToolSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)
    name: str = Field(pattern=r"^[a-z][a-z0-9_.-]+\.v[1-9][0-9]*$")
    agent: str
    description: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    input_model: type[BaseModel] = Field(exclude=True)
    output_type: Any = Field(exclude=True)
    dependency: str
    risk_level: RiskLevel
    required_roles: frozenset[str]
    timeout_seconds: float = Field(gt=0, le=120)
    retry_policy: RetryPolicy
    max_attempts: int = Field(default=2, ge=1, le=5)
    retry_backoff_seconds: float = Field(default=0.05, ge=0.0, le=5.0)
    max_concurrency: int = Field(default=8, ge=1, le=1000)
    max_output_bytes: int = Field(default=20_000, ge=256, le=1_000_000)
    circuit_failure_threshold: int = Field(default=3, ge=1, le=100)
    circuit_reset_seconds: float = Field(default=10.0, gt=0, le=600)
    audit_event: str


class ToolRegistry:
    def __init__(self) -> None:
        self._specs: dict[str, ToolSpec] = {}

    def register(self, spec: ToolSpec) -> None:
        if spec.name in self._specs:
            raise ConflictError(f"Tool 已注册：{spec.name}")
        if (
            spec.risk_level != RiskLevel.READ
            and spec.retry_policy == RetryPolicy.IDEMPOTENT
            and "idempotency_key" not in spec.input_model.model_fields
        ):
            raise ValueError("可重试写 Tool 必须声明 idempotency_key")
        self._specs[spec.name] = spec

    def allowed_for(
        self, identity: IdentityContext, *, agent: str, maximum_risk: RiskLevel
    ) -> tuple[ToolSpec, ...]:
        order = {RiskLevel.READ: 0, RiskLevel.LOW_WRITE: 1, RiskLevel.CRITICAL: 2}
        return tuple(
            spec
            for spec in self._specs.values()
            if spec.agent == agent
            and bool(identity.roles & spec.required_roles)
            and order[spec.risk_level] <= order[maximum_risk]
        )

    def require(self, name: str, identity: IdentityContext, *, agent: str) -> ToolSpec:
        spec = self._specs.get(name)
        if spec is None:
            raise NotFoundError("Tool 不存在")
        if spec.agent != agent or not identity.roles.intersection(spec.required_roles):
            raise AuthorizationError("Tool 不属于当前 Agent 或角色")
        return spec


def build_default_registry(timeout_seconds: float) -> ToolRegistry:
    registry = ToolRegistry()
    definitions: list[tuple[str, str, RiskLevel, RetryPolicy, type[BaseModel], Any, str]] = [
        (
            "kb.hybrid_retrieve.v1",
            "kb",
            RiskLevel.READ,
            RetryPolicy.IDEMPOTENT,
            QueryToolInputV1,
            tuple[str, AnswerStatus, RetrievalResult],
            "knowledge",
        ),
        (
            "faq.match.v1",
            "faq",
            RiskLevel.READ,
            RetryPolicy.IDEMPOTENT,
            QueryToolInputV1,
            str,
            "knowledge",
        ),
        (
            "order.query.v1",
            "order",
            RiskLevel.READ,
            RetryPolicy.IDEMPOTENT,
            QueryToolInputV1,
            OrderInfo,
            "order",
        ),
        (
            "order.update_address_draft.v1",
            "order",
            RiskLevel.LOW_WRITE,
            RetryPolicy.IDEMPOTENT,
            DraftToolInputV1,
            ActionDraft,
            "order",
        ),
        (
            "logistics.query.v1",
            "logistics",
            RiskLevel.READ,
            RetryPolicy.IDEMPOTENT,
            QueryToolInputV1,
            LogisticsInfo,
            "logistics",
        ),
        (
            "logistics.urge_draft.v1",
            "logistics",
            RiskLevel.LOW_WRITE,
            RetryPolicy.IDEMPOTENT,
            DraftToolInputV1,
            ActionDraft,
            "logistics",
        ),
        (
            "refund.calculate.v1",
            "refund",
            RiskLevel.READ,
            RetryPolicy.IDEMPOTENT,
            QueryToolInputV1,
            RefundQuote,
            "refund",
        ),
        (
            "refund.create_draft.v1",
            "refund",
            RiskLevel.CRITICAL,
            RetryPolicy.IDEMPOTENT,
            DraftToolInputV1,
            ActionDraft,
            "refund",
        ),
        (
            "escalation.create_ticket.v1",
            "escalation",
            RiskLevel.LOW_WRITE,
            RetryPolicy.IDEMPOTENT,
            DraftToolInputV1,
            ActionDraft,
            "escalation",
        ),
    ]
    for name, agent, risk, retry, input_model, output_type, dependency in definitions:
        registry.register(
            ToolSpec(
                name=name,
                agent=agent,
                description=name.replace(".", " "),
                input_schema=input_model.model_json_schema(),
                output_schema=TypeAdapter(output_type).json_schema(),
                input_model=input_model,
                output_type=output_type,
                dependency=dependency,
                risk_level=risk,
                required_roles=frozenset({"user", "admin"}),
                timeout_seconds=timeout_seconds,
                retry_policy=retry,
                audit_event=f"tool.{name}",
            )
        )
    return registry
