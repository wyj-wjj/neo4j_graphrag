"""Least-privilege Tool registry; agents only receive explicitly allowed schemas."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from graphrag.domain.errors import AuthorizationError, ConflictError, NotFoundError
from graphrag.domain.models import IdentityContext, RiskLevel


class RetryPolicy(StrEnum):
    NONE = "none"
    IDEMPOTENT = "idempotent"


class ToolSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    name: str = Field(pattern=r"^[a-z][a-z0-9_.-]+\.v[1-9][0-9]*$")
    agent: str
    description: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    risk_level: RiskLevel
    required_roles: frozenset[str]
    timeout_seconds: float = Field(gt=0, le=120)
    retry_policy: RetryPolicy
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
            and "idempotency_key" not in spec.input_schema.get("properties", {})
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
    definitions = [
        ("kb.hybrid_retrieve.v1", "kb", RiskLevel.READ, RetryPolicy.IDEMPOTENT),
        ("faq.match.v1", "faq", RiskLevel.READ, RetryPolicy.IDEMPOTENT),
        ("order.query.v1", "order", RiskLevel.READ, RetryPolicy.IDEMPOTENT),
        ("order.update_address_draft.v1", "order", RiskLevel.LOW_WRITE, RetryPolicy.IDEMPOTENT),
        ("logistics.query.v1", "logistics", RiskLevel.READ, RetryPolicy.IDEMPOTENT),
        ("logistics.urge_draft.v1", "logistics", RiskLevel.LOW_WRITE, RetryPolicy.IDEMPOTENT),
        ("refund.calculate.v1", "refund", RiskLevel.READ, RetryPolicy.IDEMPOTENT),
        ("refund.create_draft.v1", "refund", RiskLevel.CRITICAL, RetryPolicy.IDEMPOTENT),
        ("escalation.create_ticket.v1", "escalation", RiskLevel.LOW_WRITE, RetryPolicy.IDEMPOTENT),
    ]
    for name, agent, risk, retry in definitions:
        properties: dict[str, Any] = {"query": {"type": "string"}}
        if risk != RiskLevel.READ:
            properties["idempotency_key"] = {"type": "string", "minLength": 8}
        registry.register(
            ToolSpec(
                name=name,
                agent=agent,
                description=name.replace(".", " "),
                input_schema={
                    "type": "object",
                    "properties": properties,
                    "required": sorted(properties),
                    "additionalProperties": False,
                },
                output_schema={"type": "object"},
                risk_level=risk,
                required_roles=frozenset({"user", "admin"}),
                timeout_seconds=timeout_seconds,
                retry_policy=retry,
                audit_event=f"tool.{name}",
            )
        )
    return registry
