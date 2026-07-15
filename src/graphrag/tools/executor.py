"""Validated, timed and audited Tool invocation boundary."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from graphrag.domain.errors import ValidationError
from graphrag.domain.ids import new_id
from graphrag.domain.models import IdentityContext
from graphrag.domain.ports import AuditPort, TracePort
from graphrag.tools.registry import ToolRegistry, ToolSpec

ResultT = TypeVar("ResultT")


@dataclass(slots=True)
class ToolExecutor:
    registry: ToolRegistry
    audit: AuditPort
    trace: TracePort

    async def invoke(
        self,
        name: str,
        identity: IdentityContext,
        *,
        agent: str,
        run_id: str,
        payload: dict[str, Any],
        handler: Callable[[], Awaitable[ResultT]],
    ) -> ResultT:
        spec = self.registry.require(name, identity, agent=agent)
        self._validate_input(spec, payload)
        fields: dict[str, Any] = {
            "tool_call_id": new_id(),
            "run_id": run_id,
            "tenant_id": identity.tenant_id,
            "user_id": identity.user_id,
            "tool_name": spec.name,
            "schema_version": int(spec.name.rsplit(".v", 1)[1]),
            "risk_level": spec.risk_level.value,
            "input_keys": sorted(payload),
        }
        try:
            with self.trace.span(
                "tool.invoke",
                {
                    "run_id": run_id,
                    "tenant_id": identity.tenant_id,
                    "tool_name": spec.name,
                    "risk_level": spec.risk_level.value,
                },
            ):
                async with asyncio.timeout(spec.timeout_seconds):
                    result = await handler()
        except Exception as exc:
            await self.audit.record(
                spec.audit_event,
                {**fields, "status": "failed", "error_type": type(exc).__name__},
            )
            raise
        await self.audit.record(spec.audit_event, {**fields, "status": "succeeded"})
        return result

    @staticmethod
    def _validate_input(spec: ToolSpec, payload: dict[str, Any]) -> None:
        schema = spec.input_schema
        properties = schema.get("properties", {})
        unknown = set(payload).difference(properties)
        if schema.get("additionalProperties") is False and unknown:
            raise ValidationError(f"Tool 输入包含未知字段：{', '.join(sorted(unknown))}")
        missing = set(schema.get("required", ())).difference(payload)
        if missing:
            raise ValidationError(f"Tool 输入缺少字段：{', '.join(sorted(missing))}")
        for key, value in payload.items():
            definition = properties.get(key, {})
            if definition.get("type") == "string":
                if not isinstance(value, str):
                    raise ValidationError(f"Tool 字段 {key} 必须是字符串")
                if len(value) < int(definition.get("minLength", 0)):
                    raise ValidationError(f"Tool 字段 {key} 长度不足")
