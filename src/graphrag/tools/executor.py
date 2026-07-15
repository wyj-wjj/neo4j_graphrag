"""Validated, bounded and audited Tool invocation boundary."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from time import monotonic
from typing import Any, TypeVar, cast

from pydantic import TypeAdapter
from pydantic import ValidationError as PydanticValidationError

from graphrag.domain.errors import (
    AppError,
    DependencyError,
    OperationTimeoutError,
    ValidationError,
)
from graphrag.domain.ids import new_id
from graphrag.domain.models import IdentityContext
from graphrag.domain.ports import AuditPort, TracePort
from graphrag.tools.registry import RetryPolicy, ToolRegistry, ToolSpec

ResultT = TypeVar("ResultT")


@dataclass(slots=True)
class CircuitState:
    consecutive_failures: int = 0
    opened_at: float | None = None


@dataclass(slots=True)
class ToolExecutor:
    """Execute a Tool through one policy boundary.

    The timeout is a total budget, including bulkhead waiting, retries and backoff.
    A retry is only possible for an explicitly idempotent Tool and a transient error.
    """

    registry: ToolRegistry
    audit: AuditPort
    trace: TracePort
    _bulkheads: dict[str, asyncio.Semaphore] = field(default_factory=dict, init=False)
    _circuits: dict[str, CircuitState] = field(default_factory=dict, init=False)

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
        self._assert_circuit_available(spec)
        fields: dict[str, Any] = {
            "tool_call_id": new_id(),
            "run_id": run_id,
            "tenant_id": identity.tenant_id,
            "user_id": identity.user_id,
            "tool_name": spec.name,
            "schema_version": int(spec.name.rsplit(".v", 1)[1]),
            "risk_level": spec.risk_level.value,
            "dependency": spec.dependency,
            "output_trust_domain": "validated_tool_result",
            "input_keys": sorted(payload),
        }
        attempt_counter = [0]
        try:
            with self.trace.span(
                "tool.invoke",
                {
                    "run_id": run_id,
                    "tenant_id": identity.tenant_id,
                    "tool_name": spec.name,
                    "risk_level": spec.risk_level.value,
                    "dependency": spec.dependency,
                },
            ):
                async with asyncio.timeout(spec.timeout_seconds):
                    async with self._bulkhead_for(spec):
                        result = await self._invoke_with_retries(
                            spec, fields, handler, attempt_counter
                        )
            validated = self._validate_output(spec, result)
        except asyncio.CancelledError:
            await self.audit.record(
                spec.audit_event,
                {**fields, "status": "cancelled", "attempts": attempt_counter[0]},
            )
            raise
        except TimeoutError as exc:
            self._record_transient_failure(spec)
            error = OperationTimeoutError(spec.dependency)
            await self._record_failure(fields, spec, error, attempt_counter[0])
            raise error from exc
        except Exception as exc:
            if self._is_transient(exc):
                self._record_transient_failure(spec)
            await self._record_failure(fields, spec, exc, attempt_counter[0])
            raise

        self._record_success(spec)
        await self.audit.record(
            spec.audit_event,
            {
                **fields,
                "status": "succeeded",
                "attempts": attempt_counter[0],
                "retries": max(0, attempt_counter[0] - 1),
            },
        )
        return cast(ResultT, validated)

    async def _invoke_with_retries(
        self,
        spec: ToolSpec,
        fields: dict[str, Any],
        handler: Callable[[], Awaitable[ResultT]],
        attempt_counter: list[int],
    ) -> ResultT:
        max_attempts = spec.max_attempts if spec.retry_policy == RetryPolicy.IDEMPOTENT else 1
        attempts = 0
        while True:
            attempts += 1
            attempt_counter[0] = attempts
            try:
                return await handler()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                can_retry = attempts < max_attempts and self._is_transient(exc)
                if not can_retry:
                    raise
                await self.audit.record(
                    spec.audit_event,
                    {
                        **fields,
                        "status": "retrying",
                        "attempt": attempts,
                        "error_type": type(exc).__name__,
                    },
                )
                if spec.retry_backoff_seconds:
                    await asyncio.sleep(spec.retry_backoff_seconds * attempts)

    async def _record_failure(
        self,
        fields: dict[str, Any],
        spec: ToolSpec,
        exc: BaseException,
        attempts: int,
    ) -> None:
        await self.audit.record(
            spec.audit_event,
            {
                **fields,
                "status": "failed",
                "attempts": attempts,
                "retries": max(0, attempts - 1),
                "error_type": type(exc).__name__,
            },
        )

    @staticmethod
    def _validate_input(spec: ToolSpec, payload: dict[str, Any]) -> None:
        try:
            spec.input_model.model_validate(payload)
        except PydanticValidationError as exc:
            raise ValidationError("Tool 输入不符合版本化 Schema") from exc

    @staticmethod
    def _validate_output(spec: ToolSpec, result: Any) -> Any:
        try:
            adapter = TypeAdapter(spec.output_type)
            validated = adapter.validate_python(result)
            if len(adapter.dump_json(validated)) > spec.max_output_bytes:
                raise ValidationError("Tool 输出超过允许大小")
            return validated
        except PydanticValidationError as exc:
            raise ValidationError("Tool 输出不符合版本化 Schema") from exc

    def _bulkhead_for(self, spec: ToolSpec) -> asyncio.Semaphore:
        semaphore = self._bulkheads.get(spec.dependency)
        if semaphore is None:
            semaphore = asyncio.Semaphore(spec.max_concurrency)
            self._bulkheads[spec.dependency] = semaphore
        return semaphore

    def _assert_circuit_available(self, spec: ToolSpec) -> None:
        state = self._circuits.setdefault(spec.dependency, CircuitState())
        if state.opened_at is None:
            return
        if monotonic() - state.opened_at >= spec.circuit_reset_seconds:
            state.opened_at = None
            state.consecutive_failures = 0
            return
        raise DependencyError(
            spec.dependency,
            f"{spec.dependency} 依赖熔断中",
            retryable=True,
        )

    def _record_transient_failure(self, spec: ToolSpec) -> None:
        state = self._circuits.setdefault(spec.dependency, CircuitState())
        state.consecutive_failures += 1
        if state.consecutive_failures >= spec.circuit_failure_threshold:
            state.opened_at = monotonic()

    def _record_success(self, spec: ToolSpec) -> None:
        state = self._circuits.setdefault(spec.dependency, CircuitState())
        state.consecutive_failures = 0
        state.opened_at = None

    @staticmethod
    def _is_transient(exc: BaseException) -> bool:
        if isinstance(exc, (TimeoutError, ConnectionError)):
            return True
        return isinstance(exc, AppError) and exc.retryable
