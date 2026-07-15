"""Non-production HTTP simulator backed only by a validated synthetic dataset."""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, Header, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator

from graphrag_data_factory.deterministic import canonical_json_bytes, deterministic_id
from graphrag_data_factory.models import (
    ApprovalRecord,
    DatasetManifest,
    OrderRecord,
    PackageRecord,
    RefundRecord,
    UserRecord,
)
from graphrag_data_factory.validator import DatasetValidator

SCHEMA_VERSION = "synthetic-business-api-v1"
PRIVILEGED_ROLES = frozenset({"customer_service", "supervisor", "tenant_admin"})


class SimulatorSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    environment: Literal["development", "test"]
    enabled: Literal[True] = True
    fault_control_enabled: bool = False
    control_secret: str | None = Field(default=None, min_length=16)

    @model_validator(mode="after")
    def require_control_secret(self) -> SimulatorSettings:
        if self.fault_control_enabled and self.control_secret is None:
            raise ValueError("fault control requires an explicit control secret")
        return self


class DraftRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    idempotency_key: str = Field(min_length=8, max_length=200)
    masked_address: str | None = Field(default=None, min_length=3, max_length=500)


class RefundQuoteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    order_id: str
    amount: Decimal = Field(gt=Decimal("0"), decimal_places=2)


class RefundDraftRequest(RefundQuoteRequest):
    idempotency_key: str = Field(min_length=8, max_length=200)


class ApprovalCallbackRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    decision: Literal["approved", "rejected"]
    idempotency_key: str = Field(min_length=8, max_length=200)


class FaultControlRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    dependency: Literal["order", "logistics", "refund", "approval"]
    mode: Literal["off", "error", "timeout"]


class Identity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: str
    user_id: str
    role: str


class SimulatorError(RuntimeError):
    def __init__(
        self,
        status_code: int,
        error_code: str,
        message: str,
        *,
        retryable: bool = False,
        scenario_id: str = "simulator-policy",
    ) -> None:
        self.status_code = status_code
        self.error_code = error_code
        self.message = message
        self.retryable = retryable
        self.scenario_id = scenario_id
        super().__init__(message)


class SyntheticBusinessStore:
    """Read-only facts plus isolated draft/idempotency/fault state."""

    def __init__(
        self,
        *,
        manifest: DatasetManifest,
        users: tuple[UserRecord, ...],
        orders: tuple[OrderRecord, ...],
        packages: tuple[PackageRecord, ...],
        refunds: tuple[RefundRecord, ...],
        approvals: tuple[ApprovalRecord, ...],
    ) -> None:
        self.manifest = manifest
        self.users = {item.user_id: item for item in users}
        self.orders = {item.order_id: item for item in orders}
        self.packages = {item.package_id: item for item in packages}
        self.refunds = {item.refund_id: item for item in refunds}
        self.approvals = {item.approval_id: item for item in approvals}
        self.idempotency: dict[tuple[str, str, str], tuple[str, dict[str, Any]]] = {}
        self.fault_modes: dict[str, Literal["error", "timeout"]] = {}
        self.audit: list[dict[str, str]] = []

    @classmethod
    def from_dataset(cls, dataset_dir: Path) -> SyntheticBusinessStore:
        root = dataset_dir.resolve()
        manifest = DatasetValidator().validate(root)
        return cls(
            manifest=manifest,
            users=_load_jsonl(root / "users.jsonl", UserRecord),
            orders=_load_jsonl(root / "orders.jsonl", OrderRecord),
            packages=_load_jsonl(root / "packages.jsonl", PackageRecord),
            refunds=_load_jsonl(root / "refunds.jsonl", RefundRecord),
            approvals=_load_jsonl(root / "approvals.jsonl", ApprovalRecord),
        )

    def identity(self, tenant_id: str, user_id: str) -> Identity:
        user = self.users.get(user_id)
        if user is None or user.tenant_id != tenant_id:
            raise SimulatorError(401, "invalid_synthetic_identity", "测试身份无效")
        return Identity(tenant_id=tenant_id, user_id=user_id, role=user.role)

    def require_order(self, identity: Identity, order_id: str) -> OrderRecord:
        order = self.orders.get(order_id)
        if order is None or order.tenant_id != identity.tenant_id:
            raise SimulatorError(404, "resource_not_found", "合成资源不存在")
        if order.owner_user_id != identity.user_id and identity.role not in PRIVILEGED_ROLES:
            raise SimulatorError(403, "forbidden", "无权访问该合成订单")
        return order

    def idempotent(
        self,
        identity: Identity,
        operation: str,
        key: str,
        payload: dict[str, Any],
        result: dict[str, Any],
    ) -> dict[str, Any]:
        identity_key = (identity.tenant_id, operation, key)
        payload_hash = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        previous = self.idempotency.get(identity_key)
        if previous is not None:
            if previous[0] != payload_hash:
                raise SimulatorError(
                    409,
                    "idempotency_payload_conflict",
                    "同一幂等键对应不同请求",
                )
            return previous[1]
        self.idempotency[identity_key] = (payload_hash, result)
        return result


def create_simulator_app(dataset_dir: Path, settings: SimulatorSettings) -> FastAPI:
    """Build a test-only FastAPI app; settings cannot represent production."""

    store = SyntheticBusinessStore.from_dataset(dataset_dir)
    app = FastAPI(title="Synthetic Business Simulator", version="1.0.0")
    app.state.store = store
    app.state.settings = settings

    @app.exception_handler(SimulatorError)
    async def simulator_error(request: Request, exc: SimulatorError) -> JSONResponse:
        trace_id = request.headers.get("x-trace-id", "missing-test-trace")
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error_code": exc.error_code,
                "message": exc.message,
                "retryable": exc.retryable,
                "trace_id": trace_id,
                "source": "fake",
                "is_synthetic": True,
                "scenario_id": exc.scenario_id,
            },
        )

    @app.exception_handler(RequestValidationError)
    async def request_validation_error(request: Request, _: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error_code": "invalid_request",
                "message": "合成模拟请求不符合契约",
                "retryable": False,
                "trace_id": request.headers.get("x-trace-id", "missing-test-trace"),
                "source": "fake",
                "is_synthetic": True,
                "scenario_id": "simulator-request-validation",
            },
        )

    def context(
        x_synthetic_tenant_id: str = Header(),
        x_synthetic_user_id: str = Header(),
        x_trace_id: str = Header(min_length=8, max_length=200),
    ) -> tuple[Identity, str]:
        return store.identity(x_synthetic_tenant_id, x_synthetic_user_id), x_trace_id

    @app.get("/synthetic/v1/health")
    async def health() -> dict[str, Any]:
        return {
            "status": "ready",
            "source": "fake",
            "is_synthetic": True,
            "dataset_id": store.manifest.dataset_id,
        }

    @app.get("/synthetic/v1/orders/{order_id}")
    async def query_order(
        order_id: str,
        x_synthetic_tenant_id: str = Header(),
        x_synthetic_user_id: str = Header(),
        x_trace_id: str = Header(min_length=8, max_length=200),
    ) -> dict[str, Any]:
        identity, trace_id = context(x_synthetic_tenant_id, x_synthetic_user_id, x_trace_id)
        _check_fault(store, "order")
        order = store.require_order(identity, order_id)
        return _success(order.scenario_id, trace_id, order.model_dump(mode="json"))

    @app.get("/synthetic/v1/orders/{order_id}/logistics")
    async def query_logistics(
        order_id: str,
        x_synthetic_tenant_id: str = Header(),
        x_synthetic_user_id: str = Header(),
        x_trace_id: str = Header(min_length=8, max_length=200),
    ) -> dict[str, Any]:
        identity, trace_id = context(x_synthetic_tenant_id, x_synthetic_user_id, x_trace_id)
        _check_fault(store, "logistics")
        order = store.require_order(identity, order_id)
        packages = [
            item.model_dump(mode="json")
            for item in store.packages.values()
            if item.order_id == order.order_id
        ]
        return _success(order.scenario_id, trace_id, {"packages": packages})

    @app.post("/synthetic/v1/orders/{order_id}/address-drafts")
    async def address_draft(
        order_id: str,
        body: DraftRequest,
        x_synthetic_tenant_id: str = Header(),
        x_synthetic_user_id: str = Header(),
        x_trace_id: str = Header(min_length=8, max_length=200),
    ) -> dict[str, Any]:
        identity, trace_id = context(x_synthetic_tenant_id, x_synthetic_user_id, x_trace_id)
        _check_fault(store, "order")
        order = store.require_order(identity, order_id)
        if body.masked_address is None:
            raise SimulatorError(422, "masked_address_required", "缺少脱敏地址")
        if order.status not in {"created", "paid", "allocated"}:
            raise SimulatorError(409, "order_state_conflict", "当前状态不允许修改地址")
        payload = {"order_id": order_id, "masked_address": body.masked_address}
        draft = _draft(store, identity, "update_address", body.idempotency_key, payload)
        result = store.idempotent(identity, "address_draft", body.idempotency_key, payload, draft)
        store.audit.append(_audit(identity, trace_id, "address_draft", "accepted"))
        return _success(order.scenario_id, trace_id, result)

    @app.post("/synthetic/v1/orders/{order_id}/urge-drafts")
    async def urge_draft(
        order_id: str,
        body: DraftRequest,
        x_synthetic_tenant_id: str = Header(),
        x_synthetic_user_id: str = Header(),
        x_trace_id: str = Header(min_length=8, max_length=200),
    ) -> dict[str, Any]:
        identity, trace_id = context(x_synthetic_tenant_id, x_synthetic_user_id, x_trace_id)
        _check_fault(store, "logistics")
        order = store.require_order(identity, order_id)
        payload = {"order_id": order_id}
        draft = _draft(store, identity, "urge_delivery", body.idempotency_key, payload)
        result = store.idempotent(identity, "urge_draft", body.idempotency_key, payload, draft)
        store.audit.append(_audit(identity, trace_id, "urge_draft", "accepted"))
        return _success(order.scenario_id, trace_id, result)

    @app.post("/synthetic/v1/refunds/quote")
    async def refund_quote(
        body: RefundQuoteRequest,
        x_synthetic_tenant_id: str = Header(),
        x_synthetic_user_id: str = Header(),
        x_trace_id: str = Header(min_length=8, max_length=200),
    ) -> dict[str, Any]:
        identity, trace_id = context(x_synthetic_tenant_id, x_synthetic_user_id, x_trace_id)
        _check_fault(store, "refund")
        order = store.require_order(identity, body.order_id)
        maximum = order.total - order.refunded_amount
        if body.amount > maximum:
            raise SimulatorError(422, "refund_amount_exceeds_maximum", "退款金额超过上限")
        quote = {
            "quote_id": deterministic_id(store.manifest.dataset_id, "quote", body.order_id),
            "order_id": body.order_id,
            "amount": str(body.amount),
            "max_refundable_amount": str(maximum),
            "requires_approval": body.amount >= Decimal("100.00") or order.high_value,
        }
        return _success(order.scenario_id, trace_id, quote)

    @app.post("/synthetic/v1/refunds/drafts")
    async def refund_draft(
        body: RefundDraftRequest,
        x_synthetic_tenant_id: str = Header(),
        x_synthetic_user_id: str = Header(),
        x_trace_id: str = Header(min_length=8, max_length=200),
    ) -> dict[str, Any]:
        identity, trace_id = context(x_synthetic_tenant_id, x_synthetic_user_id, x_trace_id)
        _check_fault(store, "refund")
        order = store.require_order(identity, body.order_id)
        maximum = order.total - order.refunded_amount
        if body.amount > maximum:
            raise SimulatorError(422, "refund_amount_exceeds_maximum", "退款金额超过上限")
        payload = {"order_id": body.order_id, "amount": str(body.amount)}
        draft = _draft(store, identity, "refund", body.idempotency_key, payload)
        draft["status"] = "pending_approval"
        draft["risk_level"] = "critical"
        result = store.idempotent(identity, "refund_draft", body.idempotency_key, payload, draft)
        store.audit.append(_audit(identity, trace_id, "refund_draft", "accepted"))
        return _success(order.scenario_id, trace_id, result)

    @app.post("/synthetic/v1/approvals/{approval_id}/callback")
    async def approval_callback(
        approval_id: str,
        body: ApprovalCallbackRequest,
        x_synthetic_tenant_id: str = Header(),
        x_synthetic_user_id: str = Header(),
        x_trace_id: str = Header(min_length=8, max_length=200),
    ) -> dict[str, Any]:
        identity, trace_id = context(x_synthetic_tenant_id, x_synthetic_user_id, x_trace_id)
        _check_fault(store, "approval")
        approval = store.approvals.get(approval_id)
        if approval is None or approval.tenant_id != identity.tenant_id:
            raise SimulatorError(404, "resource_not_found", "合成资源不存在")
        if identity.role != "supervisor":
            raise SimulatorError(403, "approval_role_required", "只有测试主管可以审批")
        if approval.status != "pending":
            raise SimulatorError(409, "approval_state_conflict", "当前审批状态不可回调")
        payload = {"approval_id": approval_id, "decision": body.decision}
        result = store.idempotent(
            identity,
            "approval_callback",
            body.idempotency_key,
            payload,
            {"approval_id": approval_id, "status": body.decision},
        )
        store.audit.append(_audit(identity, trace_id, "approval_callback", body.decision))
        return _success(approval.scenario_id, trace_id, result)

    @app.post("/synthetic/v1/control/faults")
    async def control_fault(
        body: FaultControlRequest,
        x_synthetic_control_secret: str = Header(),
    ) -> dict[str, Any]:
        if (
            not settings.fault_control_enabled
            or settings.control_secret is None
            or not hmac.compare_digest(x_synthetic_control_secret, settings.control_secret)
        ):
            raise SimulatorError(403, "fault_control_forbidden", "故障控制未授权")
        if body.mode == "off":
            store.fault_modes.pop(body.dependency, None)
        else:
            store.fault_modes[body.dependency] = body.mode
        return {
            "source": "fake",
            "is_synthetic": True,
            "dependency": body.dependency,
            "mode": body.mode,
        }

    return app


def _load_jsonl(path: Path, model: type[BaseModel]) -> tuple[Any, ...]:
    return tuple(
        model.model_validate(json.loads(line))
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    )


def _success(scenario_id: str, trace_id: str, data: Any) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "fake",
        "is_synthetic": True,
        "scenario_id": scenario_id,
        "trace_id": trace_id,
        "data": data,
    }


def _draft(
    store: SyntheticBusinessStore,
    identity: Identity,
    action: str,
    idempotency_key: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "draft_id": deterministic_id(
            store.manifest.dataset_id,
            f"{identity.tenant_id}-{action}",
            idempotency_key,
        ),
        "tenant_id": identity.tenant_id,
        "user_id": identity.user_id,
        "action_type": action,
        "idempotency_key": idempotency_key,
        "payload": payload,
        "risk_level": "low_write",
        "status": "draft",
        "expires_at": (store.manifest.deterministic_created_at + timedelta(minutes=30)).isoformat(),
        "source": "fake",
        "is_synthetic": True,
    }


def _check_fault(store: SyntheticBusinessStore, dependency: str) -> None:
    mode = store.fault_modes.get(dependency)
    if mode == "timeout":
        raise SimulatorError(
            504,
            "synthetic_dependency_timeout",
            "受控合成超时",
            retryable=True,
            scenario_id=f"fault-{dependency}-timeout",
        )
    if mode == "error":
        raise SimulatorError(
            503,
            "synthetic_dependency_error",
            "受控合成依赖错误",
            retryable=True,
            scenario_id=f"fault-{dependency}-error",
        )


def _audit(identity: Identity, trace_id: str, operation: str, outcome: str) -> dict[str, str]:
    return {
        "tenant_id": identity.tenant_id,
        "user_id": identity.user_id,
        "trace_id": trace_id,
        "operation": operation,
        "outcome": outcome,
        "source": "fake",
    }


__all__ = [
    "SCHEMA_VERSION",
    "SimulatorSettings",
    "SyntheticBusinessStore",
    "create_simulator_app",
]
