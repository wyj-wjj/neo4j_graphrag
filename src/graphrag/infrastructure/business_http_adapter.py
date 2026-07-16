"""Dev/test-only HTTP adapter for the validated synthetic business simulator."""

from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

import httpx

from graphrag.domain.errors import (
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    DependencyError,
    NotFoundError,
    OperationTimeoutError,
    ValidationError,
)
from graphrag.domain.ids import new_id
from graphrag.domain.models import (
    ActionDraft,
    IdentityContext,
    LogisticsInfo,
    OrderInfo,
    RefundQuote,
    RiskLevel,
    SourceKind,
    StrictModel,
)
from graphrag.domain.ports import KnowledgeRepositoryPort


class SyntheticEnvelope(StrictModel):
    schema_version: Literal["synthetic-business-api-v1"]
    source: Literal["fake"]
    is_synthetic: Literal[True]
    scenario_id: str
    trace_id: str
    data: dict[str, Any]


class SyntheticRecord(StrictModel):
    dataset_id: str
    dataset_version: Literal["synthetic-commerce-v1"]
    scenario_id: str
    tenant_id: str
    source: Literal["fake"]
    is_synthetic: Literal[True]
    schema_version: Literal["synthetic-record-v1"]


class OrderLinePayload(StrictModel):
    line_id: str
    product_id: str
    sku: str
    quantity: int
    unit_price: Decimal
    subtotal: Decimal
    returnable: bool


class StatePointPayload(StrictModel):
    state: str
    occurred_at: datetime


class OrderPayload(SyntheticRecord):
    order_id: str
    owner_user_id: str
    status: str
    currency: Literal["CNY"]
    lines: tuple[OrderLinePayload, ...]
    subtotal: Decimal
    discount: Decimal
    shipping_fee: Decimal
    tax: Decimal
    total: Decimal
    refunded_amount: Decimal
    address_snapshot: str
    high_value: bool
    status_history: tuple[StatePointPayload, ...]


class TrackingPointPayload(StatePointPayload):
    location: str
    detail: str


class PackagePayload(SyntheticRecord):
    package_id: str
    order_id: str
    tracking_number: str
    carrier: str
    status: str
    line_quantities: dict[str, int]
    tracking_events: tuple[TrackingPointPayload, ...]


class LogisticsPayload(StrictModel):
    packages: tuple[PackagePayload, ...]


class RefundQuotePayload(StrictModel):
    quote_id: str
    order_id: str
    amount: Decimal
    max_refundable_amount: Decimal
    requires_approval: bool


class DraftPayload(StrictModel):
    draft_id: str
    tenant_id: str
    user_id: str
    action_type: Literal["update_address", "urge_delivery", "refund", "ticket"]
    idempotency_key: str
    payload: dict[str, Any]
    risk_level: RiskLevel
    status: Literal["draft", "pending_approval", "expired", "cancelled"]
    expires_at: datetime
    source: Literal["fake"]
    is_synthetic: Literal[True]


class SyntheticBusinessHTTPAdapter:
    """Map the explicitly synthetic HTTP contract to stable business Ports."""

    def __init__(
        self,
        *,
        base_url: str,
        repository: KnowledgeRepositoryPort,
        timeout_seconds: float,
        read_max_attempts: int,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.repository = repository
        self.timeout_seconds = timeout_seconds
        self.read_max_attempts = read_max_attempts
        self.client = client or httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=httpx.Timeout(timeout_seconds),
            follow_redirects=False,
        )
        self._owns_client = client is None

    async def query(self, identity: IdentityContext, order_id: str) -> OrderInfo:
        envelope = await self._request(
            "GET",
            f"/synthetic/v1/orders/{self._safe_identifier(order_id)}",
            identity=identity,
            retry_read=True,
        )
        payload = OrderPayload.model_validate(envelope.data)
        self._validate_tenant(identity, payload.tenant_id)
        return OrderInfo(
            order_id=payload.order_id,
            owner_user_id=payload.owner_user_id,
            status=payload.status,
            items=tuple(f"{line.sku} x{line.quantity}" for line in payload.lines),
            masked_address=payload.address_snapshot,
            source=SourceKind.FAKE,
        )

    async def query_logistics(self, identity: IdentityContext, order_id: str) -> LogisticsInfo:
        envelope = await self._request(
            "GET",
            f"/synthetic/v1/orders/{self._safe_identifier(order_id)}/logistics",
            identity=identity,
            retry_read=True,
        )
        payload = LogisticsPayload.model_validate(envelope.data)
        if any(package.tenant_id != identity.tenant_id for package in payload.packages):
            raise AuthorizationError("业务响应租户不匹配")
        events = tuple(
            event.detail for package in payload.packages for event in package.tracking_events
        )
        statuses = sorted({package.status for package in payload.packages})
        return LogisticsInfo(
            order_id=order_id,
            status=",".join(statuses) if statuses else "not_shipped",
            events=events,
            source=SourceKind.FAKE,
        )

    async def create_address_draft(
        self,
        identity: IdentityContext,
        order_id: str,
        masked_address: str,
        idempotency_key: str,
    ) -> ActionDraft:
        envelope = await self._request(
            "POST",
            f"/synthetic/v1/orders/{self._safe_identifier(order_id)}/address-drafts",
            identity=identity,
            json={"idempotency_key": idempotency_key, "masked_address": masked_address},
        )
        return await self._save_draft(identity, envelope)

    async def create_urge_draft(
        self, identity: IdentityContext, order_id: str, idempotency_key: str
    ) -> ActionDraft:
        envelope = await self._request(
            "POST",
            f"/synthetic/v1/orders/{self._safe_identifier(order_id)}/urge-drafts",
            identity=identity,
            json={"idempotency_key": idempotency_key},
        )
        return await self._save_draft(identity, envelope)

    async def calculate(self, identity: IdentityContext, order_id: str, amount: str) -> RefundQuote:
        envelope = await self._request(
            "POST",
            "/synthetic/v1/refunds/quote",
            identity=identity,
            json={"order_id": order_id, "amount": amount},
        )
        payload = RefundQuotePayload.model_validate(envelope.data)
        if payload.order_id != order_id:
            raise ValidationError("退款试算响应订单不匹配")
        return RefundQuote(
            order_id=payload.order_id,
            amount=payload.amount,
            requires_approval=payload.requires_approval,
            source=SourceKind.FAKE,
        )

    async def create_draft(
        self,
        identity: IdentityContext,
        quote: RefundQuote,
        idempotency_key: str,
    ) -> ActionDraft:
        envelope = await self._request(
            "POST",
            "/synthetic/v1/refunds/drafts",
            identity=identity,
            json={
                "order_id": quote.order_id,
                "amount": str(quote.amount),
                "idempotency_key": idempotency_key,
            },
        )
        return await self._save_draft(identity, envelope)

    async def health(self) -> dict[str, str]:
        try:
            response = await self.client.get("/synthetic/v1/health")
            response.raise_for_status()
        except Exception as exc:
            raise DependencyError("synthetic_business", "合成业务模拟器不可用") from exc
        payload = response.json()
        if payload.get("source") != "fake" or payload.get("is_synthetic") is not True:
            raise DependencyError(
                "synthetic_business",
                "业务模拟器身份标记无效",
                retryable=False,
            )
        return {"status": "ready", "source": "fake"}

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def _save_draft(
        self, identity: IdentityContext, envelope: SyntheticEnvelope
    ) -> ActionDraft:
        payload = DraftPayload.model_validate(envelope.data)
        self._validate_tenant(identity, payload.tenant_id)
        if payload.user_id != identity.user_id:
            raise AuthorizationError("业务响应用户不匹配")
        draft = ActionDraft(
            draft_id=payload.draft_id,
            tenant_id=payload.tenant_id,
            user_id=payload.user_id,
            action_type=payload.action_type,
            idempotency_key=payload.idempotency_key,
            payload=payload.payload,
            risk_level=payload.risk_level,
            status=payload.status,
            source=SourceKind.FAKE,
            expires_at=payload.expires_at,
        )
        return await self.repository.save_draft(draft)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        identity: IdentityContext,
        json: dict[str, Any] | None = None,
        retry_read: bool = False,
    ) -> SyntheticEnvelope:
        trace_id = new_id()
        headers = {
            "x-synthetic-tenant-id": identity.tenant_id,
            "x-synthetic-user-id": identity.user_id,
            "x-trace-id": trace_id,
        }
        attempts = self.read_max_attempts if retry_read else 1
        for attempt in range(attempts):
            try:
                response = await self.client.request(method, path, headers=headers, json=json)
            except httpx.TimeoutException as exc:
                if attempt + 1 == attempts:
                    raise OperationTimeoutError("synthetic_business") from exc
                await asyncio.sleep(0)
                continue
            except httpx.HTTPError as exc:
                if attempt + 1 == attempts:
                    raise DependencyError("synthetic_business", "合成业务 HTTP 请求失败") from exc
                await asyncio.sleep(0)
                continue
            if response.is_success:
                envelope = SyntheticEnvelope.model_validate(response.json())
                if envelope.trace_id != trace_id:
                    raise ValidationError("业务响应 Trace ID 不匹配")
                return envelope
            if attempt + 1 < attempts and self._response_retryable(response):
                await asyncio.sleep(0)
                continue
            self._raise_response_error(response)
        raise AssertionError("unreachable synthetic HTTP retry state")

    @staticmethod
    def _response_retryable(response: httpx.Response) -> bool:
        if response.status_code < 500:
            return False
        try:
            return response.json().get("retryable") is True
        except ValueError:
            return True

    @staticmethod
    def _raise_response_error(response: httpx.Response) -> None:
        try:
            payload = response.json()
            message = str(payload.get("message", "业务服务请求失败"))[:500]
            retryable = payload.get("retryable") is True
        except ValueError:
            message = "业务服务返回非结构化错误"
            retryable = response.status_code >= 500
        if response.status_code == 401:
            raise AuthenticationError(message)
        if response.status_code == 403:
            raise AuthorizationError(message)
        if response.status_code == 404:
            raise NotFoundError(message)
        if response.status_code == 409:
            raise ConflictError(message)
        if response.status_code == 422:
            raise ValidationError(message)
        raise DependencyError("synthetic_business", message, retryable=retryable)

    @staticmethod
    def _safe_identifier(value: str) -> str:
        cleaned = value.strip()
        if not cleaned or any(character in cleaned for character in ("/", "\\", "?", "#")):
            raise ValidationError("非法业务资源标识")
        return cleaned

    @staticmethod
    def _validate_tenant(identity: IdentityContext, tenant_id: str) -> None:
        if tenant_id != identity.tenant_id:
            raise AuthorizationError("业务响应租户不匹配")


__all__ = ["SyntheticBusinessHTTPAdapter"]
