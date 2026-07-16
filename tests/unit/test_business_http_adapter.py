from __future__ import annotations

from typing import Any

import httpx
import pytest

from graphrag.domain.errors import AuthorizationError, DependencyError
from graphrag.domain.models import IdentityContext
from graphrag.infrastructure.business_http_adapter import SyntheticBusinessHTTPAdapter
from graphrag.infrastructure.memory import InMemoryKnowledgeRepository


def _envelope(request: httpx.Request, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "synthetic-business-api-v1",
        "source": "fake",
        "is_synthetic": True,
        "scenario_id": "scenario-order-000001",
        "trace_id": request.headers["x-trace-id"],
        "data": data,
    }


def _order(*, tenant_id: str = "synthetic-tenant-001") -> dict[str, Any]:
    return {
        "dataset_id": "commerce-v1-ci-small-seed-20260714",
        "dataset_version": "synthetic-commerce-v1",
        "scenario_id": "scenario-order-000001",
        "tenant_id": tenant_id,
        "source": "fake",
        "is_synthetic": True,
        "schema_version": "synthetic-record-v1",
        "order_id": "order-1",
        "owner_user_id": "user-1",
        "status": "paid",
        "currency": "CNY",
        "lines": [
            {
                "line_id": "line-1",
                "product_id": "product-1",
                "sku": "SYN-00000001",
                "quantity": 2,
                "unit_price": "10.00",
                "subtotal": "20.00",
                "returnable": True,
            }
        ],
        "subtotal": "20.00",
        "discount": "0.00",
        "shipping_fee": "0.00",
        "tax": "0.00",
        "total": "20.00",
        "refunded_amount": "0.00",
        "address_snapshot": "测试省/演示市/样例区/***路***号",
        "high_value": False,
        "status_history": [{"state": "paid", "occurred_at": "2026-01-01T00:00:00Z"}],
    }


def _identity() -> IdentityContext:
    return IdentityContext(
        tenant_id="synthetic-tenant-001",
        user_id="user-1",
        roles=frozenset({"customer"}),
    )


def _adapter(handler: Any) -> tuple[SyntheticBusinessHTTPAdapter, InMemoryKnowledgeRepository]:
    repository = InMemoryKnowledgeRepository()
    client = httpx.AsyncClient(
        base_url="http://simulator.test",
        transport=httpx.MockTransport(handler),
    )
    return (
        SyntheticBusinessHTTPAdapter(
            base_url="http://simulator.test",
            repository=repository,
            timeout_seconds=1,
            read_max_attempts=2,
            client=client,
        ),
        repository,
    )


@pytest.mark.asyncio
async def test_synthetic_http_adapter_validates_headers_source_and_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["x-synthetic-tenant-id"] == "synthetic-tenant-001"
        assert request.headers["x-synthetic-user-id"] == "user-1"
        assert "customer" not in request.headers.values()
        return httpx.Response(200, json=_envelope(request, _order()))

    adapter, _ = _adapter(handler)
    result = await adapter.query(_identity(), "order-1")

    assert result.order_id == "order-1"
    assert result.items == ("SYN-00000001 x2",)
    await adapter.client.aclose()


@pytest.mark.asyncio
async def test_synthetic_http_adapter_rejects_cross_tenant_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_envelope(request, _order(tenant_id="other")))

    adapter, _ = _adapter(handler)
    with pytest.raises(AuthorizationError, match="租户"):
        await adapter.query(_identity(), "order-1")
    await adapter.client.aclose()


@pytest.mark.asyncio
async def test_synthetic_http_adapter_retries_reads_but_not_writes() -> None:
    calls = 0

    def read_handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                503,
                json={"message": "temporary", "retryable": True},
            )
        return httpx.Response(200, json=_envelope(request, _order()))

    adapter, _ = _adapter(read_handler)
    assert (await adapter.query(_identity(), "order-1")).order_id == "order-1"
    assert calls == 2
    await adapter.client.aclose()

    write_calls = 0

    def write_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal write_calls
        write_calls += 1
        return httpx.Response(503, json={"message": "temporary", "retryable": True})

    adapter, _ = _adapter(write_handler)
    with pytest.raises(DependencyError):
        await adapter.create_urge_draft(_identity(), "order-1", "idempotency-0001")
    assert write_calls == 1
    await adapter.client.aclose()
