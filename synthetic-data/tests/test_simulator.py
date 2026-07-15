from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from graphrag_data_factory.exporter import DatasetExporter
from graphrag_data_factory.factory import DatasetBundle
from graphrag_data_factory.models import (
    ApprovalRecord,
    DatasetProfile,
    OrderRecord,
    PackageRecord,
    RefundRecord,
    UserRecord,
)
from graphrag_data_factory.simulator import SimulatorSettings, create_simulator_app


def _headers(order: OrderRecord) -> dict[str, str]:
    return {
        "X-Synthetic-Tenant-Id": order.tenant_id,
        "X-Synthetic-User-Id": order.owner_user_id,
        "X-Trace-Id": "synthetic-trace-0001",
    }


def _identity_headers(tenant_id: str, user_id: str) -> dict[str, str]:
    return {
        "X-Synthetic-Tenant-Id": tenant_id,
        "X-Synthetic-User-Id": user_id,
        "X-Trace-Id": "synthetic-trace-0002",
    }


@pytest.mark.contract
def test_simulator_cannot_be_configured_for_production() -> None:
    with pytest.raises(ValidationError, match="development"):
        SimulatorSettings.model_validate({"environment": "production", "enabled": True})
    with pytest.raises(ValidationError, match="control secret"):
        SimulatorSettings(environment="test", fault_control_enabled=True)


@pytest.mark.contract
def test_simulator_queries_enforce_identity_tenant_and_fake_envelope(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
    ci_bundle: DatasetBundle,
) -> None:
    _, profile_path = ci_profile
    target = tmp_path / "dataset"
    DatasetExporter().export(ci_bundle, profile_path=profile_path, target=target)
    app = create_simulator_app(target, SimulatorSettings(environment="test"))
    order = next(item for item in ci_bundle.records["order"] if isinstance(item, OrderRecord))
    cross_user = next(
        item
        for item in ci_bundle.records["user"]
        if isinstance(item, UserRecord) and item.tenant_id != order.tenant_id
    )
    with TestClient(app) as client:
        response = client.get(f"/synthetic/v1/orders/{order.order_id}", headers=_headers(order))
        assert response.status_code == 200
        assert response.json()["source"] == "fake"
        assert response.json()["is_synthetic"] is True
        invalid = client.get(f"/synthetic/v1/orders/{order.order_id}")
        assert invalid.status_code == 422
        assert invalid.json()["error_code"] == "invalid_request"
        forbidden_headers = _headers(order) | {
            "X-Synthetic-Tenant-Id": cross_user.tenant_id,
            "X-Synthetic-User-Id": cross_user.user_id,
        }
        response = client.get(f"/synthetic/v1/orders/{order.order_id}", headers=forbidden_headers)
        assert response.status_code == 404
        assert response.json()["error_code"] == "resource_not_found"


@pytest.mark.contract
def test_simulator_drafts_are_idempotent_and_conflicts_are_rejected(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
    ci_bundle: DatasetBundle,
) -> None:
    _, profile_path = ci_profile
    target = tmp_path / "dataset"
    DatasetExporter().export(ci_bundle, profile_path=profile_path, target=target)
    app = create_simulator_app(target, SimulatorSettings(environment="test"))
    order = next(
        item
        for item in ci_bundle.records["order"]
        if isinstance(item, OrderRecord) and item.status in {"created", "paid", "allocated"}
    )
    path = f"/synthetic/v1/orders/{order.order_id}/address-drafts"
    body = {
        "idempotency_key": "synthetic-address-key-0001",
        "masked_address": "测试省/演示市/样例区/***路***号",
    }
    with TestClient(app) as client:
        first = client.post(path, headers=_headers(order), json=body)
        replay = client.post(path, headers=_headers(order), json=body)
        conflict = client.post(
            path,
            headers=_headers(order),
            json=body | {"masked_address": "测试省/演示市/样例区/***街***号"},
        )
    assert first.status_code == 200
    assert replay.json()["data"] == first.json()["data"]
    assert conflict.status_code == 409
    assert conflict.json()["error_code"] == "idempotency_payload_conflict"


@pytest.mark.fault
def test_fault_control_requires_secret_and_returns_retryable_error(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
    ci_bundle: DatasetBundle,
) -> None:
    _, profile_path = ci_profile
    target = tmp_path / "dataset"
    DatasetExporter().export(ci_bundle, profile_path=profile_path, target=target)
    control_secret = "-".join(("synthetic", "control", "credential", "0001"))
    app = create_simulator_app(
        target,
        SimulatorSettings(
            environment="test",
            fault_control_enabled=True,
            control_secret=control_secret,
        ),
    )
    order = next(item for item in ci_bundle.records["order"] if isinstance(item, OrderRecord))
    with TestClient(app) as client:
        denied = client.post(
            "/synthetic/v1/control/faults",
            headers={"X-Synthetic-Control-Secret": "invalid-control-value"},
            json={"dependency": "order", "mode": "error"},
        )
        enabled = client.post(
            "/synthetic/v1/control/faults",
            headers={"X-Synthetic-Control-Secret": control_secret},
            json={"dependency": "order", "mode": "error"},
        )
        failed = client.get(f"/synthetic/v1/orders/{order.order_id}", headers=_headers(order))
    assert denied.status_code == 403
    assert enabled.status_code == 200
    assert failed.status_code == 503
    assert failed.json()["retryable"] is True
    assert failed.json()["scenario_id"] == "fault-order-error"


@pytest.mark.contract
def test_logistics_refund_and_approval_contracts_use_dataset_truth(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
    ci_bundle: DatasetBundle,
) -> None:
    _, profile_path = ci_profile
    target = tmp_path / "dataset"
    DatasetExporter().export(ci_bundle, profile_path=profile_path, target=target)
    app = create_simulator_app(target, SimulatorSettings(environment="test"))
    package = next(item for item in ci_bundle.records["package"] if isinstance(item, PackageRecord))
    orders = {
        item.order_id: item for item in ci_bundle.records["order"] if isinstance(item, OrderRecord)
    }
    logistics_order = orders[package.order_id]
    approval = next(
        item
        for item in ci_bundle.records["approval"]
        if isinstance(item, ApprovalRecord) and item.status == "pending"
    )
    refund = next(
        item
        for item in ci_bundle.records["refund"]
        if isinstance(item, RefundRecord) and item.refund_id == approval.refund_id
    )
    refund_order = orders[refund.order_id]
    with TestClient(app) as client:
        logistics = client.get(
            f"/synthetic/v1/orders/{logistics_order.order_id}/logistics",
            headers=_headers(logistics_order),
        )
        quote = client.post(
            "/synthetic/v1/refunds/quote",
            headers=_headers(refund_order),
            json={"order_id": refund_order.order_id, "amount": "1.00"},
        )
        draft = client.post(
            "/synthetic/v1/refunds/drafts",
            headers=_headers(refund_order),
            json={
                "order_id": refund_order.order_id,
                "amount": "1.00",
                "idempotency_key": "synthetic-refund-draft-key-0001",
            },
        )
        approved = client.post(
            f"/synthetic/v1/approvals/{approval.approval_id}/callback",
            headers=_identity_headers(approval.tenant_id, approval.approver_user_id),
            json={
                "decision": "approved",
                "idempotency_key": "synthetic-approval-key-0001",
            },
        )
        denied = client.post(
            f"/synthetic/v1/approvals/{approval.approval_id}/callback",
            headers=_headers(refund_order),
            json={
                "decision": "approved",
                "idempotency_key": "synthetic-approval-key-0002",
            },
        )
    assert logistics.status_code == 200
    assert package.package_id in {
        item["package_id"] for item in logistics.json()["data"]["packages"]
    }
    assert quote.status_code == 200
    assert quote.json()["data"]["max_refundable_amount"]
    assert draft.status_code == 200
    assert draft.json()["data"]["status"] == "pending_approval"
    assert approved.status_code == 200
    assert approved.json()["data"]["status"] == "approved"
    assert denied.status_code == 403
    assert denied.json()["error_code"] == "approval_role_required"
