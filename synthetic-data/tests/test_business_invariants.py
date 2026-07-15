from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from graphrag_data_factory.exporter import DatasetExporter
from graphrag_data_factory.factory import DatasetBundle
from graphrag_data_factory.models import DatasetProfile, OrderRecord, RefundRecord
from graphrag_data_factory.validator import DatasetValidator


@pytest.mark.invariant
def test_ci_small_contains_exact_declared_primary_counts(
    ci_profile: tuple[DatasetProfile, Path], ci_bundle: DatasetBundle
) -> None:
    profile, _ = ci_profile
    assert ci_bundle.record_count("tenant") == 2
    assert ci_bundle.record_count("user") == profile.counts.users
    assert ci_bundle.record_count("product") == profile.counts.products
    assert ci_bundle.record_count("order") == 500
    assert ci_bundle.record_count("conversation") == 200
    assert ci_bundle.record_count("knowledge") == 50
    assert ci_bundle.record_count("event") == 2000
    assert ci_bundle.record_count("evaluation_case") == profile.counts.evaluation_cases
    assert ci_bundle.record_count("security_case") == profile.counts.security_cases
    assert ci_bundle.record_count("memory_case") == profile.counts.memory_cases
    assert ci_bundle.record_count("golden_candidate") == profile.counts.golden_candidates
    assert ci_bundle.record_count("event_delivery") == profile.counts.event_deliveries
    assert ci_bundle.record_count("physical_knowledge_file") == 27
    assert ci_bundle.record_count("fault_schedule") == profile.counts.fault_schedules


@pytest.mark.invariant
def test_order_amounts_and_timelines_are_exact(ci_bundle: DatasetBundle) -> None:
    orders = ci_bundle.records["order"]
    assert orders
    for raw_order in orders:
        assert isinstance(raw_order, OrderRecord)
        order = raw_order
        assert order.subtotal == sum((line.subtotal for line in order.lines), Decimal("0.00"))
        assert order.total == order.subtotal - order.discount + order.shipping_fee + order.tax
        times = [point.occurred_at for point in order.status_history]
        assert times == sorted(times)
        assert all(value.tzinfo is not None for value in times)


@pytest.mark.invariant
def test_refunds_never_exceed_or_bypass_approval(ci_bundle: DatasetBundle) -> None:
    approvals = {item.refund_id for item in ci_bundle.records["approval"]}
    for raw_refund in ci_bundle.records["refund"]:
        assert isinstance(raw_refund, RefundRecord)
        assert raw_refund.requested_amount <= raw_refund.max_refundable_amount
        if raw_refund.requires_approval:
            assert raw_refund.refund_id in approvals
            assert raw_refund.status != "reconciled"


@pytest.mark.invariant
def test_complete_dataset_passes_independent_validator(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
    ci_bundle: DatasetBundle,
) -> None:
    _, profile_path = ci_profile
    target = tmp_path / "dataset"
    manifest = DatasetExporter().export(ci_bundle, profile_path=profile_path, target=target)
    validated = DatasetValidator().validate(target)
    assert validated == manifest
    assert sum(manifest.record_counts.values()) == sum(
        len(records) for records in ci_bundle.records.values()
    )


@pytest.mark.invariant
def test_dynamic_business_facts_are_not_written_to_knowledge(ci_bundle: DatasetBundle) -> None:
    order_ids = {order.order_id for order in ci_bundle.records["order"]}
    knowledge_text = "\n".join(
        content.decode("utf-8", errors="ignore") for content in ci_bundle.knowledge_files.values()
    )
    assert not any(order_id in knowledge_text for order_id in order_ids)
    assert "动态订单、物流和退款事实必须通过业务工具查询" in knowledge_text


@pytest.mark.invariant
def test_records_use_only_fake_synthetic_markers(ci_bundle: DatasetBundle) -> None:
    for records in ci_bundle.records.values():
        for record in records:
            assert record.source == "fake"
            assert record.is_synthetic is True
            assert record.tenant_id.startswith("synthetic-")
