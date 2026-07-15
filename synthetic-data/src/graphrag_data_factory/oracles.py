"""Independent business and replay oracles for machine-verifiable truth."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import ClassVar, Literal

from graphrag_data_factory.deterministic import canonical_json_bytes, money, sha256_bytes
from graphrag_data_factory.models import (
    ApprovalRecord,
    EventDeliveryRecord,
    EventRecord,
    OrderRecord,
    RefundRecord,
    UserRecord,
)


@dataclass(frozen=True)
class AccessDecision:
    allowed: bool
    reason_code: Literal[
        "owner_allowed",
        "role_allowed",
        "explicit_deny",
        "cross_tenant",
        "missing_policy",
        "dependency_unavailable",
    ]


class AuthorizationOracle:
    """Fail-closed RBAC/ABAC truth independent from the application AuthorizationPort."""

    ROLE_ACCESS: ClassVar[dict[str, frozenset[str]]] = {
        "knowledge": frozenset({"customer_service", "knowledge_admin", "tenant_admin"}),
        "approval": frozenset({"supervisor", "tenant_admin"}),
        "audit": frozenset({"auditor", "tenant_admin"}),
        "order": frozenset({"customer_service", "supervisor", "tenant_admin"}),
    }

    @classmethod
    def decide(
        cls,
        actor: UserRecord,
        *,
        resource_type: Literal["order", "knowledge", "approval", "audit"],
        resource_tenant_id: str,
        owner_user_id: str | None,
        explicit_deny: bool,
        dependency_available: bool,
        policy_present: bool,
    ) -> AccessDecision:
        if not dependency_available:
            return AccessDecision(False, "dependency_unavailable")
        if actor.tenant_id != resource_tenant_id:
            return AccessDecision(False, "cross_tenant")
        if explicit_deny:
            return AccessDecision(False, "explicit_deny")
        if not policy_present:
            return AccessDecision(False, "missing_policy")
        if owner_user_id is not None and actor.user_id == owner_user_id:
            return AccessDecision(True, "owner_allowed")
        if actor.role in cls.ROLE_ACCESS[resource_type]:
            return AccessDecision(True, "role_allowed")
        return AccessDecision(False, "missing_policy")


@dataclass(frozen=True)
class OrderDecision:
    allowed: bool
    error_code: str | None
    requires_approval: bool
    max_refundable_amount: Decimal


class OrderOracle:
    ADDRESS_MUTABLE_STATES = frozenset({"created", "paid", "allocated"})
    REFUNDABLE_STATES = frozenset({"delivered", "closed", "refund_pending"})

    @staticmethod
    def max_refundable(order: OrderRecord) -> Decimal:
        eligible = sum((line.subtotal for line in order.lines if line.returnable), Decimal("0.00"))
        return money(max(Decimal("0.00"), eligible - order.refunded_amount))

    @classmethod
    def decide(cls, actor: UserRecord, order: OrderRecord, operation: str) -> OrderDecision:
        if actor.tenant_id != order.tenant_id:
            return OrderDecision(False, "cross_tenant", False, money(0))
        can_view = actor.user_id == order.owner_user_id or actor.role in {
            "customer_service",
            "supervisor",
            "tenant_admin",
        }
        if not can_view:
            return OrderDecision(False, "order_forbidden", False, money(0))
        maximum = cls.max_refundable(order)
        if operation == "view":
            return OrderDecision(True, None, False, maximum)
        if operation == "update_address":
            if order.status not in cls.ADDRESS_MUTABLE_STATES:
                return OrderDecision(False, "address_locked_after_shipment", False, maximum)
            return OrderDecision(True, None, order.high_value, maximum)
        if operation == "calculate_refund":
            if order.status not in cls.REFUNDABLE_STATES:
                return OrderDecision(False, "order_not_refundable", False, maximum)
            if maximum <= 0:
                return OrderDecision(False, "no_refundable_lines", False, maximum)
            return OrderDecision(True, None, maximum >= Decimal("100.00"), maximum)
        raise ValueError(f"unknown order operation: {operation}")


@dataclass(frozen=True)
class LogisticsDecision:
    allowed: bool
    error_code: str | None
    escalate: bool


class LogisticsOracle:
    @staticmethod
    def decide(anomaly: str, urge_attempt: int) -> LogisticsDecision:
        if urge_attempt > 2:
            return LogisticsDecision(False, "urge_rate_limited", anomaly in {"lost", "damaged"})
        if anomaly == "timeout":
            return LogisticsDecision(False, "provider_timeout", True)
        if anomaly in {"lost", "damaged", "wrong_hub", "returned", "refused"}:
            return LogisticsDecision(True, None, True)
        if anomaly == "stale":
            return LogisticsDecision(True, None, False)
        return LogisticsDecision(False, "urge_not_required", False)


@dataclass(frozen=True)
class RefundLifecycleDecision:
    status: Literal[
        "blocked",
        "pending",
        "executed",
        "unknown",
        "reconciled",
        "compensated",
        "manual_intervention",
    ]
    side_effect_count: int
    error_code: str | None


class RefundOracle:
    @staticmethod
    def decide(
        refund: RefundRecord,
        approval: ApprovalRecord | None,
        attempt_kind: str,
    ) -> RefundLifecycleDecision:
        if refund.requires_approval:
            if approval is None or approval.status == "pending":
                return RefundLifecycleDecision("pending", 0, "approval_pending")
            if approval.status != "approved":
                return RefundLifecycleDecision("blocked", 0, f"approval_{approval.status}")
        if attempt_kind == "payload_conflict":
            return RefundLifecycleDecision("blocked", 0, "idempotency_conflict")
        if attempt_kind == "idempotent_replay":
            return RefundLifecycleDecision("executed", 0, None)
        if attempt_kind == "unknown_result":
            return RefundLifecycleDecision("unknown", 1, "result_unknown")
        if attempt_kind == "reconciled":
            return RefundLifecycleDecision("reconciled", 0, None)
        if attempt_kind == "compensation":
            return RefundLifecycleDecision("compensated", 0, None)
        if attempt_kind == "compensation_failed":
            return RefundLifecycleDecision("manual_intervention", 0, "compensation_failed")
        return RefundLifecycleDecision("executed", 1, None)


@dataclass(frozen=True)
class ReplaySummary:
    unique_events: int
    duplicate_events: int
    dlq_events: int
    terminal_hash: str


class EventReplayOracle:
    """Inbox/DLQ truth: event_id idempotency and poison/unknown-schema quarantine."""

    @staticmethod
    def replay(
        deliveries: tuple[EventDeliveryRecord, ...],
        events: dict[str, EventRecord],
    ) -> ReplaySummary:
        inbox: dict[str, EventRecord] = {}
        duplicate_count = 0
        dlq_count = 0
        for delivery in sorted(deliveries, key=lambda item: item.delivery_ordinal):
            if delivery.expected_route == "dlq" or delivery.event_id is None:
                dlq_count += 1
                continue
            event = events.get(delivery.event_id)
            if event is None:
                dlq_count += 1
                continue
            if event.event_id in inbox:
                duplicate_count += 1
                continue
            inbox[event.event_id] = event
        ordered = [
            event.model_dump(mode="json")
            for event in sorted(
                inbox.values(),
                key=lambda item: (item.aggregate_type, item.aggregate_id, item.sequence),
            )
        ]
        return ReplaySummary(
            unique_events=len(inbox),
            duplicate_events=duplicate_count,
            dlq_events=dlq_count,
            terminal_hash=sha256_bytes(canonical_json_bytes(ordered)),
        )


__all__ = [
    "AccessDecision",
    "AuthorizationOracle",
    "EventReplayOracle",
    "LogisticsDecision",
    "LogisticsOracle",
    "OrderDecision",
    "OrderOracle",
    "RefundLifecycleDecision",
    "RefundOracle",
    "ReplaySummary",
]
