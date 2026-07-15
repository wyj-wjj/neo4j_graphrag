"""Independent dataset validation; it does not call production business code."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, ClassVar

import yaml
from pydantic import BaseModel, ValidationError

from graphrag_data_factory.constants import (
    COMPATIBILITY,
    ORACLE_VERSION,
    RULES_VERSION,
    SUPPORTED_GENERATOR_VERSIONS,
    TEMPLATE_VERSION,
)
from graphrag_data_factory.deterministic import canonical_json_bytes, sha256_bytes, sha256_file
from graphrag_data_factory.models import (
    AccessDecisionRecord,
    ApprovalRecord,
    ConversationRecord,
    DatasetManifest,
    DatasetProfile,
    DlqRepairRecord,
    EvaluationCaseRecord,
    EventDeliveryRecord,
    EventRecord,
    FaultScheduleRecord,
    GoldenCandidateRecord,
    KnowledgeRecord,
    LogisticsOracleRecord,
    MemoryCaseRecord,
    OrderOracleRecord,
    OrderRecord,
    PackageRecord,
    PhysicalKnowledgeFileRecord,
    ProductRecord,
    RefundLifecycleRecord,
    RefundRecord,
    ReplayExpectationRecord,
    SecurityCaseRecord,
    SyntheticRecord,
    TenantRecord,
    UserRecord,
)
from graphrag_data_factory.oracles import (
    AuthorizationOracle,
    EventReplayOracle,
    LogisticsOracle,
    OrderOracle,
    RefundOracle,
)
from graphrag_data_factory.physical_files import parse_physical_file


class DatasetValidationError(ValueError):
    """Stable validation failure with a machine-readable category."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


class DatasetValidator:
    """Validate checksums, schemas and cross-domain invariants."""

    RECORD_MODELS: ClassVar[dict[str, type[BaseModel]]] = {
        "access_decision": AccessDecisionRecord,
        "tenant": TenantRecord,
        "user": UserRecord,
        "product": ProductRecord,
        "order": OrderRecord,
        "order_oracle": OrderOracleRecord,
        "package": PackageRecord,
        "logistics_oracle": LogisticsOracleRecord,
        "refund": RefundRecord,
        "approval": ApprovalRecord,
        "refund_lifecycle": RefundLifecycleRecord,
        "knowledge": KnowledgeRecord,
        "physical_knowledge_file": PhysicalKnowledgeFileRecord,
        "conversation": ConversationRecord,
        "evaluation_case": EvaluationCaseRecord,
        "memory_case": MemoryCaseRecord,
        "golden_candidate": GoldenCandidateRecord,
        "security_case": SecurityCaseRecord,
        "fault_schedule": FaultScheduleRecord,
        "event": EventRecord,
        "event_delivery": EventDeliveryRecord,
        "replay_expectation": ReplayExpectationRecord,
        "dlq_repair": DlqRepairRecord,
    }
    NON_RECORD_TYPES: ClassVar[frozenset[str]] = frozenset(
        {"profile", "knowledge_source", "validation_report"}
    )
    FORBIDDEN_PATTERNS: ClassVar[tuple[tuple[str, re.Pattern[str]], ...]] = (
        ("leakage", re.compile(r"\b1[3-9]\d{9}\b")),
        ("leakage", re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.(?!invalid\b)[A-Za-z]{2,}")),
        ("leakage", re.compile(r"(?i)\b(?:sk|api)[-_][A-Za-z0-9]{16,}\b")),
        ("leakage", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
    )

    def validate(self, dataset_dir: Path) -> DatasetManifest:
        root = dataset_dir.resolve()
        manifest_path = root / "manifest.json"
        if not manifest_path.is_file():
            raise DatasetValidationError("resource", "manifest.json is missing")
        try:
            manifest = DatasetManifest.model_validate_json(manifest_path.read_bytes())
        except ValidationError as exc:
            raise DatasetValidationError("schema", f"invalid manifest: {exc}") from exc
        if not manifest.is_synthetic or manifest.production_slo_eligible:
            raise DatasetValidationError("resource", "manifest does not enforce synthetic scope")
        self._validate_versions(manifest)

        declared = {item.relative_path for item in manifest.files}
        actual = {
            path.relative_to(root).as_posix()
            for path in root.rglob("*")
            if path.is_file() and path.name != "manifest.json"
        }
        if actual != declared:
            missing = sorted(declared - actual)
            extra = sorted(actual - declared)
            raise DatasetValidationError(
                "resource", f"file inventory mismatch; missing={missing}, extra={extra}"
            )

        loaded: dict[str, list[BaseModel]] = defaultdict(list)
        for entry in manifest.files:
            path = root / entry.relative_path
            if sha256_file(path) != entry.sha256:
                raise DatasetValidationError(
                    "determinism", f"checksum mismatch: {entry.relative_path}"
                )
            if path.stat().st_size != entry.size_bytes:
                raise DatasetValidationError("resource", f"size mismatch: {entry.relative_path}")
            if entry.record_type in self.RECORD_MODELS:
                records = self._load_records(path, self.RECORD_MODELS[entry.record_type])
                if len(records) != entry.records:
                    raise DatasetValidationError(
                        "resource", f"record count mismatch: {entry.relative_path}"
                    )
                loaded[entry.record_type].extend(records)
            elif entry.record_type not in self.NON_RECORD_TYPES:
                raise DatasetValidationError("schema", f"unknown record type: {entry.record_type}")

        self._validate_manifest_counts(manifest, loaded)
        self._validate_profile(root, manifest)
        self._validate_common(manifest, loaded)
        self._validate_relationships(root, loaded)
        self._scan_forbidden_content(root, manifest)
        return manifest

    @staticmethod
    def _validate_versions(manifest: DatasetManifest) -> None:
        expected = {
            "rules_version": RULES_VERSION,
            "template_version": TEMPLATE_VERSION,
            "oracle_version": ORACLE_VERSION,
        }
        mismatched = [
            field for field, value in expected.items() if getattr(manifest, field) != value
        ]
        if manifest.generator_version not in SUPPORTED_GENERATOR_VERSIONS:
            mismatched.insert(0, "generator_version")
        if mismatched or manifest.compatibility != COMPATIBILITY:
            details = ", ".join(mismatched) or "compatibility"
            raise DatasetValidationError(
                "schema", f"dataset version contract is incompatible: {details}"
            )

    @staticmethod
    def _load_records(path: Path, model: type[BaseModel]) -> list[BaseModel]:
        records = []
        with path.open("r", encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, start=1):
                try:
                    payload = json.loads(line)
                    records.append(model.model_validate(payload))
                except (json.JSONDecodeError, ValidationError) as exc:
                    raise DatasetValidationError(
                        "schema", f"{path.name}:{line_number}: {exc}"
                    ) from exc
        return records

    @staticmethod
    def _validate_manifest_counts(
        manifest: DatasetManifest, loaded: dict[str, list[BaseModel]]
    ) -> None:
        observed = {record_type: len(records) for record_type, records in loaded.items()}
        for record_type, expected in manifest.record_counts.items():
            if observed.get(record_type, 0) != expected:
                raise DatasetValidationError(
                    "resource",
                    f"manifest count mismatch for {record_type}: expected={expected}, "
                    f"observed={observed.get(record_type, 0)}",
                )

    @staticmethod
    def _validate_profile(root: Path, manifest: DatasetManifest) -> None:
        profile_path = root / "profile.yaml"
        if not profile_path.is_file() or sha256_file(profile_path) != manifest.profile_sha256:
            raise DatasetValidationError("determinism", "profile hash mismatch")
        try:
            raw_profile = yaml.safe_load(profile_path.read_text(encoding="utf-8"))
            profile = DatasetProfile.model_validate(raw_profile)
        except (yaml.YAMLError, ValidationError) as exc:
            raise DatasetValidationError("schema", f"invalid embedded profile: {exc}") from exc
        if profile.name != manifest.profile:
            raise DatasetValidationError("schema", "embedded profile name mismatch")
        expected = {
            "tenant": profile.counts.tenants,
            "user": profile.counts.users,
            "product": profile.counts.products,
            "order": profile.counts.orders,
            "conversation": profile.counts.conversations,
            "knowledge": profile.counts.knowledge_documents,
            "physical_knowledge_file": profile.counts.physical_files,
            "event": profile.counts.events,
            "evaluation_case": profile.counts.evaluation_cases,
            "security_case": profile.counts.security_cases,
            "memory_case": profile.counts.memory_cases,
            "golden_candidate": profile.counts.golden_candidates,
            "event_delivery": profile.counts.event_deliveries,
            "fault_schedule": profile.counts.fault_schedules,
        }
        for record_type, count in expected.items():
            if manifest.record_counts.get(record_type) != count:
                raise DatasetValidationError(
                    "resource", f"profile count mismatch for {record_type}"
                )

    @staticmethod
    def _validate_common(manifest: DatasetManifest, loaded: dict[str, list[BaseModel]]) -> None:
        ids: set[tuple[str, str]] = set()
        for record_type, records in loaded.items():
            for record in records:
                if not isinstance(record, SyntheticRecord):
                    raise DatasetValidationError(
                        "schema", f"record lacks synthetic metadata: {record_type}"
                    )
                dataset_id = record.dataset_id
                if dataset_id != manifest.dataset_id:
                    raise DatasetValidationError(
                        "relationship", f"foreign dataset record in {record_type}"
                    )
                if record.source != "fake" or not record.is_synthetic:
                    raise DatasetValidationError(
                        "schema", f"unmarked synthetic record in {record_type}"
                    )
                identity = (record_type, DatasetValidator._entity_id(record_type, record))
                if identity in ids:
                    raise DatasetValidationError(
                        "relationship", f"duplicate identity in {record_type}: {identity[1]}"
                    )
                ids.add(identity)

    @staticmethod
    def _entity_id(record_type: str, record: BaseModel) -> str:
        names = {
            "access_decision": "decision_id",
            "tenant": "tenant_id",
            "user": "user_id",
            "product": "product_id",
            "order": "order_id",
            "order_oracle": "oracle_case_id",
            "package": "package_id",
            "logistics_oracle": "oracle_case_id",
            "refund": "refund_id",
            "approval": "approval_id",
            "refund_lifecycle": "lifecycle_id",
            "knowledge": "document_id",
            "physical_knowledge_file": "physical_file_id",
            "conversation": "conversation_id",
            "evaluation_case": "case_id",
            "memory_case": "memory_case_id",
            "golden_candidate": "candidate_id",
            "security_case": "security_case_id",
            "fault_schedule": "fault_id",
            "event": "event_id",
            "event_delivery": "delivery_id",
            "replay_expectation": "replay_id",
            "dlq_repair": "repair_id",
        }
        return str(getattr(record, names[record_type]))

    def _validate_relationships(self, root: Path, loaded: dict[str, list[BaseModel]]) -> None:
        tenants = {item.tenant_id: item for item in self._typed(loaded, "tenant", TenantRecord)}
        users = {item.user_id: item for item in self._typed(loaded, "user", UserRecord)}
        products = {item.product_id: item for item in self._typed(loaded, "product", ProductRecord)}
        orders = {item.order_id: item for item in self._typed(loaded, "order", OrderRecord)}
        packages = {item.package_id: item for item in self._typed(loaded, "package", PackageRecord)}
        refunds = {item.refund_id: item for item in self._typed(loaded, "refund", RefundRecord)}
        approvals = self._typed(loaded, "approval", ApprovalRecord)
        approvals_by_id = {item.approval_id: item for item in approvals}
        knowledge = self._typed(loaded, "knowledge", KnowledgeRecord)
        knowledge_by_id = {item.document_id: item for item in knowledge}
        conversations = {
            item.conversation_id: item
            for item in self._typed(loaded, "conversation", ConversationRecord)
        }
        evaluations = {
            item.case_id: item
            for item in self._typed(loaded, "evaluation_case", EvaluationCaseRecord)
        }

        if not tenants:
            raise DatasetValidationError("relationship", "no tenants")
        for record_type, records in loaded.items():
            for record in records:
                if not isinstance(record, SyntheticRecord):
                    raise DatasetValidationError(
                        "schema", f"record lacks synthetic metadata: {record_type}"
                    )
                if record.tenant_id not in tenants:
                    raise DatasetValidationError("tenant", f"unknown tenant in {record_type}")

        for user in users.values():
            self._same_tenant(user.tenant_id, tenants[user.tenant_id].tenant_id, "user")
        for decision in self._typed(loaded, "access_decision", AccessDecisionRecord):
            actor = self._require(users, decision.actor_user_id, "access actor")
            self._same_tenant(decision.tenant_id, actor.tenant_id, "access actor")
            owner_user_id = None
            if decision.resource_type == "order":
                resource = self._require(orders, decision.resource_id, "access order")
                owner_user_id = resource.owner_user_id
                self._same_tenant(
                    decision.resource_tenant_id, resource.tenant_id, "access order resource"
                )
            elif decision.resource_type == "knowledge":
                resource = self._require(knowledge_by_id, decision.resource_id, "access knowledge")
                self._same_tenant(
                    decision.resource_tenant_id,
                    resource.tenant_id,
                    "access knowledge resource",
                )
            elif decision.resource_type == "approval":
                resource = self._require(approvals_by_id, decision.resource_id, "access approval")
                self._same_tenant(
                    decision.resource_tenant_id,
                    resource.tenant_id,
                    "access approval resource",
                )
            oracle = AuthorizationOracle.decide(
                actor,
                resource_type=decision.resource_type,
                resource_tenant_id=decision.resource_tenant_id,
                owner_user_id=owner_user_id,
                explicit_deny=decision.explicit_deny,
                dependency_available=decision.dependency_available,
                policy_present=decision.relation != "missing_policy",
            )
            if (
                oracle.allowed != decision.expected_allowed
                or oracle.reason_code != decision.reason_code
            ):
                raise DatasetValidationError(
                    "permission", f"access oracle mismatch: {decision.decision_id}"
                )
        for order in orders.values():
            owner = self._require(users, order.owner_user_id, "order owner")
            self._same_tenant(order.tenant_id, owner.tenant_id, "order owner")
            for line in order.lines:
                product = self._require(products, line.product_id, "order product")
                self._same_tenant(order.tenant_id, product.tenant_id, "order product")
            self._monotonic(
                [point.occurred_at for point in order.status_history], f"order {order.order_id}"
            )
        for case in self._typed(loaded, "order_oracle", OrderOracleRecord):
            actor = self._require(users, case.actor_user_id, "order oracle actor")
            order = self._require(orders, case.order_id, "order oracle order")
            decision = OrderOracle.decide(actor, order, case.operation)
            if (
                decision.allowed != case.expected_allowed
                or decision.error_code != case.expected_error_code
                or decision.requires_approval != case.address_requires_approval
                or decision.max_refundable_amount != case.max_refundable_amount
            ):
                raise DatasetValidationError(
                    "permission", f"order oracle mismatch: {case.oracle_case_id}"
                )
        for package in packages.values():
            order = self._require(orders, package.order_id, "package order")
            self._same_tenant(package.tenant_id, order.tenant_id, "package order")
            self._monotonic(
                [point.occurred_at for point in package.tracking_events],
                f"package {package.package_id}",
            )
        for case in self._typed(loaded, "logistics_oracle", LogisticsOracleRecord):
            self._require(packages, case.package_id, "logistics oracle package")
            self._require(users, case.actor_user_id, "logistics oracle actor")
            decision = LogisticsOracle.decide(case.anomaly, case.urge_attempt)
            if (
                decision.allowed != case.expected_allowed
                or decision.error_code != case.expected_error_code
                or decision.escalate != case.expected_escalation
            ):
                raise DatasetValidationError(
                    "relationship", f"logistics oracle mismatch: {case.oracle_case_id}"
                )
        approval_by_refund = {approval.refund_id: approval for approval in approvals}
        for refund in refunds.values():
            order = self._require(orders, refund.order_id, "refund order")
            requester = self._require(users, refund.requested_by, "refund requester")
            self._same_tenant(refund.tenant_id, order.tenant_id, "refund order")
            self._same_tenant(refund.tenant_id, requester.tenant_id, "refund requester")
            if refund.requested_amount > refund.max_refundable_amount:
                raise DatasetValidationError(
                    "amount", f"refund exceeds maximum: {refund.refund_id}"
                )
            if refund.requires_approval and refund.refund_id not in approval_by_refund:
                raise DatasetValidationError(
                    "permission", f"approval missing for refund: {refund.refund_id}"
                )
        for approval in approvals:
            refund = self._require(refunds, approval.refund_id, "approval refund")
            approver = self._require(users, approval.approver_user_id, "approval actor")
            self._same_tenant(approval.tenant_id, refund.tenant_id, "approval refund")
            self._same_tenant(approval.tenant_id, approver.tenant_id, "approval actor")
            if approver.role != "supervisor":
                raise DatasetValidationError("permission", "approval actor is not supervisor")
            if approval.status != "pending" and approval.decided_at is None:
                raise DatasetValidationError("time", "decided approval lacks timestamp")
        for lifecycle in self._typed(loaded, "refund_lifecycle", RefundLifecycleRecord):
            refund = self._require(refunds, lifecycle.refund_id, "lifecycle refund")
            approval = approval_by_refund.get(refund.refund_id)
            if lifecycle.approval_id != (approval.approval_id if approval is not None else None):
                raise DatasetValidationError("relationship", "lifecycle approval mismatch")
            decision = RefundOracle.decide(refund, approval, lifecycle.attempt_kind)
            if (
                decision.status != lifecycle.expected_status
                or decision.side_effect_count != lifecycle.expected_side_effect_count
                or decision.error_code != lifecycle.expected_error_code
            ):
                raise DatasetValidationError(
                    "permission", f"refund lifecycle mismatch: {lifecycle.lifecycle_id}"
                )
        for item in knowledge:
            source = root / item.relative_path
            if not source.is_file() or sha256_file(source) != item.content_sha256:
                raise DatasetValidationError(
                    "relationship", f"knowledge source mismatch: {item.document_id}"
                )
            if item.evidence_anchor_id not in source.read_text(encoding="utf-8"):
                raise DatasetValidationError(
                    "relationship", f"knowledge anchor missing: {item.document_id}"
                )
        for physical in self._typed(loaded, "physical_knowledge_file", PhysicalKnowledgeFileRecord):
            document = self._require(
                knowledge_by_id, physical.document_id, "physical knowledge document"
            )
            self._same_tenant(physical.tenant_id, document.tenant_id, "physical knowledge")
            source = root / physical.relative_path
            if (
                not source.is_file()
                or source.stat().st_size != physical.size_bytes
                or sha256_file(source) != physical.content_sha256
            ):
                raise DatasetValidationError(
                    "relationship", f"physical knowledge mismatch: {physical.physical_file_id}"
                )
            try:
                parsed = parse_physical_file(source, physical.format)
            except Exception as exc:
                if physical.expected_outcome == "reject_corrupt":
                    continue
                raise DatasetValidationError(
                    "schema", f"physical knowledge parse failed: {physical.physical_file_id}"
                ) from exc
            if physical.expected_outcome == "reject_corrupt":
                raise DatasetValidationError(
                    "schema",
                    f"corrupt physical knowledge was accepted: {physical.physical_file_id}",
                )
            if physical.expected_outcome == "parse_success" and (
                physical.evidence_anchor_id is None or physical.evidence_anchor_id not in parsed
            ):
                raise DatasetValidationError(
                    "relationship",
                    f"physical knowledge anchor missing: {physical.physical_file_id}",
                )
            for code, pattern in self.FORBIDDEN_PATTERNS:
                if pattern.search(parsed):
                    raise DatasetValidationError(
                        code,
                        f"forbidden content in physical file: {physical.relative_path}",
                    )
        for conversation in conversations.values():
            user = self._require(users, conversation.user_id, "conversation user")
            self._same_tenant(conversation.tenant_id, user.tenant_id, "conversation user")
            if conversation.order_id is not None:
                order = self._require(orders, conversation.order_id, "conversation order")
                self._same_tenant(conversation.tenant_id, order.tenant_id, "conversation order")
        family_splits: defaultdict[str, set[str]] = defaultdict(set)
        for conversation in conversations.values():
            family_splits[conversation.scenario_family].add(conversation.split)
        leaked = [family for family, splits in family_splits.items() if len(splits) > 1]
        if leaked:
            raise DatasetValidationError("leakage", f"scenario family split leakage: {leaked[:3]}")
        anchors = {item.evidence_anchor_id: item for item in knowledge}
        for case in evaluations.values():
            conversation = self._require(
                conversations, case.conversation_id, "evaluation conversation"
            )
            self._same_tenant(case.tenant_id, conversation.tenant_id, "evaluation conversation")
            if case.split != conversation.split:
                raise DatasetValidationError("leakage", "evaluation split mismatch")
            for anchor_id in case.evidence_anchor_ids:
                anchor = self._require(anchors, anchor_id, "evaluation anchor")
                self._same_tenant(case.tenant_id, anchor.tenant_id, "evaluation anchor")
        for case in self._typed(loaded, "memory_case", MemoryCaseRecord):
            conversation = self._require(conversations, case.conversation_id, "memory conversation")
            self._same_tenant(case.tenant_id, conversation.tenant_id, "memory conversation")
            if case.turns != len(conversation.turns):
                raise DatasetValidationError("relationship", "memory turn count mismatch")
        for candidate in self._typed(loaded, "golden_candidate", GoldenCandidateRecord):
            evaluation = self._require(
                evaluations, candidate.evaluation_case_id, "golden candidate evaluation"
            )
            self._same_tenant(candidate.tenant_id, evaluation.tenant_id, "golden candidate")
            if candidate.review_status != "pending_independent_review":
                raise DatasetValidationError("permission", "unreviewed candidate marked reviewed")
            if candidate.evidence_anchor_ids != evaluation.evidence_anchor_ids:
                raise DatasetValidationError(
                    "relationship", "golden candidate evidence differs from evaluation"
                )
            known_facts = set(conversations) | set(orders)
            if any(fact_id not in known_facts for fact_id in candidate.fact_record_ids):
                raise DatasetValidationError(
                    "relationship", "golden candidate references unknown fact"
                )
            if len(set(candidate.review_checklist)) != len(candidate.review_checklist):
                raise DatasetValidationError(
                    "schema", "golden candidate checklist contains duplicates"
                )

        aggregate_ids = {
            "order": set(orders),
            "package": set(packages),
            "refund": set(refunds),
            "knowledge": {item.document_id for item in knowledge},
            "conversation": set(conversations),
        }
        event_sequences: defaultdict[tuple[str, str], list[int]] = defaultdict(list)
        events = {event.event_id: event for event in self._typed(loaded, "event", EventRecord)}
        for event in events.values():
            if event.aggregate_id not in aggregate_ids[event.aggregate_type]:
                raise DatasetValidationError("relationship", "event references unknown aggregate")
            event_sequences[(event.aggregate_type, event.aggregate_id)].append(event.sequence)
        for key, sequences in event_sequences.items():
            if sequences != list(range(1, len(sequences) + 1)):
                raise DatasetValidationError("replay", f"non-monotonic event sequence: {key}")
        deliveries = tuple(self._typed(loaded, "event_delivery", EventDeliveryRecord))
        for delivery in deliveries:
            if delivery.event_id is not None and delivery.event_id not in events:
                raise DatasetValidationError("replay", "delivery references unknown event")
            if delivery.payload_hash != sha256_bytes(canonical_json_bytes(delivery.payload)):
                raise DatasetValidationError("replay", "delivery payload hash mismatch")
        expectations = self._typed(loaded, "replay_expectation", ReplayExpectationRecord)
        summary = EventReplayOracle.replay(deliveries, events)
        dataset_expectation = next(
            (
                expectation
                for expectation in expectations
                if expectation.aggregate_type == "dataset"
            ),
            None,
        )
        if dataset_expectation is None:
            raise DatasetValidationError("replay", "dataset replay expectation missing")
        if (
            dataset_expectation.expected_unique_events != summary.unique_events
            or dataset_expectation.expected_duplicate_events != summary.duplicate_events
            or dataset_expectation.expected_dlq_events != summary.dlq_events
            or dataset_expectation.expected_terminal_hash != summary.terminal_hash
        ):
            raise DatasetValidationError("replay", "dataset replay expectation mismatch")
        aggregate_expectations = {
            (expectation.aggregate_type, expectation.aggregate_id): expectation
            for expectation in expectations
            if expectation.aggregate_type != "dataset"
        }
        expected_aggregate_keys = {
            (event.aggregate_type, event.aggregate_id) for event in events.values()
        }
        if set(aggregate_expectations) != expected_aggregate_keys:
            raise DatasetValidationError(
                "replay", "aggregate replay expectation inventory mismatch"
            )
        for key, aggregate_expectation in aggregate_expectations.items():
            aggregate_events = {
                event.event_id: event
                for event in events.values()
                if (event.aggregate_type, event.aggregate_id) == key
            }
            aggregate_deliveries = tuple(
                delivery for delivery in deliveries if delivery.event_id in aggregate_events
            )
            aggregate_summary = EventReplayOracle.replay(aggregate_deliveries, aggregate_events)
            if (
                aggregate_expectation.expected_unique_events != aggregate_summary.unique_events
                or aggregate_expectation.expected_duplicate_events
                != aggregate_summary.duplicate_events
                or aggregate_expectation.expected_dlq_events != aggregate_summary.dlq_events
                or aggregate_expectation.expected_terminal_hash != aggregate_summary.terminal_hash
            ):
                raise DatasetValidationError(
                    "replay", f"aggregate replay expectation mismatch: {key}"
                )
        deliveries_by_id = {delivery.delivery_id: delivery for delivery in deliveries}
        replay_ids: set[str] = set()
        audit_ids: set[str] = set()
        for repair in self._typed(loaded, "dlq_repair", DlqRepairRecord):
            delivery = self._require(deliveries_by_id, repair.delivery_id, "DLQ repair delivery")
            operator = self._require(users, repair.operator_user_id, "DLQ repair operator")
            self._same_tenant(repair.tenant_id, delivery.tenant_id, "DLQ repair delivery")
            self._same_tenant(repair.tenant_id, operator.tenant_id, "DLQ repair operator")
            if (
                delivery.expected_route != "dlq"
                or delivery.delivery_kind != repair.failure_kind
                or delivery.payload_hash != repair.original_payload_hash
                or operator.role != "supervisor"
            ):
                raise DatasetValidationError(
                    "replay", f"invalid DLQ repair truth: {repair.repair_id}"
                )
            if repair.replay_id in replay_ids or repair.replay_audit_id in audit_ids:
                raise DatasetValidationError("replay", "duplicate DLQ replay or audit identity")
            replay_ids.add(repair.replay_id)
            audit_ids.add(repair.replay_audit_id)

    @staticmethod
    def _typed(
        loaded: dict[str, list[BaseModel]], record_type: str, model: type[BaseModel]
    ) -> list[Any]:
        records = loaded.get(record_type, [])
        if any(not isinstance(record, model) for record in records):
            raise DatasetValidationError("schema", f"wrong loaded type for {record_type}")
        return records

    @staticmethod
    def _require(mapping: dict[str, Any], key: str, label: str) -> Any:
        try:
            return mapping[key]
        except KeyError as exc:
            raise DatasetValidationError("relationship", f"missing {label}: {key}") from exc

    @staticmethod
    def _same_tenant(left: str, right: str, label: str) -> None:
        if left != right:
            raise DatasetValidationError("tenant", f"cross-tenant reference: {label}")

    @staticmethod
    def _monotonic(values: list[Any], label: str) -> None:
        if values != sorted(values) or len(values) != len(set(values)):
            raise DatasetValidationError("time", f"non-monotonic timeline: {label}")

    def _scan_forbidden_content(self, root: Path, manifest: DatasetManifest) -> None:
        for entry in manifest.files:
            path = root / entry.relative_path
            if entry.media_type not in {
                "application/json",
                "application/x-ndjson",
                "application/yaml",
                "text/markdown",
            }:
                continue
            text = path.read_bytes().decode("utf-8", errors="ignore")
            for code, pattern in self.FORBIDDEN_PATTERNS:
                if pattern.search(text):
                    raise DatasetValidationError(
                        code, f"forbidden content in {entry.relative_path}"
                    )


__all__ = ["DatasetValidationError", "DatasetValidator"]
