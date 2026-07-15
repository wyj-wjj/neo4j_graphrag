"""Disk-backed independent validation for multi-million-record datasets."""

from __future__ import annotations

import hashlib
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError

from graphrag_data_factory.deterministic import canonical_json_bytes, sha256_bytes, sha256_file
from graphrag_data_factory.models import (
    AccessDecisionRecord,
    ApprovalRecord,
    ConversationRecord,
    DatasetManifest,
    DlqRepairRecord,
    EvaluationCaseRecord,
    EventDeliveryRecord,
    EventRecord,
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
    SyntheticRecord,
    UserRecord,
)
from graphrag_data_factory.oracles import (
    AuthorizationOracle,
    LogisticsOracle,
    OrderOracle,
    RefundOracle,
)
from graphrag_data_factory.physical_files import parse_physical_file
from graphrag_data_factory.streaming import CHECKPOINT_FILE_NAME
from graphrag_data_factory.validator import DatasetValidationError, DatasetValidator


class StreamingDatasetValidator:
    """Validate all records while keeping relationships in a temporary SQLite index."""

    PROCESS_ORDER = (
        "tenant",
        "user",
        "product",
        "order",
        "package",
        "refund",
        "approval",
        "knowledge",
        "physical_knowledge_file",
        "conversation",
        "evaluation_case",
        "access_decision",
        "order_oracle",
        "logistics_oracle",
        "refund_lifecycle",
        "memory_case",
        "golden_candidate",
        "security_case",
        "fault_schedule",
        "event",
        "event_delivery",
        "replay_expectation",
        "dlq_repair",
    )
    STORED_FACTS = frozenset(
        {
            "tenant",
            "user",
            "product",
            "order",
            "package",
            "refund",
            "approval",
            "knowledge",
            "conversation",
            "evaluation_case",
        }
    )
    FORBIDDEN_PATTERNS = DatasetValidator.FORBIDDEN_PATTERNS

    def __init__(self) -> None:
        self._dataset_id = ""
        self._root = Path(".")
        self._dataset_unique_events = 0
        self._dataset_duplicate_events = 0
        self._dataset_dlq_events = 0
        self._dataset_terminal_hash = ""
        self._dataset_expectations = 0
        self._aggregate_expectations = 0

    def validate(
        self, dataset_dir: Path, *, allow_generation_checkpoint: bool = False
    ) -> DatasetManifest:
        self._dataset_unique_events = 0
        self._dataset_duplicate_events = 0
        self._dataset_dlq_events = 0
        self._dataset_terminal_hash = ""
        self._dataset_expectations = 0
        self._aggregate_expectations = 0
        root = dataset_dir.resolve()
        manifest = self._load_manifest(root)
        self._root = root
        self._dataset_id = manifest.dataset_id
        self._validate_inventory(
            root,
            manifest,
            allow_generation_checkpoint=allow_generation_checkpoint,
        )
        record_entries = [
            entry for entry in manifest.files if entry.record_type in DatasetValidator.RECORD_MODELS
        ]
        if len(record_entries) != len({entry.record_type for entry in record_entries}):
            raise DatasetValidationError(
                "schema", "streaming datasets require one file per modeled record type"
            )
        entries = {entry.record_type: entry for entry in record_entries}
        if any(
            entry.record_type in DatasetValidator.RECORD_MODELS
            and entry.record_type not in self.PROCESS_ORDER
            for entry in manifest.files
        ):
            raise DatasetValidationError("schema", "streaming validator process order incomplete")

        with tempfile.TemporaryDirectory(prefix="synthetic-validation-") as temporary:
            connection = sqlite3.connect(Path(temporary) / "validation.sqlite3")
            try:
                self._create_schema(connection)
                observed: dict[str, int] = {}
                for record_type in self.PROCESS_ORDER:
                    entry = entries.get(record_type)
                    if entry is None:
                        continue
                    if record_type == "replay_expectation":
                        self._finalize_event_truth(connection)
                    observed[record_type] = self._process_records(
                        connection,
                        root / entry.relative_path,
                        record_type,
                        entry.records,
                    )
                    connection.commit()
                self._validate_final_relationships(connection)
            finally:
                connection.close()

        self._validate_file_metadata(root, manifest, observed)
        DatasetValidator._validate_profile(root, manifest)
        self._scan_forbidden_content(root, manifest)
        return manifest

    @staticmethod
    def _load_manifest(root: Path) -> DatasetManifest:
        path = root / "manifest.json"
        if not path.is_file():
            raise DatasetValidationError("resource", "manifest.json is missing")
        try:
            manifest = DatasetManifest.model_validate_json(path.read_bytes())
        except ValidationError as exc:
            raise DatasetValidationError("schema", f"invalid manifest: {exc}") from exc
        if not manifest.is_synthetic or manifest.production_slo_eligible:
            raise DatasetValidationError("resource", "manifest does not enforce synthetic scope")
        DatasetValidator._validate_versions(manifest)
        return manifest

    @staticmethod
    def _validate_inventory(
        root: Path,
        manifest: DatasetManifest,
        *,
        allow_generation_checkpoint: bool,
    ) -> None:
        declared = {entry.relative_path for entry in manifest.files}
        ignored = {CHECKPOINT_FILE_NAME} if allow_generation_checkpoint else set()
        actual = {
            path.relative_to(root).as_posix()
            for path in root.rglob("*")
            if path.is_file()
            and path.name != "manifest.json"
            and path.relative_to(root).as_posix() not in ignored
        }
        if actual != declared:
            raise DatasetValidationError(
                "resource",
                "file inventory mismatch; "
                f"missing={sorted(declared - actual)}, extra={sorted(actual - declared)}",
            )

    @staticmethod
    def _create_schema(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            PRAGMA journal_mode=OFF;
            PRAGMA synchronous=OFF;
            PRAGMA temp_store=FILE;
            CREATE TABLE identities (
                record_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                PRIMARY KEY (record_type, entity_id)
            ) WITHOUT ROWID;
            CREATE TABLE facts (
                record_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                payload BLOB NOT NULL,
                PRIMARY KEY (record_type, entity_id)
            ) WITHOUT ROWID;
            CREATE INDEX facts_tenant ON facts(record_type, tenant_id, entity_id);
            CREATE TABLE anchors (
                anchor_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE scenario_families (
                family TEXT PRIMARY KEY,
                split TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE approvals_required (
                refund_id TEXT PRIMARY KEY
            ) WITHOUT ROWID;
            CREATE TABLE approval_by_refund (
                refund_id TEXT PRIMARY KEY,
                payload BLOB NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE events (
                event_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                aggregate_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                payload BLOB NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX events_aggregate
                ON events(aggregate_type, aggregate_id, sequence);
            CREATE TABLE inbox (
                event_id TEXT PRIMARY KEY
            ) WITHOUT ROWID;
            CREATE TABLE aggregate_stats (
                aggregate_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                unique_events INTEGER NOT NULL DEFAULT 0,
                duplicate_events INTEGER NOT NULL DEFAULT 0,
                last_sequence INTEGER NOT NULL DEFAULT 0,
                terminal_hash TEXT,
                PRIMARY KEY (aggregate_type, aggregate_id)
            ) WITHOUT ROWID;
            CREATE TABLE deliveries (
                delivery_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                event_id TEXT,
                delivery_kind TEXT NOT NULL,
                expected_route TEXT NOT NULL,
                payload_hash TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE dlq_replay_ids (
                replay_id TEXT PRIMARY KEY,
                replay_audit_id TEXT NOT NULL UNIQUE
            ) WITHOUT ROWID;
            """
        )

    def _process_records(
        self,
        connection: sqlite3.Connection,
        path: Path,
        record_type: str,
        expected_count: int,
    ) -> int:
        model = DatasetValidator.RECORD_MODELS[record_type]
        count = 0
        with path.open("r", encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, start=1):
                try:
                    record = model.model_validate_json(line)
                except ValidationError as exc:
                    raise DatasetValidationError(
                        "schema", f"{path.name}:{line_number}: {exc}"
                    ) from exc
                self._validate_common(connection, record_type, record)
                self._validate_record(connection, record_type, record)
                count += 1
        if count != expected_count:
            raise DatasetValidationError(
                "resource",
                f"record count mismatch: {path.name}; expected={expected_count}, observed={count}",
            )
        return count

    def _validate_common(
        self, connection: sqlite3.Connection, record_type: str, record: BaseModel
    ) -> None:
        if not isinstance(record, SyntheticRecord):
            raise DatasetValidationError("schema", f"record lacks metadata: {record_type}")
        if record.dataset_id != self._dataset_id:
            raise DatasetValidationError("relationship", f"foreign dataset in {record_type}")
        if record.source != "fake" or not record.is_synthetic:
            raise DatasetValidationError("schema", f"unmarked synthetic record in {record_type}")
        entity_id = DatasetValidator._entity_id(record_type, record)
        try:
            connection.execute(
                "INSERT INTO identities(record_type, entity_id) VALUES (?, ?)",
                (record_type, entity_id),
            )
        except sqlite3.IntegrityError as exc:
            raise DatasetValidationError(
                "relationship", f"duplicate identity in {record_type}: {entity_id}"
            ) from exc
        if record_type != "tenant":
            self._require_tenant(connection, record.tenant_id, record_type)
        if record_type in self.STORED_FACTS:
            connection.execute(
                "INSERT INTO facts(record_type, entity_id, tenant_id, payload) VALUES (?, ?, ?, ?)",
                (
                    record_type,
                    entity_id,
                    record.tenant_id,
                    canonical_json_bytes(record.model_dump(mode="json")),
                ),
            )

    def _validate_record(
        self, connection: sqlite3.Connection, record_type: str, record: BaseModel
    ) -> None:
        if record_type == "tenant":
            return
        if record_type == "user":
            return
        if record_type == "product":
            return
        if record_type == "order":
            self._validate_order(connection, self._as(record, OrderRecord))
        elif record_type == "package":
            self._validate_package(connection, self._as(record, PackageRecord))
        elif record_type == "refund":
            self._validate_refund(connection, self._as(record, RefundRecord))
        elif record_type == "approval":
            self._validate_approval(connection, self._as(record, ApprovalRecord))
        elif record_type == "knowledge":
            self._validate_knowledge(connection, self._as(record, KnowledgeRecord))
        elif record_type == "physical_knowledge_file":
            self._validate_physical(connection, self._as(record, PhysicalKnowledgeFileRecord))
        elif record_type == "conversation":
            self._validate_conversation(connection, self._as(record, ConversationRecord))
        elif record_type == "evaluation_case":
            self._validate_evaluation(connection, self._as(record, EvaluationCaseRecord))
        elif record_type == "access_decision":
            self._validate_access(connection, self._as(record, AccessDecisionRecord))
        elif record_type == "order_oracle":
            self._validate_order_oracle(connection, self._as(record, OrderOracleRecord))
        elif record_type == "logistics_oracle":
            self._validate_logistics_oracle(connection, self._as(record, LogisticsOracleRecord))
        elif record_type == "refund_lifecycle":
            self._validate_refund_lifecycle(connection, self._as(record, RefundLifecycleRecord))
        elif record_type == "memory_case":
            self._validate_memory(connection, self._as(record, MemoryCaseRecord))
        elif record_type == "golden_candidate":
            self._validate_golden(connection, self._as(record, GoldenCandidateRecord))
        elif record_type == "event":
            self._validate_event(connection, self._as(record, EventRecord))
        elif record_type == "event_delivery":
            self._validate_delivery(connection, self._as(record, EventDeliveryRecord))
        elif record_type == "replay_expectation":
            self._validate_replay_expectation(connection, self._as(record, ReplayExpectationRecord))
        elif record_type == "dlq_repair":
            self._validate_dlq_repair(connection, self._as(record, DlqRepairRecord))

    def _validate_order(self, connection: sqlite3.Connection, order: OrderRecord) -> None:
        owner = self._fact(connection, "user", order.owner_user_id, UserRecord)
        self._same_tenant(order.tenant_id, owner.tenant_id, "order owner")
        for line in order.lines:
            product = self._fact(connection, "product", line.product_id, ProductRecord)
            self._same_tenant(order.tenant_id, product.tenant_id, "order product")
        self._monotonic(
            [point.occurred_at for point in order.status_history], f"order {order.order_id}"
        )

    def _validate_package(self, connection: sqlite3.Connection, package: PackageRecord) -> None:
        order = self._fact(connection, "order", package.order_id, OrderRecord)
        self._same_tenant(package.tenant_id, order.tenant_id, "package order")
        self._monotonic(
            [point.occurred_at for point in package.tracking_events],
            f"package {package.package_id}",
        )

    def _validate_refund(self, connection: sqlite3.Connection, refund: RefundRecord) -> None:
        order = self._fact(connection, "order", refund.order_id, OrderRecord)
        requester = self._fact(connection, "user", refund.requested_by, UserRecord)
        self._same_tenant(refund.tenant_id, order.tenant_id, "refund order")
        self._same_tenant(refund.tenant_id, requester.tenant_id, "refund requester")
        if refund.requested_amount > refund.max_refundable_amount:
            raise DatasetValidationError("amount", f"refund exceeds maximum: {refund.refund_id}")
        if refund.requires_approval:
            connection.execute(
                "INSERT INTO approvals_required(refund_id) VALUES (?)", (refund.refund_id,)
            )

    def _validate_approval(self, connection: sqlite3.Connection, approval: ApprovalRecord) -> None:
        refund = self._fact(connection, "refund", approval.refund_id, RefundRecord)
        approver = self._fact(connection, "user", approval.approver_user_id, UserRecord)
        self._same_tenant(approval.tenant_id, refund.tenant_id, "approval refund")
        self._same_tenant(approval.tenant_id, approver.tenant_id, "approval actor")
        if approver.role != "supervisor":
            raise DatasetValidationError("permission", "approval actor is not supervisor")
        if approval.status != "pending" and approval.decided_at is None:
            raise DatasetValidationError("time", "decided approval lacks timestamp")
        try:
            connection.execute(
                "INSERT INTO approval_by_refund(refund_id, payload) VALUES (?, ?)",
                (
                    approval.refund_id,
                    canonical_json_bytes(approval.model_dump(mode="json")),
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise DatasetValidationError(
                "relationship", f"multiple approvals for refund: {approval.refund_id}"
            ) from exc
        connection.execute(
            "DELETE FROM approvals_required WHERE refund_id = ?", (approval.refund_id,)
        )

    def _validate_knowledge(
        self, connection: sqlite3.Connection, knowledge: KnowledgeRecord
    ) -> None:
        source = self._root / knowledge.relative_path
        if not source.is_file() or sha256_file(source) != knowledge.content_sha256:
            raise DatasetValidationError(
                "relationship", f"knowledge source mismatch: {knowledge.document_id}"
            )
        if knowledge.evidence_anchor_id not in source.read_text(encoding="utf-8"):
            raise DatasetValidationError(
                "relationship", f"knowledge anchor missing: {knowledge.document_id}"
            )
        try:
            connection.execute(
                "INSERT INTO anchors(anchor_id, tenant_id) VALUES (?, ?)",
                (knowledge.evidence_anchor_id, knowledge.tenant_id),
            )
        except sqlite3.IntegrityError as exc:
            raise DatasetValidationError("relationship", "duplicate evidence anchor") from exc

    def _validate_physical(
        self, connection: sqlite3.Connection, physical: PhysicalKnowledgeFileRecord
    ) -> None:
        document = self._fact(connection, "knowledge", physical.document_id, KnowledgeRecord)
        self._same_tenant(physical.tenant_id, document.tenant_id, "physical knowledge")
        source = self._root / physical.relative_path
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
                return
            raise DatasetValidationError(
                "schema", f"physical parse failed: {physical.physical_file_id}"
            ) from exc
        if physical.expected_outcome == "reject_corrupt":
            raise DatasetValidationError(
                "schema", f"corrupt physical file accepted: {physical.physical_file_id}"
            )
        if physical.expected_outcome == "parse_success" and (
            physical.evidence_anchor_id is None or physical.evidence_anchor_id not in parsed
        ):
            raise DatasetValidationError(
                "relationship", f"physical anchor missing: {physical.physical_file_id}"
            )
        self._scan_text(parsed, physical.relative_path)

    def _validate_conversation(
        self, connection: sqlite3.Connection, conversation: ConversationRecord
    ) -> None:
        user = self._fact(connection, "user", conversation.user_id, UserRecord)
        self._same_tenant(conversation.tenant_id, user.tenant_id, "conversation user")
        if conversation.order_id is not None:
            order = self._fact(connection, "order", conversation.order_id, OrderRecord)
            self._same_tenant(conversation.tenant_id, order.tenant_id, "conversation order")
        existing = connection.execute(
            "SELECT split FROM scenario_families WHERE family = ?",
            (conversation.scenario_family,),
        ).fetchone()
        if existing is not None and existing[0] != conversation.split:
            raise DatasetValidationError("leakage", "scenario family split leakage")
        connection.execute(
            "INSERT OR IGNORE INTO scenario_families(family, split) VALUES (?, ?)",
            (conversation.scenario_family, conversation.split),
        )

    def _validate_evaluation(
        self, connection: sqlite3.Connection, case: EvaluationCaseRecord
    ) -> None:
        conversation = self._fact(
            connection, "conversation", case.conversation_id, ConversationRecord
        )
        self._same_tenant(case.tenant_id, conversation.tenant_id, "evaluation conversation")
        if case.split != conversation.split:
            raise DatasetValidationError("leakage", "evaluation split mismatch")
        for anchor_id in case.evidence_anchor_ids:
            row = connection.execute(
                "SELECT tenant_id FROM anchors WHERE anchor_id = ?", (anchor_id,)
            ).fetchone()
            if row is None:
                raise DatasetValidationError("relationship", "missing evaluation anchor")
            self._same_tenant(case.tenant_id, str(row[0]), "evaluation anchor")

    def _validate_access(
        self, connection: sqlite3.Connection, decision: AccessDecisionRecord
    ) -> None:
        actor = self._fact(connection, "user", decision.actor_user_id, UserRecord)
        self._same_tenant(decision.tenant_id, actor.tenant_id, "access actor")
        owner_user_id = None
        if decision.resource_type in {"order", "knowledge", "approval"}:
            model: type[BaseModel]
            if decision.resource_type == "order":
                model = OrderRecord
            elif decision.resource_type == "knowledge":
                model = KnowledgeRecord
            else:
                model = ApprovalRecord
            resource = self._fact_any(
                connection, decision.resource_type, decision.resource_id, model
            )
            if not isinstance(resource, SyntheticRecord):
                raise DatasetValidationError("schema", "access resource lacks metadata")
            self._same_tenant(decision.resource_tenant_id, resource.tenant_id, "access resource")
            if isinstance(resource, OrderRecord):
                owner_user_id = resource.owner_user_id
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

    def _validate_order_oracle(
        self, connection: sqlite3.Connection, case: OrderOracleRecord
    ) -> None:
        actor = self._fact(connection, "user", case.actor_user_id, UserRecord)
        order = self._fact(connection, "order", case.order_id, OrderRecord)
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

    def _validate_logistics_oracle(
        self, connection: sqlite3.Connection, case: LogisticsOracleRecord
    ) -> None:
        self._fact(connection, "package", case.package_id, PackageRecord)
        self._fact(connection, "user", case.actor_user_id, UserRecord)
        decision = LogisticsOracle.decide(case.anomaly, case.urge_attempt)
        if (
            decision.allowed != case.expected_allowed
            or decision.error_code != case.expected_error_code
            or decision.escalate != case.expected_escalation
        ):
            raise DatasetValidationError(
                "relationship", f"logistics oracle mismatch: {case.oracle_case_id}"
            )

    def _validate_refund_lifecycle(
        self, connection: sqlite3.Connection, lifecycle: RefundLifecycleRecord
    ) -> None:
        refund = self._fact(connection, "refund", lifecycle.refund_id, RefundRecord)
        approval_row = connection.execute(
            "SELECT payload FROM approval_by_refund WHERE refund_id = ?",
            (refund.refund_id,),
        ).fetchone()
        approval = (
            ApprovalRecord.model_validate_json(approval_row[0])
            if approval_row is not None
            else None
        )
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

    def _validate_memory(self, connection: sqlite3.Connection, case: MemoryCaseRecord) -> None:
        conversation = self._fact(
            connection, "conversation", case.conversation_id, ConversationRecord
        )
        self._same_tenant(case.tenant_id, conversation.tenant_id, "memory conversation")
        if case.turns != len(conversation.turns):
            raise DatasetValidationError("relationship", "memory turn count mismatch")

    def _validate_golden(
        self, connection: sqlite3.Connection, candidate: GoldenCandidateRecord
    ) -> None:
        evaluation = self._fact(
            connection, "evaluation_case", candidate.evaluation_case_id, EvaluationCaseRecord
        )
        self._same_tenant(candidate.tenant_id, evaluation.tenant_id, "golden candidate")
        if candidate.review_status != "pending_independent_review":
            raise DatasetValidationError("permission", "unreviewed candidate marked reviewed")
        if candidate.evidence_anchor_ids != evaluation.evidence_anchor_ids:
            raise DatasetValidationError("relationship", "golden evidence mismatch")
        if len(set(candidate.review_checklist)) != len(candidate.review_checklist):
            raise DatasetValidationError("schema", "golden candidate checklist contains duplicates")
        for fact_id in candidate.fact_record_ids:
            known = connection.execute(
                "SELECT 1 FROM facts WHERE entity_id = ? "
                "AND record_type IN ('conversation', 'order') LIMIT 1",
                (fact_id,),
            ).fetchone()
            if known is None:
                raise DatasetValidationError("relationship", "golden references unknown fact")

    def _validate_event(self, connection: sqlite3.Connection, event: EventRecord) -> None:
        fact_type = event.aggregate_type
        aggregate_tenant = self._fact_tenant(connection, fact_type, event.aggregate_id)
        if aggregate_tenant is None:
            raise DatasetValidationError("relationship", "event references unknown aggregate")
        self._same_tenant(event.tenant_id, aggregate_tenant, "event aggregate")
        connection.execute(
            "INSERT INTO events"
            "(event_id, tenant_id, aggregate_type, aggregate_id, sequence, payload) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                event.event_id,
                event.tenant_id,
                event.aggregate_type,
                event.aggregate_id,
                event.sequence,
                canonical_json_bytes(event.model_dump(mode="json")),
            ),
        )
        connection.execute(
            "INSERT OR IGNORE INTO aggregate_stats(aggregate_type, aggregate_id) VALUES (?, ?)",
            (event.aggregate_type, event.aggregate_id),
        )
        expected_sequence = (
            int(
                connection.execute(
                    "SELECT last_sequence FROM aggregate_stats "
                    "WHERE aggregate_type = ? AND aggregate_id = ?",
                    (event.aggregate_type, event.aggregate_id),
                ).fetchone()[0]
            )
            + 1
        )
        if event.sequence != expected_sequence:
            raise DatasetValidationError(
                "replay",
                f"non-monotonic event sequence: {event.aggregate_type}/{event.aggregate_id}",
            )
        connection.execute(
            "UPDATE aggregate_stats SET last_sequence = ? "
            "WHERE aggregate_type = ? AND aggregate_id = ?",
            (event.sequence, event.aggregate_type, event.aggregate_id),
        )

    def _validate_delivery(
        self, connection: sqlite3.Connection, delivery: EventDeliveryRecord
    ) -> None:
        if delivery.payload_hash != sha256_bytes(canonical_json_bytes(delivery.payload)):
            raise DatasetValidationError("replay", "delivery payload hash mismatch")
        expected_routes = {
            "normal": "inbox",
            "out_of_order": "inbox",
            "duplicate": "duplicate_ignored",
            "poison": "dlq",
            "unknown_schema": "dlq",
        }
        if expected_routes[delivery.delivery_kind] != delivery.expected_route:
            raise DatasetValidationError("replay", "delivery kind and route disagree")
        aggregate: tuple[str, str] | None = None
        if delivery.expected_route == "dlq":
            if delivery.event_id is not None:
                raise DatasetValidationError("replay", "DLQ delivery unexpectedly has event id")
            self._dataset_dlq_events += 1
        else:
            if delivery.event_id is None:
                raise DatasetValidationError("replay", "routable delivery lacks event id")
            row = connection.execute(
                "SELECT tenant_id, aggregate_type, aggregate_id FROM events WHERE event_id = ?",
                (delivery.event_id,),
            ).fetchone()
            if row is None:
                raise DatasetValidationError("replay", "delivery references unknown event")
            self._same_tenant(delivery.tenant_id, str(row[0]), "event delivery")
            aggregate = (str(row[1]), str(row[2]))
            inserted = connection.execute(
                "INSERT OR IGNORE INTO inbox(event_id) VALUES (?)", (delivery.event_id,)
            ).rowcount
            if bool(inserted) == (delivery.expected_route == "duplicate_ignored"):
                raise DatasetValidationError("replay", "delivery route disagrees with inbox state")
            column = "unique_events" if inserted else "duplicate_events"
            connection.execute(
                f"UPDATE aggregate_stats SET {column} = {column} + 1 "  # noqa: S608
                "WHERE aggregate_type = ? AND aggregate_id = ?",
                aggregate,
            )
            if inserted:
                self._dataset_unique_events += 1
            else:
                self._dataset_duplicate_events += 1
        connection.execute(
            "INSERT INTO deliveries(delivery_id, tenant_id, event_id, delivery_kind, "
            "expected_route, payload_hash) VALUES (?, ?, ?, ?, ?, ?)",
            (
                delivery.delivery_id,
                delivery.tenant_id,
                delivery.event_id,
                delivery.delivery_kind,
                delivery.expected_route,
                delivery.payload_hash,
            ),
        )

    def _finalize_event_truth(self, connection: sqlite3.Connection) -> None:
        invalid = connection.execute(
            "SELECT aggregate_type, aggregate_id FROM events GROUP BY aggregate_type, aggregate_id "
            "HAVING MIN(sequence) != 1 OR MAX(sequence) != COUNT(*) "
            "OR COUNT(DISTINCT sequence) != COUNT(*) LIMIT 1"
        ).fetchone()
        if invalid is not None:
            raise DatasetValidationError("replay", "non-monotonic event sequence")
        query = connection.execute(
            "SELECT e.aggregate_type, e.aggregate_id, e.payload FROM events e "
            "JOIN inbox i ON i.event_id = e.event_id "
            "ORDER BY e.aggregate_type, e.aggregate_id, e.sequence"
        )
        dataset_digest = hashlib.sha256()
        dataset_digest.update(b"[")
        dataset_first = True
        current_key: tuple[str, str] | None = None
        aggregate_digest = hashlib.sha256()
        aggregate_first = True
        for aggregate_type, aggregate_id, payload in query:
            key = (str(aggregate_type), str(aggregate_id))
            if current_key != key:
                if current_key is not None:
                    aggregate_digest.update(b"]\n")
                    connection.execute(
                        "UPDATE aggregate_stats SET terminal_hash = ? "
                        "WHERE aggregate_type = ? AND aggregate_id = ?",
                        (aggregate_digest.hexdigest(), *current_key),
                    )
                current_key = key
                aggregate_digest = hashlib.sha256()
                aggregate_digest.update(b"[")
                aggregate_first = True
            encoded = bytes(payload).rstrip(b"\n")
            if not dataset_first:
                dataset_digest.update(b",")
            dataset_digest.update(encoded)
            dataset_first = False
            if not aggregate_first:
                aggregate_digest.update(b",")
            aggregate_digest.update(encoded)
            aggregate_first = False
        if current_key is not None:
            aggregate_digest.update(b"]\n")
            connection.execute(
                "UPDATE aggregate_stats SET terminal_hash = ? "
                "WHERE aggregate_type = ? AND aggregate_id = ?",
                (aggregate_digest.hexdigest(), *current_key),
            )
        dataset_digest.update(b"]\n")
        self._dataset_terminal_hash = dataset_digest.hexdigest()

    def _validate_replay_expectation(
        self, connection: sqlite3.Connection, expectation: ReplayExpectationRecord
    ) -> None:
        if expectation.aggregate_type == "dataset":
            self._dataset_expectations += 1
            observed = (
                self._dataset_unique_events,
                self._dataset_duplicate_events,
                self._dataset_dlq_events,
                self._dataset_terminal_hash,
            )
        else:
            self._aggregate_expectations += 1
            row = connection.execute(
                "SELECT unique_events, duplicate_events, terminal_hash FROM aggregate_stats "
                "WHERE aggregate_type = ? AND aggregate_id = ?",
                (expectation.aggregate_type, expectation.aggregate_id),
            ).fetchone()
            if row is None:
                raise DatasetValidationError("replay", "expectation references unknown aggregate")
            observed = (int(row[0]), int(row[1]), 0, str(row[2]))
        expected = (
            expectation.expected_unique_events,
            expectation.expected_duplicate_events,
            expectation.expected_dlq_events,
            expectation.expected_terminal_hash,
        )
        if observed != expected:
            raise DatasetValidationError(
                "replay",
                f"replay expectation mismatch: {expectation.aggregate_type}/"
                f"{expectation.aggregate_id}",
            )

    def _validate_dlq_repair(self, connection: sqlite3.Connection, repair: DlqRepairRecord) -> None:
        row = connection.execute(
            "SELECT tenant_id, delivery_kind, expected_route, payload_hash FROM deliveries "
            "WHERE delivery_id = ?",
            (repair.delivery_id,),
        ).fetchone()
        if row is None:
            raise DatasetValidationError("replay", "DLQ repair references unknown delivery")
        operator = self._fact(connection, "user", repair.operator_user_id, UserRecord)
        self._same_tenant(repair.tenant_id, str(row[0]), "DLQ delivery")
        self._same_tenant(repair.tenant_id, operator.tenant_id, "DLQ operator")
        if (
            str(row[2]) != "dlq"
            or str(row[1]) != repair.failure_kind
            or str(row[3]) != repair.original_payload_hash
            or operator.role != "supervisor"
        ):
            raise DatasetValidationError("replay", "invalid DLQ repair truth")
        try:
            connection.execute(
                "INSERT INTO dlq_replay_ids(replay_id, replay_audit_id) VALUES (?, ?)",
                (repair.replay_id, repair.replay_audit_id),
            )
        except sqlite3.IntegrityError as exc:
            raise DatasetValidationError(
                "replay", "duplicate DLQ replay or audit identity"
            ) from exc

    def _validate_final_relationships(self, connection: sqlite3.Connection) -> None:
        missing_approval = connection.execute(
            "SELECT refund_id FROM approvals_required LIMIT 1"
        ).fetchone()
        if missing_approval is not None:
            raise DatasetValidationError("permission", "approval missing for refund")
        event_count = int(connection.execute("SELECT COUNT(*) FROM events").fetchone()[0])
        inbox_count = int(connection.execute("SELECT COUNT(*) FROM inbox").fetchone()[0])
        if inbox_count != event_count:
            raise DatasetValidationError("replay", "not every event reached the inbox exactly once")
        aggregate_count = int(
            connection.execute("SELECT COUNT(*) FROM aggregate_stats").fetchone()[0]
        )
        missing_terminal_hash = connection.execute(
            "SELECT 1 FROM aggregate_stats WHERE terminal_hash IS NULL LIMIT 1"
        ).fetchone()
        if missing_terminal_hash is not None:
            raise DatasetValidationError("replay", "aggregate terminal hash is missing")
        if self._dataset_expectations != 1:
            raise DatasetValidationError(
                "replay", "dataset replay expectation missing or duplicate"
            )
        if self._aggregate_expectations != aggregate_count:
            raise DatasetValidationError(
                "replay", "aggregate replay expectation inventory mismatch"
            )

    @staticmethod
    def _validate_file_metadata(
        root: Path, manifest: DatasetManifest, observed: dict[str, int]
    ) -> None:
        for entry in manifest.files:
            path = root / entry.relative_path
            if sha256_file(path) != entry.sha256:
                raise DatasetValidationError(
                    "determinism", f"checksum mismatch: {entry.relative_path}"
                )
            if path.stat().st_size != entry.size_bytes:
                raise DatasetValidationError("resource", f"size mismatch: {entry.relative_path}")
            if entry.record_type in DatasetValidator.RECORD_MODELS:
                if observed.get(entry.record_type, 0) != entry.records:
                    raise DatasetValidationError(
                        "resource", f"record count mismatch: {entry.relative_path}"
                    )
            elif entry.record_type not in DatasetValidator.NON_RECORD_TYPES:
                raise DatasetValidationError("schema", f"unknown record type: {entry.record_type}")
        for record_type, expected in manifest.record_counts.items():
            if observed.get(record_type, 0) != expected:
                raise DatasetValidationError(
                    "resource", f"manifest count mismatch for {record_type}"
                )

    @staticmethod
    def _fact(
        connection: sqlite3.Connection,
        record_type: str,
        entity_id: str,
        model: type[Any],
    ) -> Any:
        return StreamingDatasetValidator._fact_any(connection, record_type, entity_id, model)

    @staticmethod
    def _fact_any(
        connection: sqlite3.Connection,
        record_type: str,
        entity_id: str,
        model: type[BaseModel],
    ) -> BaseModel:
        row = connection.execute(
            "SELECT payload FROM facts WHERE record_type = ? AND entity_id = ?",
            (record_type, entity_id),
        ).fetchone()
        if row is None:
            raise DatasetValidationError("relationship", f"missing {record_type}: {entity_id}")
        return model.model_validate_json(row[0])

    @staticmethod
    def _fact_exists(connection: sqlite3.Connection, record_type: str, entity_id: str) -> bool:
        return (
            connection.execute(
                "SELECT 1 FROM facts WHERE record_type = ? AND entity_id = ?",
                (record_type, entity_id),
            ).fetchone()
            is not None
        )

    @staticmethod
    def _fact_tenant(
        connection: sqlite3.Connection, record_type: str, entity_id: str
    ) -> str | None:
        row = connection.execute(
            "SELECT tenant_id FROM facts WHERE record_type = ? AND entity_id = ?",
            (record_type, entity_id),
        ).fetchone()
        return str(row[0]) if row is not None else None

    @staticmethod
    def _require_tenant(connection: sqlite3.Connection, tenant_id: str, label: str) -> None:
        if not StreamingDatasetValidator._fact_exists(connection, "tenant", tenant_id):
            raise DatasetValidationError("tenant", f"unknown tenant in {label}")

    @staticmethod
    def _as(record: BaseModel, model: type[Any]) -> Any:
        if not isinstance(record, model):
            raise DatasetValidationError("schema", f"wrong record type: {model.__name__}")
        return record

    @staticmethod
    def _same_tenant(left: str, right: str, label: str) -> None:
        if left != right:
            raise DatasetValidationError("tenant", f"cross-tenant reference: {label}")

    @staticmethod
    def _monotonic(values: list[Any], label: str) -> None:
        if values != sorted(values) or len(values) != len(set(values)):
            raise DatasetValidationError("time", f"non-monotonic timeline: {label}")

    def _scan_forbidden_content(self, root: Path, manifest: DatasetManifest) -> None:
        text_media = {
            "application/json",
            "application/x-ndjson",
            "application/yaml",
            "text/markdown",
            "text/plain",
            "text/html",
        }
        for entry in manifest.files:
            if entry.media_type not in text_media:
                continue
            path = root / entry.relative_path
            with path.open("r", encoding="utf-8", errors="ignore") as stream:
                for line_number, line in enumerate(stream, start=1):
                    self._scan_text(line, f"{entry.relative_path}:{line_number}")

    @classmethod
    def _scan_text(cls, text: str, label: str) -> None:
        for code, pattern in cls.FORBIDDEN_PATTERNS:
            if pattern.search(text):
                raise DatasetValidationError(code, f"forbidden content in {label}")


__all__ = ["StreamingDatasetValidator"]
