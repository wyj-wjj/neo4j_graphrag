"""Canonical JSONL export, manifest creation and atomic publication."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from graphrag_data_factory.constants import (
    COMPATIBILITY,
    DATASET_VERSION,
    GENERATOR_VERSION,
    ORACLE_VERSION,
    RULES_VERSION,
    SCHEMA_VERSION,
    SPEC_VERSION,
    TEMPLATE_VERSION,
)
from graphrag_data_factory.deterministic import (
    canonical_json_bytes,
    seed_id,
    sha256_bytes,
    sha256_file,
)
from graphrag_data_factory.factory import DatasetBundle
from graphrag_data_factory.models import DatasetManifest, FileManifest
from graphrag_data_factory.validator import DatasetValidator

FILE_NAMES = {
    "access_decision": "access-decisions.jsonl",
    "tenant": "tenants.jsonl",
    "user": "users.jsonl",
    "product": "products.jsonl",
    "order": "orders.jsonl",
    "order_oracle": "order-oracles.jsonl",
    "package": "packages.jsonl",
    "logistics_oracle": "logistics-oracles.jsonl",
    "refund": "refunds.jsonl",
    "approval": "approvals.jsonl",
    "refund_lifecycle": "refund-lifecycles.jsonl",
    "knowledge": "knowledge-metadata.jsonl",
    "physical_knowledge_file": "physical-knowledge-files.jsonl",
    "conversation": "conversations.jsonl",
    "evaluation_case": "evaluation-cases.jsonl",
    "memory_case": "memory-cases.jsonl",
    "golden_candidate": "golden-candidates.jsonl",
    "security_case": "security-cases.jsonl",
    "fault_schedule": "fault-schedules.jsonl",
    "event": "events.jsonl",
    "event_delivery": "event-deliveries.jsonl",
    "replay_expectation": "replay-expectations.jsonl",
    "dlq_repair": "dlq-repairs.jsonl",
}


class DatasetExporter:
    """Publish only after the complete temporary tree passes validation."""

    def __init__(self, validator: DatasetValidator | None = None) -> None:
        self.validator = validator or DatasetValidator()

    def export(
        self,
        bundle: DatasetBundle,
        *,
        profile_path: Path,
        target: Path,
        replace: bool = False,
        fail_at: str | None = None,
    ) -> DatasetManifest:
        target = target.resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() and not replace:
            raise FileExistsError(f"target exists; pass replace explicitly: {target}")
        if target.exists():
            self._assert_replaceable(target)

        temporary = Path(tempfile.mkdtemp(prefix=f".{target.name}.tmp-", dir=target.parent))
        backup = target.parent / f".{target.name}.backup"
        try:
            entries: list[FileManifest] = []
            for record_type, records in sorted(bundle.records.items()):
                path = temporary / FILE_NAMES[record_type]
                self._write_jsonl(path, records)
                entries.append(self._entry(path, temporary, record_type, len(records)))
            if fail_at == "after_records":
                raise RuntimeError("injected export failure after records")

            for relative_path, content in sorted(bundle.knowledge_files.items()):
                path = temporary / relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
                entries.append(self._entry(path, temporary, "knowledge_source", 1))

            copied_profile = temporary / "profile.yaml"
            shutil.copyfile(profile_path, copied_profile)
            entries.append(self._entry(copied_profile, temporary, "profile", 1))

            validation_report = temporary / "validation-report.json"
            report_payload = {
                "report_version": "synthetic-validation-report-v1",
                "dataset_id": bundle.dataset_id,
                "profile": bundle.profile.name,
                "status": "passed",
                "measurement_mode": "synthetic_offline",
                "is_synthetic": True,
                "production_slo_eligible": False,
                "quality_tiers": {
                    "evaluation_case": "silver",
                    "golden_candidate": "pending_independent_review",
                    "golden": 0,
                },
                "limitations": [
                    "not_real_business_data",
                    "not_production_slo_evidence",
                    "formal_dependency_ingestion_not_executed_by_exporter",
                ],
                "checks": {
                    "schema": "passed",
                    "relationships": "passed",
                    "amounts": "passed",
                    "timelines": "passed",
                    "tenants": "passed",
                    "fake_markers": "passed",
                    "pii_and_secrets": "passed",
                    "checksums": "passed",
                },
                "issues": [],
            }
            validation_report.write_bytes(canonical_json_bytes(report_payload))
            entries.append(self._entry(validation_report, temporary, "validation_report", 1))

            record_counts = {
                record_type: len(records) for record_type, records in sorted(bundle.records.items())
            }
            domains = tuple(sorted(bundle.records))
            manifest = DatasetManifest(
                dataset_id=bundle.dataset_id,
                dataset_version=DATASET_VERSION,
                spec_version=SPEC_VERSION,
                generator_version=GENERATOR_VERSION,
                schema_version=SCHEMA_VERSION,
                rules_version=RULES_VERSION,
                template_version=TEMPLATE_VERSION,
                oracle_version=ORACLE_VERSION,
                profile=bundle.profile.name,
                profile_sha256=sha256_file(copied_profile),
                root_seed=bundle.profile.root_seed,
                derived_seed_ids={
                    domain: seed_id(bundle.profile.root_seed, domain) for domain in domains
                },
                deterministic_created_at=bundle.profile.reference_time,
                status="validated",
                files=tuple(sorted(entries, key=lambda item: item.relative_path)),
                record_counts=record_counts,
                compatibility=COMPATIBILITY,
            )
            (temporary / "manifest.json").write_bytes(
                canonical_json_bytes(manifest.model_dump(mode="json"))
            )
            if fail_at == "before_validate":
                raise RuntimeError("injected export failure before validation")
            self.validator.validate(temporary)
            if fail_at == "before_publish":
                raise RuntimeError("injected export failure before publish")
            self._publish(temporary, target, backup)
            return manifest
        except BaseException:
            if temporary.exists():
                shutil.rmtree(temporary)
            if backup.exists() and not target.exists():
                os.replace(backup, target)
            raise
        finally:
            if backup.exists() and target.exists():
                shutil.rmtree(backup)

    @staticmethod
    def _write_jsonl(path: Path, records: tuple[BaseModel, ...]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as stream:
            for record in records:
                stream.write(canonical_json_bytes(record.model_dump(mode="json")))

    @staticmethod
    def _entry(path: Path, root: Path, record_type: str, records: int) -> FileManifest:
        suffix = path.suffix.lower()
        media_type = {
            ".json": "application/json",
            ".jsonl": "application/x-ndjson",
            ".yaml": "application/yaml",
            ".md": "text/markdown",
            ".txt": "text/plain",
            ".html": "text/html",
            ".pdf": "application/pdf",
            ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            ".png": "image/png",
            ".jpeg": "image/jpeg",
        }.get(suffix, "application/octet-stream")
        return FileManifest(
            relative_path=path.relative_to(root).as_posix(),
            media_type=media_type,
            record_type=record_type,
            records=records,
            size_bytes=path.stat().st_size,
            sha256=sha256_file(path),
        )

    @staticmethod
    def _assert_replaceable(target: Path) -> None:
        manifest_path = target / "manifest.json"
        if not target.is_dir() or not manifest_path.is_file():
            raise ValueError("refusing to replace directory without a synthetic manifest")
        payload: Any = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            payload.get("is_synthetic") is not True
            or payload.get("production_slo_eligible") is not False
        ):
            raise ValueError("refusing to replace a directory not owned by the synthetic factory")

    @staticmethod
    def _publish(temporary: Path, target: Path, backup: Path) -> None:
        if backup.exists():
            raise FileExistsError(f"stale backup requires manual inspection: {backup}")
        if target.exists():
            os.replace(target, backup)
        try:
            os.replace(temporary, target)
        except BaseException:
            if backup.exists() and not target.exists():
                os.replace(backup, target)
            raise
        if backup.exists():
            shutil.rmtree(backup)


def manifest_digest(manifest: DatasetManifest) -> str:
    return sha256_bytes(canonical_json_bytes(manifest.model_dump(mode="json")))


__all__ = ["DatasetExporter", "manifest_digest"]
