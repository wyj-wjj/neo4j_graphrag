"""Resource estimates and manifest-owned cleanup for generated datasets."""

from __future__ import annotations

import hashlib
import os
import shutil
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from graphrag_data_factory.models import DatasetProfile
from graphrag_data_factory.streaming_validator import StreamingDatasetValidator


class GenerationEstimate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    profile: str
    estimated_records: int = Field(ge=1)
    estimated_bytes: int = Field(ge=1)
    estimated_peak_memory_bytes: int = Field(ge=1)
    output_class: str
    estimate_only: bool = True


class CleanupPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    dataset_id: str
    dataset_root: str
    owned_files: tuple[str, ...]
    owned_file_count: int = Field(ge=1)
    owned_bytes: int = Field(ge=1)
    action: str = "dry_run"


def estimate_generation(profile: DatasetProfile) -> GenerationEstimate:
    """Conservative planning numbers; never presented as measured runtime metrics."""

    counts = profile.counts
    derived = (
        counts.users * 5
        + counts.orders * 3
        + counts.orders * 2
        + counts.orders * 7
        + counts.golden_candidates
        + counts.memory_cases
        + counts.event_deliveries
        + counts.events
        + max(0, counts.event_deliveries - counts.events)
    )
    primary = sum(
        (
            counts.tenants,
            counts.users,
            counts.products,
            counts.orders,
            counts.conversations,
            counts.knowledge_documents,
            counts.physical_files,
            counts.events,
            counts.evaluation_cases,
            counts.security_cases,
            counts.fault_schedules,
        )
    )
    estimated_records = primary + derived
    estimated_bytes = estimated_records * 1_500 + counts.knowledge_documents * 8_000
    estimated_peak = min(estimated_bytes * 2, estimated_records * 3_000)
    output_class = "git-fixture" if profile.name == "ci-small" else "ignored-generated-artifact"
    return GenerationEstimate(
        profile=profile.name,
        estimated_records=estimated_records,
        estimated_bytes=estimated_bytes,
        estimated_peak_memory_bytes=estimated_peak,
        output_class=output_class,
    )


def plan_cleanup(dataset_dir: Path) -> CleanupPlan:
    root = dataset_dir.resolve()
    manifest = StreamingDatasetValidator().validate(root)
    relative_paths = tuple(
        sorted(("manifest.json", *(entry.relative_path for entry in manifest.files)))
    )
    owned_bytes = sum((root / relative_path).stat().st_size for relative_path in relative_paths)
    return CleanupPlan(
        dataset_id=manifest.dataset_id,
        dataset_root=str(root),
        owned_files=relative_paths,
        owned_file_count=len(relative_paths),
        owned_bytes=owned_bytes,
    )


def execute_cleanup(dataset_dir: Path, *, confirm_dataset_id: str) -> CleanupPlan:
    """Delete only a fully validated directory whose exact dataset ID is confirmed."""

    root = dataset_dir.resolve()
    plan = plan_cleanup(root)
    if confirm_dataset_id != plan.dataset_id:
        raise ValueError("cleanup confirmation does not match dataset_id")
    if root.parent == root or root == Path.home().resolve():
        raise ValueError("refusing to clean an unsafe root")
    quarantine = root.parent / f".{root.name}.cleanup-{manifest_suffix(plan.dataset_id)}"
    if quarantine.exists():
        raise FileExistsError(f"cleanup quarantine already exists: {quarantine}")
    os.replace(root, quarantine)
    try:
        shutil.rmtree(quarantine)
    except BaseException:
        if quarantine.exists() and not root.exists():
            os.replace(quarantine, root)
        raise
    return plan.model_copy(update={"action": "executed"})


def assert_large_output_path(profile: DatasetProfile, output: Path, project_root: Path) -> None:
    if not profile.requires_explicit_large_flag:
        return
    generated_root = (project_root / "generated").resolve()
    if not output.resolve().is_relative_to(generated_root):
        raise ValueError(f"large profiles must write below {generated_root}")


def manifest_suffix(dataset_id: str) -> str:
    return hashlib.sha256(dataset_id.encode("utf-8")).hexdigest()[:12]


__all__ = [
    "CleanupPlan",
    "GenerationEstimate",
    "assert_large_output_path",
    "estimate_generation",
    "execute_cleanup",
    "plan_cleanup",
]
