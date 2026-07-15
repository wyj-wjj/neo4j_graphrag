from __future__ import annotations

from pathlib import Path

import pytest

from graphrag_data_factory.deterministic import load_profile, project_root
from graphrag_data_factory.exporter import DatasetExporter
from graphrag_data_factory.factory import DatasetBundle
from graphrag_data_factory.models import DatasetProfile
from graphrag_data_factory.resources import (
    assert_large_output_path,
    estimate_generation,
    execute_cleanup,
    plan_cleanup,
)
from graphrag_data_factory.validator import DatasetValidationError


@pytest.mark.contract
def test_resource_estimate_is_stable_and_large_output_is_dedicated(tmp_path: Path) -> None:
    profile, _ = load_profile("staging-large")
    first = estimate_generation(profile)
    second = estimate_generation(profile)
    assert first == second
    assert first.estimated_records > 1_000_000
    with pytest.raises(ValueError, match="must write below"):
        assert_large_output_path(profile, tmp_path / "unsafe", project_root())
    assert_large_output_path(
        profile,
        project_root() / "generated" / "synthetic-commerce-v1" / "staging-large",
        project_root(),
    )


@pytest.mark.fault
def test_cleanup_is_dry_run_by_default_and_requires_exact_dataset_confirmation(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
    ci_bundle: DatasetBundle,
) -> None:
    _, profile_path = ci_profile
    target = tmp_path / "dataset"
    original = DatasetExporter().export(ci_bundle, profile_path=profile_path, target=target)
    plan = plan_cleanup(target)
    assert target.is_dir()
    assert plan.dataset_id == original.dataset_id
    assert plan.action == "dry_run"
    with pytest.raises(ValueError, match="confirmation"):
        execute_cleanup(target, confirm_dataset_id="wrong-synthetic-dataset")
    assert target.is_dir()
    executed = execute_cleanup(target, confirm_dataset_id=original.dataset_id)
    assert executed.action == "executed"
    assert not target.exists()

    rebuilt = DatasetExporter().export(ci_bundle, profile_path=profile_path, target=target)
    assert rebuilt == original


@pytest.mark.fault
def test_cleanup_rejects_untracked_or_unowned_directories(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
    ci_bundle: DatasetBundle,
) -> None:
    _, profile_path = ci_profile
    target = tmp_path / "dataset"
    DatasetExporter().export(ci_bundle, profile_path=profile_path, target=target)
    (target / "user-owned.txt").write_text("must survive", encoding="utf-8")
    with pytest.raises(DatasetValidationError, match="file inventory mismatch"):
        plan_cleanup(target)
    assert (target / "user-owned.txt").read_text(encoding="utf-8") == "must survive"

    unknown = tmp_path / "unknown"
    unknown.mkdir()
    with pytest.raises(DatasetValidationError, match=r"manifest\.json is missing"):
        plan_cleanup(unknown)
