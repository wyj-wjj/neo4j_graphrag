from __future__ import annotations

import json
from pathlib import Path

import pytest

from graphrag_data_factory.exporter import DatasetExporter
from graphrag_data_factory.factory import DatasetBundle
from graphrag_data_factory.models import DatasetProfile
from graphrag_data_factory.validator import DatasetValidationError, DatasetValidator


def _tree_bytes(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


@pytest.mark.invariant
def test_same_profile_produces_byte_identical_dataset(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
    ci_bundle: DatasetBundle,
) -> None:
    _, profile_path = ci_profile
    first = tmp_path / "first"
    second = tmp_path / "second"
    exporter = DatasetExporter()
    exporter.export(ci_bundle, profile_path=profile_path, target=first)
    exporter.export(ci_bundle, profile_path=profile_path, target=second)
    assert _tree_bytes(first) == _tree_bytes(second)


@pytest.mark.fault
@pytest.mark.parametrize("stage", ["after_records", "before_validate", "before_publish"])
def test_failed_export_never_publishes_partial_dataset(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
    ci_bundle: DatasetBundle,
    stage: str,
) -> None:
    _, profile_path = ci_profile
    target = tmp_path / "dataset"
    with pytest.raises(RuntimeError, match="injected export failure"):
        DatasetExporter().export(
            ci_bundle,
            profile_path=profile_path,
            target=target,
            fail_at=stage,
        )
    assert not target.exists()
    assert not any(path.name.startswith(".dataset.tmp-") for path in tmp_path.iterdir())


@pytest.mark.invariant
def test_tamper_and_untracked_files_are_detected(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
    ci_bundle: DatasetBundle,
) -> None:
    _, profile_path = ci_profile
    target = tmp_path / "dataset"
    DatasetExporter().export(ci_bundle, profile_path=profile_path, target=target)
    orders = target / "orders.jsonl"
    orders.write_bytes(orders.read_bytes() + b"\n")
    with pytest.raises(DatasetValidationError, match="checksum mismatch"):
        DatasetValidator().validate(target)

    DatasetExporter().export(ci_bundle, profile_path=profile_path, target=target, replace=True)
    (target / "untracked.txt").write_text("synthetic", encoding="utf-8")
    with pytest.raises(DatasetValidationError, match="file inventory mismatch"):
        DatasetValidator().validate(target)


@pytest.mark.invariant
def test_replace_refuses_unowned_directory(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
    ci_bundle: DatasetBundle,
) -> None:
    _, profile_path = ci_profile
    target = tmp_path / "unknown"
    target.mkdir()
    (target / "data.txt").write_text("user-owned", encoding="utf-8")
    with pytest.raises(ValueError, match="without a synthetic manifest"):
        DatasetExporter().export(ci_bundle, profile_path=profile_path, target=target, replace=True)
    assert (target / "data.txt").read_text(encoding="utf-8") == "user-owned"


@pytest.mark.contract
def test_manifest_has_no_runtime_timestamp_or_production_claim(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
    ci_bundle: DatasetBundle,
) -> None:
    profile, profile_path = ci_profile
    target = tmp_path / "dataset"
    DatasetExporter().export(ci_bundle, profile_path=profile_path, target=target)
    payload = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
    assert payload["deterministic_created_at"] == profile.reference_time.isoformat().replace(
        "+00:00", "Z"
    )
    assert payload["is_synthetic"] is True
    assert payload["production_slo_eligible"] is False
