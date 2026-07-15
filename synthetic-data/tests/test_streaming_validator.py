from __future__ import annotations

import json
from pathlib import Path

import pytest

from graphrag_data_factory.deterministic import canonical_json_bytes, sha256_file
from graphrag_data_factory.exporter import FILE_NAMES, DatasetExporter
from graphrag_data_factory.factory import DatasetBundle
from graphrag_data_factory.models import DatasetManifest, DatasetProfile
from graphrag_data_factory.streaming_factory import StreamingProfileExporter
from graphrag_data_factory.streaming_validator import StreamingDatasetValidator
from graphrag_data_factory.validator import DatasetValidationError


@pytest.mark.invariant
def test_streaming_validator_accepts_complete_ci_dataset(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
    ci_bundle: DatasetBundle,
) -> None:
    _, profile_path = ci_profile
    target = tmp_path / "ci-small"
    expected = DatasetExporter().export(ci_bundle, profile_path=profile_path, target=target)

    observed = StreamingDatasetValidator().validate(target)

    assert observed == expected


@pytest.mark.invariant
def test_streaming_validator_detects_relationship_tampering_with_valid_checksum(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
    ci_bundle: DatasetBundle,
) -> None:
    _, profile_path = ci_profile
    target = tmp_path / "ci-small"
    DatasetExporter().export(ci_bundle, profile_path=profile_path, target=target)

    orders_path = target / FILE_NAMES["order"]
    lines = orders_path.read_text(encoding="utf-8").splitlines()
    first_order = json.loads(lines[0])
    first_order["owner_user_id"] = "synthetic-missing-user"
    lines[0] = json.dumps(first_order, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    orders_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    manifest_path = target / "manifest.json"
    manifest = DatasetManifest.model_validate_json(manifest_path.read_bytes())
    replacement = next(item for item in manifest.files if item.record_type == "order")
    replacement = replacement.model_copy(
        update={"sha256": sha256_file(orders_path), "size_bytes": orders_path.stat().st_size}
    )
    files = tuple(replacement if item.record_type == "order" else item for item in manifest.files)
    manifest = manifest.model_copy(update={"files": files})
    manifest_path.write_bytes(canonical_json_bytes(manifest.model_dump(mode="json")))

    with pytest.raises(DatasetValidationError, match="missing user"):
        StreamingDatasetValidator().validate(target)


@pytest.mark.contract
def test_streaming_validator_rejects_incompatible_generator_version(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
    ci_bundle: DatasetBundle,
) -> None:
    _, profile_path = ci_profile
    target = tmp_path / "ci-small"
    manifest = DatasetExporter().export(ci_bundle, profile_path=profile_path, target=target)
    manifest = manifest.model_copy(update={"generator_version": "obsolete-generator"})
    (target / "manifest.json").write_bytes(canonical_json_bytes(manifest.model_dump(mode="json")))

    with pytest.raises(DatasetValidationError, match="generator_version"):
        StreamingDatasetValidator().validate(target)


@pytest.mark.invariant
def test_streaming_export_is_byte_identical_to_existing_ci_export(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
    ci_bundle: DatasetBundle,
) -> None:
    profile, profile_path = ci_profile
    expected = tmp_path / "expected"
    streamed = tmp_path / "streamed"
    expected_manifest = DatasetExporter().export(
        ci_bundle,
        profile_path=profile_path,
        target=expected,
    )

    large_gated_ci_profile = profile.model_copy(update={"requires_explicit_large_flag": True})
    streamed_manifest = StreamingProfileExporter().export(
        large_gated_ci_profile,
        profile_path=profile_path,
        target=streamed,
    )

    assert streamed_manifest == expected_manifest
    expected_files = sorted(
        path.relative_to(expected) for path in expected.rglob("*") if path.is_file()
    )
    streamed_files = sorted(
        path.relative_to(streamed) for path in streamed.rglob("*") if path.is_file()
    )
    assert streamed_files == expected_files
    assert all(
        (streamed / relative_path).read_bytes() == (expected / relative_path).read_bytes()
        for relative_path in expected_files
    )
