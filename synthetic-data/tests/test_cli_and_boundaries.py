from __future__ import annotations

from pathlib import Path

import pytest

from graphrag_data_factory.cli import main
from graphrag_data_factory.deterministic import load_profile, project_root
from graphrag_data_factory.factory import DatasetFactory
from graphrag_data_factory.models import DatasetManifest, DatasetProfile
from graphrag_data_factory.resources import estimate_generation


@pytest.mark.contract
def test_cli_generates_and_validates_ci_small(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    target = tmp_path / "ci-small"
    assert main(["generate", "--profile", "ci-small", "--output", str(target)]) == 0
    assert main(["validate", str(target)]) == 0
    output = capsys.readouterr().out
    assert "validated synthetic dataset" in output
    assert "valid synthetic dataset" in output


@pytest.mark.contract
def test_production_package_does_not_import_data_factory() -> None:
    production_root = project_root().parent / "src" / "graphrag"
    matches = []
    for path in production_root.rglob("*.py"):
        if "graphrag_data_factory" in path.read_text(encoding="utf-8"):
            matches.append(path)
    assert matches == []


@pytest.mark.contract
def test_cli_estimates_without_generating_and_large_profile_requires_exact_confirmation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["estimate", "--profile", "dev-standard"]) == 0
    assert '"estimate_only": true' in capsys.readouterr().out
    with pytest.raises(SystemExit, match="confirm-profile"):
        main(
            [
                "generate",
                "--profile",
                "dev-standard",
                "--allow-large",
                "--output",
                str(tmp_path / "must-not-exist"),
            ]
        )
    assert not (tmp_path / "must-not-exist").exists()


@pytest.mark.contract
def test_simulator_cli_refuses_non_loopback_binding(tmp_path: Path) -> None:
    all_interfaces = ".".join(("0", "0", "0", "0"))
    with pytest.raises(SystemExit, match="loopback"):
        main(["serve-simulator", str(tmp_path / "missing"), "--host", all_interfaces])


@pytest.mark.contract
def test_dev_materialization_dispatches_to_resumable_streaming_exporter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile, _ = load_profile("dev-standard")
    estimate = estimate_generation(profile)
    observed: dict[str, object] = {}
    fixture_manifest = DatasetManifest.model_validate_json(
        (project_root() / "fixtures" / "ci-small" / "manifest.json").read_bytes()
    )

    def fake_export(
        _self: object,
        selected_profile: DatasetProfile,
        **kwargs: object,
    ) -> DatasetManifest:
        observed["profile"] = selected_profile.name
        observed.update(kwargs)
        return fixture_manifest

    monkeypatch.setattr("graphrag_data_factory.cli.StreamingProfileExporter.export", fake_export)
    target = project_root() / "generated" / "synthetic-commerce-v1" / "cli-dispatch-dev"
    assert (
        main(
            [
                "generate",
                "--profile",
                profile.name,
                "--allow-large",
                "--confirm-profile",
                profile.name,
                "--confirm-estimated-bytes",
                str(estimate.estimated_bytes),
                "--resume",
                "--output",
                str(target),
            ]
        )
        == 0
    )
    assert observed["profile"] == "dev-standard"
    assert observed["target"] == target
    assert observed["resume"] is True
    with pytest.raises(ValueError, match="in-memory factory refuses"):
        DatasetFactory(profile)


@pytest.mark.contract
def test_staging_large_remains_disabled_after_all_confirmations() -> None:
    profile, _ = load_profile("staging-large")
    estimate = estimate_generation(profile)
    target = project_root() / "generated" / "synthetic-commerce-v1" / "disabled-test"
    with pytest.raises(SystemExit, match="disk-backed working set"):
        main(
            [
                "generate",
                "--profile",
                profile.name,
                "--allow-large",
                "--confirm-profile",
                profile.name,
                "--confirm-estimated-bytes",
                str(estimate.estimated_bytes),
                "--output",
                str(target),
            ]
        )
    assert not target.exists()
