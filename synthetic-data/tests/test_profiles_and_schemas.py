from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from graphrag_data_factory.cli import main
from graphrag_data_factory.deterministic import load_profile, project_root
from graphrag_data_factory.models import DatasetProfile
from graphrag_data_factory.schema_export import export_schemas


@pytest.mark.contract
def test_all_versioned_profiles_load_and_match_file_name() -> None:
    for name in ("ci-small", "dev-standard", "failure-lab", "staging-large"):
        profile, path = load_profile(name)
        assert profile.name == name
        assert path.stem == name
        assert sum(profile.intent_percentages.values()) == 100


@pytest.mark.contract
def test_unknown_profile_fails_before_file_creation(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unsupported profile"):
        load_profile("unknown")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.contract
def test_invalid_percentages_and_unknown_fields_are_rejected() -> None:
    profile, path = load_profile("ci-small")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    payload["intent_percentages"]["faq"] = 19
    with pytest.raises(ValidationError, match="sum to 100"):
        DatasetProfile.model_validate(payload)
    payload = profile.model_dump(mode="json") | {"unexpected": True}
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        DatasetProfile.model_validate(payload)


@pytest.mark.contract
@pytest.mark.parametrize("profile", ["dev-standard", "failure-lab", "staging-large"])
def test_large_profiles_require_explicit_flag(tmp_path: Path, profile: str) -> None:
    output = tmp_path / profile
    with pytest.raises(SystemExit, match="requires --allow-large"):
        main(["generate", "--profile", profile, "--output", str(output)])
    assert not output.exists()


@pytest.mark.contract
def test_json_schema_snapshots_are_current(tmp_path: Path) -> None:
    generated = export_schemas(tmp_path)
    committed_root = project_root() / "schemas"
    assert len(generated) == len(tuple(committed_root.glob("*.schema.json")))
    for path in generated:
        assert path.read_bytes() == (committed_root / path.name).read_bytes()
