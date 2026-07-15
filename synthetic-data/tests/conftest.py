from __future__ import annotations

from pathlib import Path

import pytest

from graphrag_data_factory.deterministic import load_profile
from graphrag_data_factory.factory import DatasetBundle, DatasetFactory
from graphrag_data_factory.models import DatasetProfile


@pytest.fixture(scope="session")
def ci_profile() -> tuple[DatasetProfile, Path]:
    return load_profile("ci-small")


@pytest.fixture(scope="session")
def ci_bundle(ci_profile: tuple[DatasetProfile, Path]) -> DatasetBundle:
    profile, _ = ci_profile
    return DatasetFactory(profile).generate()
