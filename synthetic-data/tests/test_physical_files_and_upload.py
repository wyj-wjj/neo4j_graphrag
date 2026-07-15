from __future__ import annotations

import asyncio
import io
import json
import zipfile
from pathlib import Path

import httpx
import pytest

from graphrag_data_factory.exporter import DatasetExporter
from graphrag_data_factory.factory import DatasetBundle
from graphrag_data_factory.models import DatasetProfile, PhysicalKnowledgeFileRecord
from graphrag_data_factory.physical_files import (
    FORMATS,
    is_physical_file_rejected,
    parse_physical_file,
)
from graphrag_data_factory.upload_client import KnowledgeUploadClient


@pytest.mark.invariant
def test_office_metadata_uses_a_fixed_timestamp(ci_bundle: DatasetBundle) -> None:
    for relative_path, content in ci_bundle.knowledge_files.items():
        if "/corrupt." in relative_path or not relative_path.endswith((".docx", ".xlsx", ".pptx")):
            continue
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            core = archive.read("docProps/core.xml")
        assert core.count(b"2025-01-01T00:00:00Z") >= 2


@pytest.mark.invariant
def test_physical_knowledge_matrix_is_complete_and_independently_parseable(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
    ci_bundle: DatasetBundle,
) -> None:
    _, profile_path = ci_profile
    target = tmp_path / "dataset"
    DatasetExporter().export(ci_bundle, profile_path=profile_path, target=target)
    records = tuple(
        PhysicalKnowledgeFileRecord.model_validate_json(line)
        for line in (target / "physical-knowledge-files.jsonl").read_text().splitlines()
    )
    assert len(records) == len(FORMATS) * 3
    assert {(record.format, record.variant) for record in records} == {
        (file_format, variant)
        for file_format in FORMATS
        for variant in ("normal", "boundary", "corrupt")
    }
    for record in records:
        path = target / record.relative_path
        if record.expected_outcome == "reject_corrupt":
            assert is_physical_file_rejected(path, record.format)
        else:
            parsed = parse_physical_file(path, record.format)
            assert parsed
            if record.expected_outcome == "parse_success":
                assert record.evidence_anchor_id in parsed


@pytest.mark.contract
def test_upload_client_uses_formal_api_and_checks_expected_rejections(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
    ci_bundle: DatasetBundle,
) -> None:
    _, profile_path = ci_profile
    target = tmp_path / "dataset"
    DatasetExporter().export(ci_bundle, profile_path=profile_path, target=target)
    observed_authorization: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        observed_authorization.append(request.headers.get("authorization", ""))
        if request.method == "POST":
            if b'filename="corrupt.' in request.content:
                return httpx.Response(400, json={"error_code": "validation_error"})
            return httpx.Response(
                202,
                json={
                    "task_id": "synthetic-task",
                    "document_id": "synthetic-document",
                    "status": "received",
                },
            )
        assert request.url.path == "/api/v1/ingestion-tasks/synthetic-task"
        return httpx.Response(
            200,
            json={
                "task_id": "synthetic-task",
                "document_id": "synthetic-document",
                "status": "completed",
            },
        )

    async def exercise() -> tuple[int, int]:
        token = "-".join(("explicit", "test", "credential"))
        async with KnowledgeUploadClient(
            base_url="https://synthetic.invalid",
            bearer_token=token,
            transport=httpx.MockTransport(handler),
        ) as client:
            success = await client.upload_dataset(target, poll_interval_seconds=0)
            all_results = await client.upload_dataset(
                target,
                include_expected_rejections=True,
                wait_for_terminal=False,
            )
        return len(success), len(all_results)

    success_count, all_count = asyncio.run(exercise())
    assert success_count == len(FORMATS) * 2
    assert all_count == len(FORMATS) * 3
    assert set(observed_authorization) == {"Bearer explicit-test-credential"}
    assert json.loads((target / "manifest.json").read_text())["is_synthetic"] is True


@pytest.mark.contract
def test_upload_client_requires_explicit_token() -> None:
    with pytest.raises(ValueError, match="explicitly"):
        KnowledgeUploadClient(base_url="https://synthetic.invalid", bearer_token="")
