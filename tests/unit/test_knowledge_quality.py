from __future__ import annotations

import hashlib

import pytest

from graphrag.application.knowledge_quality import KnowledgeQualityService
from graphrag.domain.events import EventEnvelope
from graphrag.domain.ids import new_id
from graphrag.domain.models import (
    ChunkRecord,
    DocumentRecord,
    DocumentVersion,
    IngestionTask,
)
from graphrag.infrastructure.memory import InMemoryKnowledgeRepository


@pytest.mark.asyncio
async def test_quality_report_finds_conflicts_duplicates_and_low_quality_without_mutation() -> None:
    repository = InMemoryKnowledgeRepository()
    for index, content in enumerate(("短内容", "另一个短内容"), start=1):
        document = DocumentRecord(tenant_id="default", title="同名 政策")
        version = DocumentVersion(
            document_id=document.document_id,
            tenant_id="default",
            version=1,
            object_key=f"doc-{index}.txt",
            content_hash=hashlib.sha256(f"version-{index}".encode()).hexdigest(),
            mime_type="text/plain",
            size_bytes=len(content.encode()),
        )
        task = IngestionTask(
            tenant_id="default",
            document_id=document.document_id,
            version_id=version.version_id,
        )
        await repository.create_document(
            document,
            version,
            task,
            EventEnvelope(
                event_type="document.received",
                tenant_id="default",
                aggregate_id=document.document_id,
                trace_id=new_id(),
            ),
        )
        chunk = ChunkRecord(
            tenant_id="default",
            document_id=document.document_id,
            version_id=version.version_id,
            ordinal=0,
            content=content,
            content_hash="f" * 64,
            embedding_model="fake",
            embedding_dimension=4,
            embedding_version="v1",
        )
        await repository.save_chunks(
            [chunk],
            EventEnvelope(
                event_type="document.chunked",
                tenant_id="default",
                aggregate_id=document.document_id,
                trace_id=new_id(),
            ),
        )

    before = len(repository.documents), len(repository.versions), len(repository.chunks)
    report = await KnowledgeQualityService(repository).report("default")
    issue_types = {item.issue_type for item in report.issues}
    assert {"conflict", "duplicate", "low_quality"} <= issue_types
    assert before == (len(repository.documents), len(repository.versions), len(repository.chunks))
