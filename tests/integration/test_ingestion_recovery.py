from __future__ import annotations

from pathlib import Path

import pytest

from graphrag.application.container import build_runtime
from graphrag.config import Settings
from graphrag.domain.errors import DependencyError
from graphrag.domain.ids import new_id
from graphrag.domain.models import IdentityContext, IngestionStatus
from graphrag.infrastructure.fakes import FakeModelProvider


@pytest.mark.asyncio
async def test_partial_failure_is_detectable_and_explicit_retry_resumes(
    settings: Settings, tmp_path: Path
) -> None:
    runtime = build_runtime(settings.model_copy(update={"upload_dir": tmp_path / "objects"}))
    identity = IdentityContext(tenant_id="default", user_id="demo-user", roles=frozenset({"admin"}))
    task = await runtime.ingestion.receive(
        identity,
        filename="policy.txt",
        content="政策内容足够用于索引".encode(),
        title="Policy",
        trace_id=new_id(),
    )
    provider = runtime.embedding
    assert isinstance(provider, FakeModelProvider)
    provider.fail_mode = "error"
    failed = await runtime.ingestion.process(task, trace_id=new_id())
    assert failed.status == IngestionStatus.FAILED
    chunks_before = await runtime.repository.list_chunks_for_version(
        "default", task.version_id or ""
    )
    assert chunks_before

    provider.fail_mode = None
    retried = await runtime.repository.retry_task("default", task.task_id)
    completed = await runtime.ingestion.process(retried, trace_id=new_id())
    assert completed.status == IngestionStatus.COMPLETED
    chunks_after = await runtime.repository.list_chunks_for_version(
        "default", task.version_id or ""
    )
    assert [item.chunk_id for item in chunks_after] == [item.chunk_id for item in chunks_before]


@pytest.mark.asyncio
async def test_parent_chunks_are_persisted_but_only_children_are_indexed(
    settings: Settings, tmp_path: Path
) -> None:
    runtime = build_runtime(settings.model_copy(update={"upload_dir": tmp_path / "parents"}))
    identity = IdentityContext(tenant_id="default", user_id="demo-user", roles=frozenset({"admin"}))
    task = await runtime.ingestion.receive(
        identity,
        filename="long.txt",
        content=("long policy " * 80).encode(),
        title="Long",
        trace_id=new_id(),
    )
    completed = await runtime.ingestion.process(task, trace_id=new_id())
    assert completed.status == IngestionStatus.COMPLETED
    chunks = await runtime.repository.list_chunks_for_version("default", task.version_id or "")
    parents = [item for item in chunks if item.chunk_kind == "parent"]
    children = [item for item in chunks if item.chunk_kind == "child"]
    assert parents and any(item.parent_chunk_id for item in children)
    vector_ids = await runtime.vector_store.list_ids("default")
    assert vector_ids == {item.chunk_id for item in children}


@pytest.mark.asyncio
async def test_retrieval_degrades_for_derived_indexes_but_authorization_fails_closed(
    settings: Settings, tmp_path: Path
) -> None:
    runtime = build_runtime(settings.model_copy(update={"upload_dir": tmp_path / "faults"}))
    identity = IdentityContext(
        tenant_id="default", user_id="demo-user", roles=frozenset({"user", "admin"})
    )
    task = await runtime.ingestion.receive(
        identity,
        filename="policy.txt",
        content="故障演练政策允许依靠关键词降级检索".encode(),
        title="Fault policy",
        trace_id=new_id(),
    )
    assert (await runtime.ingestion.process(task, trace_id=new_id())).status == (
        IngestionStatus.COMPLETED
    )

    async def unavailable(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected unavailable dependency")

    vector_search = runtime.vector_store.search
    graph_search = runtime.graph_store.search
    runtime.vector_store.search = unavailable  # type: ignore[method-assign]
    runtime.graph_store.search = unavailable  # type: ignore[method-assign]
    degraded = await runtime.retrieval.retrieve(identity, "故障演练政策")
    assert degraded.evidences
    assert degraded.branch_status["dense"].startswith("failed")
    assert degraded.branch_status["graph"].startswith("failed")
    assert degraded.branch_status["keyword"] == "succeeded"

    runtime.vector_store.search = vector_search  # type: ignore[method-assign]
    runtime.graph_store.search = graph_search  # type: ignore[method-assign]
    runtime.repository.authorize_chunks = unavailable  # type: ignore[method-assign]
    with pytest.raises(DependencyError) as caught:
        await runtime.retrieval.retrieve(identity, "故障演练政策")
    assert caught.value.dependency == "authorization"
