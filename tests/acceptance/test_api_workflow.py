from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from collections.abc import AsyncIterator, Sequence

import pytest
from fastapi.testclient import TestClient
from tests.conftest import issue_token

from graphrag.api.routes import _execute_chat_claim
from graphrag.api.schemas import ChatRequest
from graphrag.application.run_events import RunEventEmitter
from graphrag.application.sessions import TurnStatus
from graphrag.domain.models import ChatCompletion, ChatDelta, IdentityContext
from graphrag.infrastructure.fakes import FakeModelProvider
from graphrag.infrastructure.memory import InMemoryAudit, InMemoryKnowledgeRepository


class ProbeStreamingModel(FakeModelProvider):
    def __init__(self, *, blocking: bool = False) -> None:
        super().__init__(dimension=32, version="probe-stream-v1")
        self.blocking = blocking
        self.first_delta = asyncio.Event()
        self.release = asyncio.Event()
        self.completed = False
        self.stream_calls = 0

    async def complete(
        self, messages: Sequence[dict[str, str]], *, model: str, timeout: float
    ) -> ChatCompletion:
        raise AssertionError("GraphRAG generation must not call complete()")

    async def stream(
        self, messages: Sequence[dict[str, str]], *, model: str, timeout: float
    ) -> AsyncIterator[ChatDelta]:
        self.stream_calls += 1
        yield ChatDelta(sequence=1, content="provider-first|", done=False)
        self.first_delta.set()
        if self.blocking:
            await self.release.wait()
        yield ChatDelta(sequence=2, content="provider-second", done=False)
        self.completed = True
        yield ChatDelta(sequence=3, content="", done=True)


def wait_for_task(client: TestClient, task_id: str, headers: dict[str, str]) -> dict[str, object]:
    for _ in range(100):
        response = client.get(f"/api/v1/ingestion-tasks/{task_id}", headers=headers)
        assert response.status_code == 200
        payload = response.json()
        if payload["status"] in {"completed", "failed", "partial_failed"}:
            return payload
        time.sleep(0.02)
    pytest.fail("ingestion task did not terminate")


@pytest.mark.acceptance
def test_upload_ingest_retrieve_cite_and_history(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    upload = client.post(
        "/api/v1/documents",
        headers=admin_headers,
        data={"title": "星河保温杯保修政策"},
        files={
            "file": (
                "policy.txt",
                "星河保温杯提供两年保修。保修期内非人为损坏可以免费换新。".encode(),
                "text/plain",
            )
        },
    )
    assert upload.status_code == 202
    task = wait_for_task(client, upload.json()["task_id"], admin_headers)
    assert task["status"] == "completed"

    session = client.post("/api/v1/sessions", headers=admin_headers)
    session_id = session.json()["session_id"]
    answer = client.post(
        "/api/v1/chat",
        headers=admin_headers,
        json={"session_id": session_id, "query": "星河保温杯保修政策是什么"},
    )
    assert answer.status_code == 200
    payload = answer.json()
    assert payload["status"] == "answered"
    assert payload["citations"][0]["document_title"] == "星河保温杯保修政策"
    assert "[C1]" in payload["answer"]
    checkpoint = client.portal.call(client.app.state.runtime.checkpoint.get, "default", session_id)
    assert checkpoint is not None
    assert checkpoint.final_answer == payload["answer"]

    history = client.get(f"/api/v1/sessions/{session_id}/history", headers=admin_headers)
    assert [item["role"] for item in history.json()["items"]] == ["user", "assistant"]


@pytest.mark.acceptance
def test_document_version_activation_and_inactivation(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    first = client.post(
        "/api/v1/documents",
        headers=admin_headers,
        data={"title": "版本化政策"},
        files={"file": ("policy.txt", "旧政策内容".encode(), "text/plain")},
    )
    first_task = wait_for_task(client, first.json()["task_id"], admin_headers)
    document_id = str(first_task["document_id"])
    second = client.post(
        f"/api/v1/documents/{document_id}/versions",
        headers=admin_headers,
        files={"file": ("policy.txt", "新政策内容与有效规则".encode(), "text/plain")},
    )
    assert second.status_code == 202
    second_task = wait_for_task(client, second.json()["task_id"], admin_headers)
    assert second_task["status"] == "completed"

    documents = client.get("/api/v1/documents", headers=admin_headers)
    assert documents.status_code == 200
    assert documents.json()["items"][0]["document_id"] == document_id
    versions = client.get(f"/api/v1/documents/{document_id}/versions", headers=admin_headers)
    assert versions.status_code == 200
    rows = versions.json()["items"]
    assert [row["version"] for row in rows] == [1, 2]
    assert rows[0]["valid_until"] is not None
    assert rows[1]["valid_until"] is None
    assert "object_key" not in rows[0]

    deleted = client.delete(f"/api/v1/documents/{document_id}", headers=admin_headers)
    assert deleted.status_code == 204
    repository = client.app.state.runtime.repository
    stored = repository.documents[document_id]
    assert stored.status.value == "inactive"
    identity_token = issue_token(client)
    identity_headers = {"Authorization": f"Bearer {identity_token}"}
    answer = client.post(
        "/api/v1/chat", headers=identity_headers, json={"query": "版本化政策是什么"}
    )
    assert answer.status_code == 200
    assert answer.json()["status"] == "refused"


@pytest.mark.acceptance
def test_all_agent_paths_and_fake_boundaries(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    cases = [
        ("怎么开发票", "faq", "real"),
        ("订单 DEMO-1001", "order", "fake"),
        ("物流 DEMO-1001", "logistics", "fake"),
        ("退款 DEMO-1001 12.50", "refund", "fake"),
        ("我要转人工", "escalation", "real"),
    ]
    for query, intent, source in cases:
        response = client.post("/api/v1/chat", headers=admin_headers, json={"query": query})
        assert response.status_code == 200, response.text
        assert response.json()["intent"] == intent
        assert response.json()["source"] == source
    refund = client.post(
        "/api/v1/chat",
        headers=admin_headers,
        json={"query": "退款 DEMO-1001 12.50"},
    ).json()
    assert "草单" in refund["answer"]
    assert "真实退款" in refund["answer"]
    audit = client.app.state.runtime.audit
    assert isinstance(audit, InMemoryAudit)
    event_types = [event_type for event_type, _ in audit.records]
    assert "agent.step" in event_types
    assert "tool.refund.calculate.v1" in event_types
    assert "tool.refund.create_draft.v1" in event_types


@pytest.mark.acceptance
def test_sse_order_ids_and_single_end(client: TestClient, admin_headers: dict[str, str]) -> None:
    response = client.post(
        "/api/v1/chat/stream",
        headers=admin_headers,
        json={"query": "物流 DEMO-1001"},
    )
    assert response.status_code == 200
    events = [line[7:] for line in response.text.splitlines() if line.startswith("event: ")]
    assert events[0] == "start"
    assert "delta" in events
    assert "status" in events
    assert events[-1] == "end"
    assert events.count("end") == 1
    data = [
        __import__("json").loads(line[6:])
        for line in response.text.splitlines()
        if line.startswith("data: ")
    ]
    assert [item["sequence"] for item in data] == list(range(1, len(data) + 1))


@pytest.mark.acceptance
def test_provider_delta_reaches_sse_before_full_completion_and_matches_history(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    upload = client.post(
        "/api/v1/documents",
        headers=admin_headers,
        data={"title": "流式政策"},
        files={"file": ("stream.txt", "流式政策有效期为两年。".encode(), "text/plain")},
    )
    assert wait_for_task(client, upload.json()["task_id"], admin_headers)["status"] == "completed"
    runtime = client.app.state.runtime

    async def prove_early_delta() -> None:
        provider = ProbeStreamingModel(blocking=True)
        runtime.retrieval.chat_model = provider
        seen: list[str] = []

        async def on_delta(content: str) -> None:
            seen.append(content)

        task = asyncio.create_task(
            runtime.retrieval.answer(
                IdentityContext(
                    tenant_id="default",
                    user_id="demo-user",
                    roles=frozenset({"user", "admin"}),
                ),
                "流式政策是什么",
                on_delta=on_delta,
            )
        )
        await asyncio.wait_for(provider.first_delta.wait(), timeout=1)
        await asyncio.sleep(0)
        assert seen == ["provider-first|"]
        assert not task.done()
        assert provider.completed is False
        provider.release.set()
        answer, _, _ = await task
        assert answer == "provider-first|provider-second [C1]"
        assert provider.stream_calls == 1

    client.portal.call(prove_early_delta)

    provider = ProbeStreamingModel()
    runtime.retrieval.chat_model = provider
    session_id = client.post("/api/v1/sessions", headers=admin_headers).json()["session_id"]
    response = client.post(
        "/api/v1/chat/stream",
        headers=admin_headers,
        json={"session_id": session_id, "query": "流式政策是什么"},
    )
    assert response.status_code == 200
    pairs: list[tuple[str, dict[str, object]]] = []
    event_name: str | None = None
    for line in response.text.splitlines():
        if line.startswith("event: "):
            event_name = line[7:]
        elif line.startswith("data: ") and event_name is not None:
            pairs.append((event_name, json.loads(line[6:])))
    deltas = [str(data["content"]) for event, data in pairs if event == "delta"]
    assert deltas == ["provider-first|", "provider-second", " [C1]"]
    assert [event for event, _ in pairs].count("end") == 1
    assert [int(data["sequence"]) for _, data in pairs] == list(range(1, len(pairs) + 1))
    history = client.get(f"/api/v1/sessions/{session_id}/history", headers=admin_headers)
    assert "".join(deltas) == history.json()["items"][-1]["content"]


@pytest.mark.acceptance
def test_stream_cancellation_aborts_turn_without_assistant_message(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    upload = client.post(
        "/api/v1/documents",
        headers=admin_headers,
        data={"title": "取消测试政策"},
        files={"file": ("cancel.txt", "取消测试政策内容。".encode(), "text/plain")},
    )
    assert wait_for_task(client, upload.json()["task_id"], admin_headers)["status"] == "completed"
    runtime = client.app.state.runtime

    async def cancel_in_flight() -> None:
        identity = IdentityContext(
            tenant_id="default",
            user_id="demo-user",
            roles=frozenset({"user", "admin"}),
        )
        session = await runtime.sessions.create(identity)
        body = ChatRequest(
            session_id=session.session_id,
            client_turn_id="client-turn-cancel-stream-0001",
            query="取消测试政策是什么",
        )
        claim = await runtime.sessions.begin_turn(
            identity,
            session.session_id,
            client_turn_id=body.client_turn_id,
            request_id="request-cancel-stream-0001",
            query=body.query,
        )
        provider = ProbeStreamingModel(blocking=True)
        runtime.retrieval.chat_model = provider
        emitter = RunEventEmitter(
            request_id=claim.request_id,
            run_id=claim.run_id,
            session_id=claim.session_id,
        )
        task = asyncio.create_task(
            _execute_chat_claim(
                runtime,
                identity,
                body,
                session,
                claim,
                event_sink=emitter.emit,
            )
        )
        await asyncio.wait_for(provider.first_delta.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        stored = await runtime.sessions.get(identity, session.session_id)
        assert stored.active_run_id is None
        assert [message.role for message in stored.messages] == ["user"]
        assert stored.turns[body.client_turn_id].status is TurnStatus.CANCELLED

    client.portal.call(cancel_in_flight)


@pytest.mark.acceptance
def test_chat_client_turn_id_replays_without_duplicate_side_effects(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    session_id = client.post("/api/v1/sessions", headers=admin_headers).json()["session_id"]
    body = {
        "session_id": session_id,
        "client_turn_id": "client-turn-idempotency-0001",
        "query": "怎么开发票",
    }
    first = client.post("/api/v1/chat", headers=admin_headers, json=body)
    second = client.post("/api/v1/chat", headers=admin_headers, json=body)
    assert first.status_code == second.status_code == 200
    assert first.json()["client_turn_id"] == body["client_turn_id"]
    assert second.json() == first.json()
    history = client.get(f"/api/v1/sessions/{session_id}/history", headers=admin_headers)
    assert [item["role"] for item in history.json()["items"]] == ["user", "assistant"]
    record = client.app.state.runtime.sessions.sessions[session_id]
    assert record.revision == 2
    assert record.active_run_id is None


@pytest.mark.acceptance
def test_user_governed_memory_create_correct_disable_delete_and_isolate(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    created = client.post(
        "/api/v1/memories",
        headers=admin_headers,
        json={
            "category": "preference",
            "key": "response-style",
            "value": "简洁回答",
            "source_turn_id": "client-turn-memory-0001",
            "explicitly_confirmed": True,
        },
    )
    assert created.status_code == 201
    memory_id = created.json()["memory_id"]
    assert created.json()["version"] == 1
    listed = client.get("/api/v1/memories", headers=admin_headers)
    assert [item["value"] for item in listed.json()["items"]] == ["简洁回答"]

    other_headers = {"Authorization": f"Bearer {issue_token(client, user_id='other-user')}"}
    assert client.get("/api/v1/memories", headers=other_headers).json()["items"] == []
    assert client.delete(f"/api/v1/memories/{memory_id}", headers=other_headers).status_code == 404

    corrected = client.patch(
        f"/api/v1/memories/{memory_id}",
        headers=admin_headers,
        json={
            "value": "详细回答",
            "source_turn_id": "client-turn-memory-0002",
            "explicitly_confirmed": True,
        },
    )
    assert corrected.status_code == 200
    corrected_id = corrected.json()["memory_id"]
    assert corrected.json()["version"] == 2
    disabled = client.put(
        "/api/v1/memories/settings",
        headers=admin_headers,
        json={"enabled": False},
    )
    assert disabled.json() == {
        "enabled": False,
        "auto_write_enabled": False,
        "updated_at": disabled.json()["updated_at"],
    }
    assert client.get("/api/v1/memories", headers=admin_headers).json()["items"] == []
    client.put(
        "/api/v1/memories/settings",
        headers=admin_headers,
        json={"enabled": True},
    )
    assert (
        client.delete(f"/api/v1/memories/{corrected_id}", headers=admin_headers).status_code == 204
    )
    assert client.get("/api/v1/memories", headers=admin_headers).json()["items"] == []

    unconfirmed = client.post(
        "/api/v1/memories",
        headers=admin_headers,
        json={
            "category": "preference",
            "key": "language",
            "value": "中文",
            "source_turn_id": "client-turn-memory-0003",
            "explicitly_confirmed": False,
        },
    )
    assert unconfirmed.status_code == 422
    sensitive = client.post(
        "/api/v1/memories",
        headers=admin_headers,
        json={
            "category": "confirmed_fact",
            "key": "secret",
            "value": "api_key=sk-sensitive-value",
            "source_turn_id": "client-turn-memory-0004",
            "explicitly_confirmed": True,
        },
    )
    assert sensitive.status_code == 403
    audit = client.app.state.runtime.audit
    assert "简洁回答" not in str(audit.records)
    assert "详细回答" not in str(audit.records)


@pytest.mark.acceptance
def test_completed_run_persists_generation_manifest(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    response = client.post("/api/v1/chat", headers=admin_headers, json={"query": "怎么开发票"})
    assert response.status_code == 200
    record = client.app.state.runtime.sessions.sessions[response.json()["session_id"]]
    assert len(record.generation_manifests) == 1
    manifest = record.generation_manifests[0]
    assert manifest.run_id == response.json()["run_id"]
    assert manifest.prompt_bundle_version == "prompt-bundle-v1"
    assert set(manifest.prompt_hashes) == {"agent-safety", "rag-answer"}
    assert manifest.context_policy_version == "context-policy-v1"


@pytest.mark.acceptance
def test_multi_turn_context_keeps_constraints_and_resolves_reference(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    upload = client.post(
        "/api/v1/documents",
        headers=admin_headers,
        data={"title": "星河保温杯保修说明"},
        files={
            "file": (
                "warranty.txt",
                "星河保温杯的保修期是两年，保修期内非人为损坏可以免费换新。".encode(),
                "text/plain",
            )
        },
    )
    assert wait_for_task(client, upload.json()["task_id"], admin_headers)["status"] == "completed"
    session_id = client.post("/api/v1/sessions", headers=admin_headers).json()["session_id"]
    constraint_turn = client.post(
        "/api/v1/chat",
        headers=admin_headers,
        json={
            "session_id": session_id,
            "client_turn_id": "client-turn-context-0001",
            "query": "预算不超过500元，地区限定上海，而且不要执行写操作。",
        },
    )
    assert constraint_turn.status_code == 200
    first = client.post(
        "/api/v1/chat",
        headers=admin_headers,
        json={
            "session_id": session_id,
            "client_turn_id": "client-turn-context-0002",
            "query": "星河保温杯保修说明是什么？",
        },
    )
    second = client.post(
        "/api/v1/chat",
        headers=admin_headers,
        json={
            "session_id": session_id,
            "client_turn_id": "client-turn-context-0003",
            "query": "这个保修期多久？",
        },
    )
    assert first.status_code == second.status_code == 200
    assert first.json()["citations"]
    assert second.json()["citations"]
    record = client.app.state.runtime.sessions.sessions[session_id]
    assert record.conversation_state is not None
    constraints = {
        (item.kind, item.name): item.value for item in record.conversation_state.constraints
    }
    assert constraints[("amount", "amount_limit")] == "500 CNY"
    assert constraints[("region", "region")] == "上海"
    assert any(item.kind == "negation" for item in record.conversation_state.constraints)
    assert len(record.context_manifests) == 3
    assert all(
        manifest.actual_input_tokens <= manifest.input_budget_tokens
        for manifest in record.context_manifests
    )


@pytest.mark.acceptance
def test_auth_rbac_cross_user_and_uniform_errors(client: TestClient) -> None:
    unauthorized = client.post("/api/v1/chat", json={"query": "hello"})
    assert unauthorized.status_code == 401
    assert set(unauthorized.json()) == {"error_code", "message", "request_id", "retryable"}

    user_token = issue_token(client, user_id="owner")
    other_token = issue_token(client, user_id="other")
    owner = {"Authorization": f"Bearer {user_token}"}
    other = {"Authorization": f"Bearer {other_token}"}
    session_id = client.post("/api/v1/sessions", headers=owner).json()["session_id"]
    cross_user = client.get(f"/api/v1/sessions/{session_id}", headers=other)
    assert cross_user.status_code == 404
    upload = client.post(
        "/api/v1/documents",
        headers=owner,
        data={"title": "no"},
        files={"file": ("x.txt", b"data", "text/plain")},
    )
    assert upload.status_code == 403
    debug = client.post("/api/v1/admin/retrieval-debug", headers=owner, json={"query": "policy"})
    assert debug.status_code == 403


@pytest.mark.acceptance
def test_request_id_validation_upload_security_and_closed_session(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    supplied = "client-request-123"
    live = client.get("/api/v1/health/live", headers={"X-Request-ID": supplied})
    assert live.headers["X-Request-ID"] == supplied
    replaced = client.get("/api/v1/health/live", headers={"X-Request-ID": "bad id"})
    assert replaced.headers["X-Request-ID"] != "bad id"
    dangerous = client.post(
        "/api/v1/documents",
        headers=admin_headers,
        data={"title": "bad"},
        files={"file": ("malware.exe", b"MZ", "application/octet-stream")},
    )
    assert dangerous.status_code == 422
    session_id = client.post("/api/v1/sessions", headers=admin_headers).json()["session_id"]
    assert client.delete(f"/api/v1/sessions/{session_id}", headers=admin_headers).status_code == 204
    closed = client.post(
        "/api/v1/chat",
        headers=admin_headers,
        json={"session_id": session_id, "query": "hello"},
    )
    assert closed.status_code == 409


@pytest.mark.acceptance
def test_readiness_debug_and_idempotent_fake_approval_callback(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    ready = client.get("/api/v1/health/ready")
    assert ready.status_code == 200
    assert ready.json()["ready"] is True
    debug = client.post(
        "/api/v1/admin/retrieval-debug",
        headers=admin_headers,
        json={"query": "unknown policy"},
    )
    assert debug.status_code == 200
    assert set(debug.json()["branches"]) >= {"dense", "graph", "keyword"}

    refund = client.post(
        "/api/v1/chat",
        headers=admin_headers,
        json={"query": "退款10元 DEMO-1001"},
    )
    assert refund.status_code == 200
    repository = client.app.state.runtime.repository
    assert isinstance(repository, InMemoryKnowledgeRepository)
    draft_id = next(iter(repository.drafts))
    request_id = "approval-request-1"
    material = f"{request_id}:{draft_id}:approved".encode()
    signature = hmac.new(
        b"test-approval-secret-material-long-enough", material, hashlib.sha256
    ).hexdigest()
    body = {
        "request_id": request_id,
        "draft_id": draft_id,
        "decision": "approved",
        "actor_id": "admin",
        "signature": signature,
    }
    first = client.post("/api/v1/approvals/fake-callback", headers=admin_headers, json=body)
    second = client.post("/api/v1/approvals/fake-callback", headers=admin_headers, json=body)
    assert first.status_code == 200
    assert first.json()["accepted"] is True
    assert first.json()["duplicate"] is False
    assert first.json()["resumed"] is True
    assert first.json()["executed"] is False
    assert first.json()["recovery_source"] == "langgraph"
    assert "不会执行真实退款" in first.json()["final_answer"]
    assert second.json()["duplicate"] is True
    checkpoint = client.portal.call(
        client.app.state.runtime.checkpoint.get,
        "default",
        refund.json()["session_id"],
    )
    assert checkpoint is not None
    assert checkpoint.needs_approval is False
    assert checkpoint.approval_status == "approved"
    assert len(repository.drafts) == 1
    conflicting_request_id = "approval-request-conflict-1"
    conflicting_material = f"{conflicting_request_id}:{draft_id}:rejected".encode()
    conflicting_signature = hmac.new(
        b"test-approval-secret-material-long-enough",
        conflicting_material,
        hashlib.sha256,
    ).hexdigest()
    conflict = client.post(
        "/api/v1/approvals/fake-callback",
        headers=admin_headers,
        json={
            "request_id": conflicting_request_id,
            "draft_id": draft_id,
            "decision": "rejected",
            "actor_id": "admin",
            "signature": conflicting_signature,
        },
    )
    assert conflict.status_code == 409


@pytest.mark.acceptance
def test_admin_knowledge_quality_report_finds_conflict_without_deleting_documents(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    for content in ("同名政策规定保修期为一年。", "同名政策规定保修期为两年。"):
        upload = client.post(
            "/api/v1/documents",
            headers=admin_headers,
            data={"title": "冲突 政策"},
            files={"file": ("conflict.txt", content.encode(), "text/plain")},
        )
        assert wait_for_task(client, upload.json()["task_id"], admin_headers)["status"] == (
            "completed"
        )
    repository = client.app.state.runtime.repository
    before = len(repository.documents)
    response = client.get("/api/v1/admin/knowledge-quality", headers=admin_headers)
    assert response.status_code == 200
    assert "conflict" in {item["issue_type"] for item in response.json()["issues"]}
    assert len(repository.documents) == before


@pytest.mark.acceptance
def test_fake_approval_recovers_from_mysql_snapshot_when_graph_checkpoint_is_lost(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    refund = client.post(
        "/api/v1/chat",
        headers=admin_headers,
        json={
            "client_turn_id": "client-turn-recovery-0001",
            "query": "退款10元 DEMO-1001",
        },
    )
    assert refund.status_code == 200
    repository = client.app.state.runtime.repository
    assert isinstance(repository, InMemoryKnowledgeRepository)
    draft_id = next(iter(repository.drafts))
    checkpointer = client.app.state.runtime.langgraph_checkpointer
    checkpointer.storage.clear()
    checkpointer.writes.clear()
    checkpointer.blobs.clear()

    request_id = "approval-recovery-request-1"
    material = f"{request_id}:{draft_id}:rejected".encode()
    signature = hmac.new(
        b"test-approval-secret-material-long-enough", material, hashlib.sha256
    ).hexdigest()
    response = client.post(
        "/api/v1/approvals/fake-callback",
        headers=admin_headers,
        json={
            "request_id": request_id,
            "draft_id": draft_id,
            "decision": "rejected",
            "actor_id": "admin",
            "signature": signature,
        },
    )
    assert response.status_code == 200
    assert response.json()["recovery_source"] == "mysql_snapshot"
    assert response.json()["executed"] is False
    assert "没有执行任何真实退款" in response.json()["final_answer"]
    checkpoint = client.portal.call(
        client.app.state.runtime.checkpoint.get,
        "default",
        refund.json()["session_id"],
    )
    assert checkpoint is not None
    assert checkpoint.approval_status == "rejected"
    assert len(repository.drafts) == 1
