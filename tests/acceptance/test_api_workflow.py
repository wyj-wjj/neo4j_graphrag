from __future__ import annotations

import hashlib
import hmac
import time

import pytest
from fastapi.testclient import TestClient
from tests.conftest import issue_token

from graphrag.infrastructure.memory import InMemoryAudit, InMemoryKnowledgeRepository


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
    assert first.json() == {
        "request_id": request_id,
        "accepted": True,
        "duplicate": False,
        "executed": False,
    }
    assert second.json()["duplicate"] is True
