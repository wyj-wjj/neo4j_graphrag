"""Phase-one REST and SSE endpoints under /api/v1."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from collections.abc import AsyncIterator
from contextlib import suppress
from typing import Annotated, Any, cast

from fastapi import APIRouter, Depends, File, Form, Query, Request, UploadFile, status
from fastapi.responses import JSONResponse, StreamingResponse

from graphrag.api.auth import JWTAuth, get_identity, require_roles
from graphrag.api.schemas import (
    ApprovalCallbackRequest,
    ApprovalCallbackResponse,
    ChatRequest,
    ChatResponse,
    DebugCandidate,
    DebugRequest,
    DebugResponse,
    DevTokenRequest,
    DocumentListResponse,
    DocumentResponse,
    DocumentVersionListResponse,
    DocumentVersionResponse,
    HistoryResponse,
    IngestionTaskResponse,
    ReadinessResponse,
    SessionCreateResponse,
    SessionResponse,
    TokenResponse,
)
from graphrag.application.container import Runtime
from graphrag.config import AppEnvironment
from graphrag.domain.errors import (
    AuthorizationError,
    ConflictError,
    DependencyError,
    NotFoundError,
    OperationTimeoutError,
    RateLimitError,
)
from graphrag.domain.events import EventEnvelope
from graphrag.domain.models import IdentityContext
from graphrag.observability.redaction import redact_text

router = APIRouter(prefix="/api/v1")
Identity = Annotated[IdentityContext, Depends(get_identity)]


def _runtime(request: Request) -> Runtime:
    return cast(Runtime, request.app.state.runtime)


async def _rate_limit(
    runtime: Runtime, identity: IdentityContext, category: str, limit: int
) -> None:
    key = f"rate:{identity.tenant_id}:{identity.user_id}:{category}"
    allowed = await runtime.rate_limiter.allow(key, limit=limit, window_seconds=60)
    if not allowed:
        raise RateLimitError()


@router.get("/health/live", tags=["health"])
async def liveness() -> dict[str, str]:
    return {"status": "alive"}


@router.get(
    "/health/ready",
    response_model=ReadinessResponse,
    tags=["health"],
)
async def readiness(request: Request) -> JSONResponse:
    dependencies = await _runtime(request).readiness()
    ready = all(bool(item.get("ready")) for item in dependencies.values())
    payload = ReadinessResponse(ready=ready, dependencies=dependencies)
    return JSONResponse(
        status_code=200 if ready else 503,
        content=payload.model_dump(mode="json"),
    )


@router.post("/auth/dev-token", response_model=TokenResponse, tags=["auth"])
async def dev_token(request: Request, body: DevTokenRequest) -> TokenResponse:
    runtime = _runtime(request)
    if runtime.settings.app_env not in {AppEnvironment.DEV, AppEnvironment.TEST}:
        raise AuthorizationError("生产环境不存在本地测试身份入口")
    auth: JWTAuth = request.app.state.auth
    token, expires = auth.issue_dev_token(
        user_id=body.user_id,
        roles=frozenset(body.roles),
        expires_minutes=body.expires_minutes,
    )
    return TokenResponse(access_token=token, expires_in=expires)


@router.post(
    "/sessions",
    response_model=SessionCreateResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["sessions"],
)
async def create_session(request: Request, identity: Identity) -> SessionCreateResponse:
    record = await _runtime(request).sessions.create(identity)
    return SessionCreateResponse(session_id=record.session_id)


@router.get("/sessions/{session_id}", response_model=SessionResponse, tags=["sessions"])
async def get_session(request: Request, session_id: str, identity: Identity) -> SessionResponse:
    record = await _runtime(request).sessions.get(identity, session_id)
    return SessionResponse(
        session_id=record.session_id,
        user_id=record.user_id,
        closed=record.closed,
        messages=tuple(record.messages),
    )


@router.get(
    "/sessions/{session_id}/history",
    response_model=HistoryResponse,
    tags=["sessions"],
)
async def get_history(
    request: Request,
    session_id: str,
    identity: Identity,
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> HistoryResponse:
    record = await _runtime(request).sessions.get(identity, session_id)
    return HistoryResponse(
        session_id=session_id,
        items=tuple(record.messages[offset : offset + limit]),
        offset=offset,
        limit=limit,
        total=len(record.messages),
    )


@router.delete("/sessions/{session_id}", status_code=204, tags=["sessions"])
async def close_session(request: Request, session_id: str, identity: Identity) -> None:
    runtime = _runtime(request)
    await runtime.sessions.close(identity, session_id)
    await runtime.checkpoint.delete(identity.tenant_id, session_id)


@router.post(
    "/documents",
    response_model=IngestionTaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["knowledge"],
)
async def upload_document(
    request: Request,
    identity: Identity,
    title: Annotated[str, Form(min_length=1, max_length=500)],
    file: Annotated[UploadFile, File()],
) -> IngestionTaskResponse:
    require_roles(identity, "admin", "knowledge_admin")
    runtime = _runtime(request)
    await _rate_limit(
        runtime,
        identity,
        "upload",
        runtime.settings.upload_rate_limit_per_minute,
    )
    content = await file.read(runtime.settings.upload_max_bytes + 1)
    task = await runtime.ingestion.receive(
        identity,
        filename=file.filename or "",
        content=content,
        title=title,
        trace_id=request.state.request_id,
    )
    return IngestionTaskResponse.model_validate(task.model_dump(mode="python"))


@router.get("/documents", response_model=DocumentListResponse, tags=["knowledge"])
async def list_documents(
    request: Request,
    identity: Identity,
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> DocumentListResponse:
    require_roles(identity, "admin", "knowledge_admin")
    rows = await _runtime(request).repository.list_documents(
        identity.tenant_id, offset=offset, limit=limit
    )
    return DocumentListResponse(
        items=tuple(DocumentResponse.model_validate(row.model_dump()) for row in rows),
        offset=offset,
        limit=limit,
    )


@router.post(
    "/documents/{document_id}/versions",
    response_model=IngestionTaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["knowledge"],
)
async def upload_document_version(
    request: Request,
    document_id: str,
    identity: Identity,
    file: Annotated[UploadFile, File()],
) -> IngestionTaskResponse:
    require_roles(identity, "admin", "knowledge_admin")
    runtime = _runtime(request)
    await _rate_limit(runtime, identity, "upload", runtime.settings.upload_rate_limit_per_minute)
    content = await file.read(runtime.settings.upload_max_bytes + 1)
    task = await runtime.ingestion.receive_version(
        identity,
        document_id=document_id,
        filename=file.filename or "",
        content=content,
        trace_id=request.state.request_id,
    )
    return IngestionTaskResponse.model_validate(task.model_dump(mode="python"))


@router.get(
    "/documents/{document_id}/versions",
    response_model=DocumentVersionListResponse,
    tags=["knowledge"],
)
async def list_document_versions(
    request: Request, document_id: str, identity: Identity
) -> DocumentVersionListResponse:
    require_roles(identity, "admin", "knowledge_admin")
    rows = await _runtime(request).repository.list_versions(identity.tenant_id, document_id)
    return DocumentVersionListResponse(
        items=tuple(
            DocumentVersionResponse.model_validate(row.model_dump(exclude={"object_key"}))
            for row in rows
        )
    )


@router.delete("/documents/{document_id}", status_code=204, tags=["knowledge"])
async def inactivate_document(request: Request, document_id: str, identity: Identity) -> None:
    require_roles(identity, "admin", "knowledge_admin")
    runtime = _runtime(request)
    versions = await runtime.repository.list_versions(identity.tenant_id, document_id)
    chunks = [
        chunk
        for version in versions
        for chunk in await runtime.repository.list_chunks_for_version(
            identity.tenant_id, version.version_id
        )
    ]
    event = EventEnvelope(
        event_type="document.inactivated",
        tenant_id=identity.tenant_id,
        aggregate_id=document_id,
        trace_id=request.state.request_id,
        payload_summary={"version_count": len(versions), "chunk_count": len(chunks)},
    )
    await runtime.repository.inactivate_document(identity.tenant_id, document_id, event)
    await runtime.publisher.publish(event)
    # MySQL validity is changed first. Derived-index cleanup is best effort and rebuildable.
    with suppress(Exception):
        await runtime.vector_store.delete(identity.tenant_id, [chunk.chunk_id for chunk in chunks])
    for version in versions:
        with suppress(Exception):
            await runtime.graph_store.delete_document_version(
                identity.tenant_id, version.version_id
            )


@router.get(
    "/ingestion-tasks/{task_id}",
    response_model=IngestionTaskResponse,
    tags=["knowledge"],
)
async def get_ingestion_task(
    request: Request, task_id: str, identity: Identity
) -> IngestionTaskResponse:
    task = await _runtime(request).repository.get_task(identity.tenant_id, task_id)
    if task is None:
        raise NotFoundError("入库任务不存在")
    return IngestionTaskResponse.model_validate(task.model_dump(mode="python"))


@router.post(
    "/ingestion-tasks/{task_id}/retry",
    response_model=IngestionTaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["knowledge"],
)
async def retry_ingestion_task(
    request: Request, task_id: str, identity: Identity
) -> IngestionTaskResponse:
    require_roles(identity, "admin", "knowledge_admin")
    task = await _runtime(request).repository.retry_task(identity.tenant_id, task_id)
    return IngestionTaskResponse.model_validate(task.model_dump(mode="python"))


async def _run_chat(request: Request, identity: IdentityContext, body: ChatRequest) -> ChatResponse:
    runtime = _runtime(request)
    await _rate_limit(
        runtime,
        identity,
        "chat",
        runtime.settings.chat_rate_limit_per_minute,
    )
    if body.session_id is None:
        record = await runtime.sessions.create(identity)
    else:
        record = await runtime.sessions.get(identity, body.session_id)
    if record.closed:
        raise ConflictError("会话已关闭")
    try:
        with runtime.trace.span(
            "api.chat",
            {
                "request_id": request.state.request_id,
                "tenant_id": identity.tenant_id,
                "session_id": record.session_id,
            },
        ):
            async with asyncio.timeout(runtime.settings.generation_timeout_seconds + 5):
                result = await runtime.orchestrator.run(
                    identity,
                    request_id=request.state.request_id,
                    session_id=record.session_id,
                    query=body.query,
                )
    except TimeoutError as exc:
        raise OperationTimeoutError("agent") from exc
    await runtime.sessions.append(identity, record.session_id, body.query, result)
    return ChatResponse.model_validate(result.model_dump(mode="python"))


@router.post("/chat", response_model=ChatResponse, tags=["chat"])
async def chat(request: Request, identity: Identity, body: ChatRequest) -> ChatResponse:
    return await _run_chat(request, identity, body)


def _sse(event: str, data: dict[str, Any], *, event_id: str) -> str:
    encoded = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return f"id: {event_id}\nevent: {event}\ndata: {encoded}\n\n"


@router.post("/chat/stream", tags=["chat"])
async def stream_chat(request: Request, identity: Identity, body: ChatRequest) -> StreamingResponse:
    async def events() -> AsyncIterator[str]:
        sequence = 1
        yield _sse(
            "start",
            {"request_id": request.state.request_id, "sequence": sequence},
            event_id=f"{request.state.request_id}:{sequence}",
        )
        try:
            result = await _run_chat(request, identity, body)
            if await request.is_disconnected():
                return
            for offset in range(0, len(result.answer), 24):
                sequence += 1
                yield _sse(
                    "delta",
                    {
                        "run_id": result.run_id,
                        "sequence": sequence,
                        "content": result.answer[offset : offset + 24],
                    },
                    event_id=f"{result.run_id}:{sequence}",
                )
            for citation in result.citations:
                sequence += 1
                yield _sse(
                    "citation",
                    {
                        "run_id": result.run_id,
                        "sequence": sequence,
                        "citation": citation.model_dump(mode="json"),
                    },
                    event_id=f"{result.run_id}:{sequence}",
                )
            sequence += 1
            yield _sse(
                "status",
                {
                    "run_id": result.run_id,
                    "sequence": sequence,
                    "status": result.status.value,
                    "intent": result.intent.value,
                    "source": result.source.value,
                },
                event_id=f"{result.run_id}:{sequence}",
            )
            sequence += 1
            yield _sse(
                "end",
                {"run_id": result.run_id, "sequence": sequence},
                event_id=f"{result.run_id}:{sequence}",
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            sequence += 1
            yield _sse(
                "error",
                {
                    "sequence": sequence,
                    "error_code": getattr(getattr(exc, "code", None), "value", "stream_error"),
                    "message": redact_text(str(exc) or "流式回答失败"),
                },
                event_id=f"{request.state.request_id}:{sequence}",
            )
            sequence += 1
            yield _sse(
                "end",
                {"request_id": request.state.request_id, "sequence": sequence},
                event_id=f"{request.state.request_id}:{sequence}",
            )

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/admin/retrieval-debug", response_model=DebugResponse, tags=["admin"])
async def retrieval_debug(
    request: Request, identity: Identity, body: DebugRequest
) -> DebugResponse:
    require_roles(identity, "admin")
    result = await _runtime(request).retrieval.retrieve(identity, body.query)
    candidates = tuple(
        DebugCandidate(
            citation_id=citation.citation_id,
            chunk_id=evidence.chunk_id,
            document_id=evidence.document_id,
            document_title=evidence.document_title,
            score=evidence.score,
            sources=evidence.sources,
        )
        for citation, evidence in zip(result.citations, result.evidences, strict=True)
    )
    return DebugResponse(
        rewritten_query=result.query,
        branches=result.branch_status,
        candidates=candidates,
    )


@router.post(
    "/approvals/fake-callback",
    response_model=ApprovalCallbackResponse,
    tags=["approval"],
)
async def fake_approval_callback(
    request: Request, identity: Identity, body: ApprovalCallbackRequest
) -> ApprovalCallbackResponse:
    require_roles(identity, "admin")
    runtime = _runtime(request)
    if runtime.settings.app_env not in {AppEnvironment.DEV, AppEnvironment.TEST}:
        raise AuthorizationError("生产环境禁止 Fake 审批回调")
    configured_secret = runtime.settings.fake_approval_secret
    if configured_secret is None:
        raise DependencyError("approval", "Fake 审批签名密钥未配置", retryable=False)
    material = f"{body.request_id}:{body.draft_id}:{body.decision}".encode()
    expected = hmac.new(
        configured_secret.get_secret_value().encode(), material, hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(body.signature, expected):
        raise AuthorizationError("审批回调签名无效")
    draft = await runtime.repository.get_draft(identity.tenant_id, body.draft_id)
    if draft is None:
        raise NotFoundError("审批草单不存在")
    duplicate = body.request_id in runtime.approval_callbacks
    runtime.approval_callbacks.add(body.request_id)
    await runtime.audit.record(
        "approval.fake_callback",
        {
            "request_id": body.request_id,
            "draft_id": body.draft_id,
            "decision": body.decision,
            "duplicate": duplicate,
            "executed": False,
        },
    )
    return ApprovalCallbackResponse(
        request_id=body.request_id,
        accepted=True,
        duplicate=duplicate,
    )
