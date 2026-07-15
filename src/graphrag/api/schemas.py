"""Transport-only Pydantic schemas, intentionally separate from ORM models."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from graphrag.domain.models import (
    AgentIntent,
    AnswerStatus,
    ChatMessage,
    Citation,
    DocumentStatus,
    IngestionStatus,
    SourceKind,
)


class APISchema(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ErrorBody(APISchema):
    error_code: str
    message: str
    request_id: str
    retryable: bool = False


class DevTokenRequest(APISchema):
    user_id: str = Field(default="demo-user", min_length=1, max_length=128)
    roles: frozenset[Literal["user", "admin", "knowledge_admin"]] = frozenset({"user"})
    expires_minutes: int = Field(default=60, ge=1, le=120)


class TokenResponse(APISchema):
    access_token: str
    token_type: Literal["bearer"] = "bearer"  # noqa: S105 - OAuth token type, not a secret.
    expires_in: int


class SessionCreateResponse(APISchema):
    session_id: str
    created: bool = True


class SessionResponse(APISchema):
    session_id: str
    user_id: str
    closed: bool
    messages: tuple[ChatMessage, ...]


class HistoryResponse(APISchema):
    session_id: str
    items: tuple[ChatMessage, ...]
    offset: int
    limit: int
    total: int


class ChatRequest(APISchema):
    session_id: str | None = None
    query: str = Field(min_length=1, max_length=4000)


class ChatResponse(APISchema):
    request_id: str
    run_id: str
    session_id: str
    status: AnswerStatus
    answer: str
    intent: AgentIntent
    citations: tuple[Citation, ...] = ()
    source: SourceKind


class IngestionTaskResponse(APISchema):
    task_id: str
    tenant_id: str
    document_id: str
    version_id: str | None
    status: IngestionStatus
    attempt: int
    error_code: str | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime


class DocumentResponse(APISchema):
    document_id: str
    tenant_id: str
    title: str
    status: DocumentStatus
    created_at: datetime
    updated_at: datetime


class DocumentListResponse(APISchema):
    items: tuple[DocumentResponse, ...]
    offset: int
    limit: int


class DocumentVersionResponse(APISchema):
    version_id: str
    document_id: str
    tenant_id: str
    version: int
    content_hash: str
    mime_type: str
    size_bytes: int
    valid_from: datetime
    valid_until: datetime | None
    created_at: datetime


class DocumentVersionListResponse(APISchema):
    items: tuple[DocumentVersionResponse, ...]


class DebugRequest(APISchema):
    query: str = Field(min_length=1, max_length=4000)


class DebugCandidate(APISchema):
    citation_id: str
    chunk_id: str
    document_id: str
    document_title: str
    score: float
    sources: tuple[str, ...]


class DebugResponse(APISchema):
    rewritten_query: str
    branches: dict[str, str]
    candidates: tuple[DebugCandidate, ...]


class ApprovalCallbackRequest(APISchema):
    request_id: str = Field(min_length=8, max_length=128)
    draft_id: str
    decision: Literal["approved", "rejected"]
    actor_id: str = Field(min_length=1, max_length=128)
    signature: str = Field(pattern=r"^[0-9a-f]{64}$")


class ApprovalCallbackResponse(APISchema):
    request_id: str
    accepted: bool
    duplicate: bool
    executed: bool = False


class ReadinessResponse(APISchema):
    ready: bool
    dependencies: dict[str, dict[str, Any]]
