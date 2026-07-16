"""Versioned event envelope shared by in-process and future Kafka publishers."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import Field

from graphrag.domain.ids import new_id
from graphrag.domain.models import StrictModel, utc_now

RunEventType = Literal[
    "start",
    "route",
    "retrieving",
    "delta",
    "citation",
    "status",
    "error",
    "end",
]


class EventEnvelope(StrictModel):
    event_id: str = Field(default_factory=new_id)
    event_type: Literal[
        "document.received",
        "document.version.received",
        "document.chunked",
        "vector.index.requested",
        "graph.index.requested",
        "document.indexed",
        "document.reindexed",
        "document.failed",
        "document.inactivated",
        "action.draft.created",
    ]
    event_version: int = Field(default=1, ge=1)
    aggregate_version: int = Field(default=1, ge=1)
    tenant_id: str
    aggregate_id: str
    occurred_at: datetime = Field(default_factory=utc_now)
    trace_id: str
    payload_summary: dict[str, Any] = Field(default_factory=dict)


class OutboxRecord(StrictModel):
    """One unpublished event held under a short database lease."""

    event: EventEnvelope
    retry_count: int = Field(default=0, ge=0)
    available_at: datetime
    lease_owner: str = Field(min_length=1, max_length=128)
    lease_expires_at: datetime


class InboxClaimResult(StrictModel):
    disposition: Literal["claimed", "duplicate", "busy", "stale"]
    attempt_count: int = Field(default=0, ge=0)


class RunEvent(StrictModel):
    """Versioned event emitted while one Agent run is in progress."""

    event_version: int = Field(default=1, ge=1)
    event_type: RunEventType
    request_id: str
    run_id: str
    session_id: str
    sequence: int = Field(ge=1)
    data: dict[str, Any] = Field(default_factory=dict)
