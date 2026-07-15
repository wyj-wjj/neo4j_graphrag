"""Strongly typed identifiers and a monotonic UUIDv7-compatible generator."""

from __future__ import annotations

import secrets
import time
from typing import NewType
from uuid import UUID

TenantId = NewType("TenantId", str)
UserId = NewType("UserId", str)
SessionId = NewType("SessionId", str)
RunId = NewType("RunId", str)
DocumentId = NewType("DocumentId", str)
DocumentVersionId = NewType("DocumentVersionId", str)
ChunkId = NewType("ChunkId", str)
TaskId = NewType("TaskId", str)
EventId = NewType("EventId", str)
ToolCallId = NewType("ToolCallId", str)


def uuid7() -> UUID:
    """Generate an RFC 9562 UUIDv7 without requiring Python 3.14."""

    timestamp_ms = int(time.time() * 1000) & ((1 << 48) - 1)
    rand_a = secrets.randbits(12)
    rand_b = secrets.randbits(62)
    value = (timestamp_ms << 80) | (0x7 << 76) | (rand_a << 64)
    value |= (0b10 << 62) | rand_b
    return UUID(int=value)


def new_id() -> str:
    return str(uuid7())


def validate_id(value: str) -> str:
    parsed = UUID(value)
    if parsed.version != 7:
        msg = "identifier must be UUIDv7"
        raise ValueError(msg)
    return str(parsed)
