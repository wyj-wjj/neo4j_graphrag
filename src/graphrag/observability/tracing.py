"""Trace Port implementations; payload content is excluded by construction."""

from __future__ import annotations

from contextlib import AbstractContextManager, nullcontext
from typing import Any

from graphrag.observability.redaction import redact


class NoopTrace:
    def span(
        self, name: str, attributes: dict[str, Any] | None = None
    ) -> AbstractContextManager[None]:
        return nullcontext()


class LangfuseTrace:
    def __init__(self, client: Any) -> None:
        self.client = client

    def span(self, name: str, attributes: dict[str, Any] | None = None) -> Any:
        safe_attributes = redact(attributes or {})
        return self.client.start_as_current_observation(
            name=name,
            as_type="span",
            metadata=safe_attributes,
        )

    async def close(self) -> None:
        self.client.flush()
        self.client.shutdown()
