"""Ordered event stream shared by Provider deltas and API lifecycle events."""

from __future__ import annotations

import asyncio
from typing import Any

from graphrag.domain.events import RunEvent, RunEventType


class RunEventEmitter:
    """Assign exactly one monotonic sequence to every event in a run."""

    def __init__(self, *, request_id: str, run_id: str, session_id: str) -> None:
        self.request_id = request_id
        self.run_id = run_id
        self.session_id = session_id
        self._sequence = 0
        self._queue: asyncio.Queue[RunEvent] = asyncio.Queue()
        self._lock = asyncio.Lock()
        self.delta_count = 0

    @property
    def sequence(self) -> int:
        return self._sequence

    async def emit(self, event_type: RunEventType, data: dict[str, Any] | None = None) -> RunEvent:
        async with self._lock:
            self._sequence += 1
            event = RunEvent(
                event_type=event_type,
                request_id=self.request_id,
                run_id=self.run_id,
                session_id=self.session_id,
                sequence=self._sequence,
                data=data or {},
            )
            if event.event_type == "delta":
                self.delta_count += 1
            await self._queue.put(event)
            return event

    async def get(self) -> RunEvent:
        return await self._queue.get()

    def get_nowait(self) -> RunEvent:
        return self._queue.get_nowait()

    def empty(self) -> bool:
        return self._queue.empty()
