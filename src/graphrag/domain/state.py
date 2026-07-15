"""Serializable, versioned Agent state with explicit merge semantics."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from graphrag.domain.models import AgentIntent, ChatMessage, Citation, StrictModel


def append_unique(left: list[str], right: list[str]) -> list[str]:
    return list(dict.fromkeys([*left, *right]))


class AgentState(StrictModel):
    state_version: int = 1
    request_id: str
    run_id: str
    session_id: str
    tenant_id: str
    user_id: str
    roles: frozenset[str]
    original_query: str
    query: str
    chat_history: tuple[ChatMessage, ...] = ()
    next_agent: AgentIntent | None = None
    visited_agents: tuple[AgentIntent, ...] = ()
    iteration: int = Field(default=0, ge=0, le=10)
    retrieved_chunk_ids: tuple[str, ...] = ()
    evidence_context: str = ""
    tool_calls: tuple[dict[str, Any], ...] = ()
    draft_action_id: str | None = None
    final_answer: str = ""
    citations: tuple[Citation, ...] = ()
    needs_human: bool = False
    needs_approval: bool = False
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    errors: tuple[str, ...] = ()

    def update_for_agent(self, agent: AgentIntent, **updates: Any) -> AgentState:
        allowed = {
            "retrieved_chunk_ids",
            "evidence_context",
            "tool_calls",
            "draft_action_id",
            "final_answer",
            "citations",
            "needs_human",
            "needs_approval",
            "confidence",
            "errors",
            "next_agent",
        }
        unknown = set(updates) - allowed
        if unknown:
            msg = f"agent attempted to update protected fields: {sorted(unknown)}"
            raise ValueError(msg)
        visited = tuple(dict.fromkeys([*self.visited_agents, agent]))
        return self.model_copy(
            update={"visited_agents": visited, "iteration": self.iteration + 1, **updates}
        )
