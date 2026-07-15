"""Versioned Pydantic input contracts for Agent tools."""

from __future__ import annotations

from pydantic import Field

from graphrag.domain.models import StrictModel


class QueryToolInputV1(StrictModel):
    query: str = Field(min_length=1, max_length=4000)


class DraftToolInputV1(QueryToolInputV1):
    idempotency_key: str = Field(min_length=8, max_length=200)
