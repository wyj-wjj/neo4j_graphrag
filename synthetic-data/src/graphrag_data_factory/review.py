"""Independent-review gate; generated candidates cannot self-promote to Golden."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from graphrag_data_factory.deterministic import canonical_json_bytes, sha256_bytes
from graphrag_data_factory.models import GoldenCandidateRecord


class CandidateReview(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    review_id: str
    candidate_id: str
    candidate_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    reviewer_id: str
    decision: Literal["approved", "rejected"]
    findings: tuple[str, ...]
    reviewed_at: datetime


class GoldenRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    candidate_id: str
    candidate_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    independent_review_ids: tuple[str, str]
    promoted_at: datetime
    source: Literal["fake"] = "fake"
    is_synthetic: Literal[True] = True
    production_slo_eligible: Literal[False] = False
    quality_tier: Literal["golden"] = "golden"


def candidate_fingerprint(candidate: GoldenCandidateRecord) -> str:
    return sha256_bytes(canonical_json_bytes(candidate.model_dump(mode="json")))


def promote_candidate(
    candidate: GoldenCandidateRecord,
    reviews: tuple[CandidateReview, ...],
    *,
    promoted_at: datetime,
) -> GoldenRecord:
    fingerprint = candidate_fingerprint(candidate)
    matching = tuple(
        review
        for review in reviews
        if review.candidate_id == candidate.candidate_id
        and review.candidate_fingerprint == fingerprint
    )
    reviewer_ids = {review.reviewer_id for review in matching}
    if len(matching) < candidate.required_independent_reviewers or len(reviewer_ids) < 2:
        raise ValueError("two distinct current-fingerprint reviews are required")
    if any(review.decision != "approved" for review in matching):
        raise ValueError("candidate has a rejecting review")
    selected = tuple(sorted(review.review_id for review in matching)[:2])
    return GoldenRecord(
        candidate_id=candidate.candidate_id,
        candidate_fingerprint=fingerprint,
        independent_review_ids=(selected[0], selected[1]),
        promoted_at=promoted_at,
    )


__all__ = [
    "CandidateReview",
    "GoldenRecord",
    "candidate_fingerprint",
    "promote_candidate",
]
