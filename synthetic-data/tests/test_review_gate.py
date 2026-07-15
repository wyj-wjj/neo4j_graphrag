from __future__ import annotations

from datetime import UTC, datetime

import pytest

from graphrag_data_factory.factory import DatasetBundle
from graphrag_data_factory.models import GoldenCandidateRecord
from graphrag_data_factory.review import (
    CandidateReview,
    candidate_fingerprint,
    promote_candidate,
)


def _review(
    candidate: GoldenCandidateRecord, reviewer: str, decision: str = "approved"
) -> CandidateReview:
    return CandidateReview.model_validate(
        {
            "review_id": f"review-{reviewer}",
            "candidate_id": candidate.candidate_id,
            "candidate_fingerprint": candidate_fingerprint(candidate),
            "reviewer_id": reviewer,
            "decision": decision,
            "findings": [],
            "reviewed_at": "2026-01-02T00:00:00Z",
        }
    )


@pytest.mark.contract
def test_golden_promotion_requires_two_distinct_approvals(ci_bundle: DatasetBundle) -> None:
    candidate = next(
        item
        for item in ci_bundle.records["golden_candidate"]
        if isinstance(item, GoldenCandidateRecord)
    )
    first = _review(candidate, "independent-reviewer-a")
    with pytest.raises(ValueError, match="two distinct"):
        promote_candidate(candidate, (first,), promoted_at=datetime.now(UTC))
    duplicate = first.model_copy(update={"review_id": "review-duplicate"})
    with pytest.raises(ValueError, match="two distinct"):
        promote_candidate(candidate, (first, duplicate), promoted_at=datetime.now(UTC))
    rejected = _review(candidate, "independent-reviewer-b", "rejected")
    with pytest.raises(ValueError, match="rejecting"):
        promote_candidate(candidate, (first, rejected), promoted_at=datetime.now(UTC))

    second = _review(candidate, "independent-reviewer-b")
    promoted = promote_candidate(
        candidate,
        (first, second),
        promoted_at=datetime(2026, 1, 3, tzinfo=UTC),
    )
    assert promoted.quality_tier == "golden"
    assert promoted.production_slo_eligible is False


@pytest.mark.contract
def test_changed_candidate_invalidates_old_reviews(ci_bundle: DatasetBundle) -> None:
    candidate = next(
        item
        for item in ci_bundle.records["golden_candidate"]
        if isinstance(item, GoldenCandidateRecord)
    )
    reviews = (
        _review(candidate, "independent-reviewer-a"),
        _review(candidate, "independent-reviewer-b"),
    )
    changed = candidate.model_copy(update={"expected_behavior": "changed-after-review"})
    with pytest.raises(ValueError, match="current-fingerprint"):
        promote_candidate(changed, reviews, promoted_at=datetime.now(UTC))
