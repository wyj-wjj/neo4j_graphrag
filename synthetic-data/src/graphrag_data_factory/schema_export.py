"""Generate deterministic JSON Schema snapshots for non-Python consumers."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel

from graphrag_data_factory.models import DatasetManifest, DatasetProfile
from graphrag_data_factory.resources import CleanupPlan, GenerationEstimate
from graphrag_data_factory.review import CandidateReview, GoldenRecord
from graphrag_data_factory.scoring import BehaviorScore, ObservedAgentResult, RankingMetrics
from graphrag_data_factory.simulator import (
    ApprovalCallbackRequest,
    DraftRequest,
    FaultControlRequest,
    RefundDraftRequest,
    RefundQuoteRequest,
)
from graphrag_data_factory.upload_client import UploadResult, UploadTask
from graphrag_data_factory.validator import DatasetValidator


def export_schemas(target: Path) -> tuple[Path, ...]:
    target.mkdir(parents=True, exist_ok=True)
    models: dict[str, type[BaseModel]] = {
        "dataset-profile": DatasetProfile,
        "dataset-manifest": DatasetManifest,
        "business-draft-request": DraftRequest,
        "business-refund-quote-request": RefundQuoteRequest,
        "business-refund-draft-request": RefundDraftRequest,
        "business-approval-callback-request": ApprovalCallbackRequest,
        "business-fault-control-request": FaultControlRequest,
        "knowledge-upload-task": UploadTask,
        "knowledge-upload-result": UploadResult,
        "generation-estimate": GenerationEstimate,
        "cleanup-plan": CleanupPlan,
        "candidate-review": CandidateReview,
        "golden-record": GoldenRecord,
        "observed-agent-result": ObservedAgentResult,
        "behavior-score": BehaviorScore,
        "ranking-metrics": RankingMetrics,
        **{name.replace("_", "-"): model for name, model in DatasetValidator.RECORD_MODELS.items()},
    }
    paths = []
    for name, model in sorted(models.items()):
        path = target / f"{name}.schema.json"
        payload = model.model_json_schema(mode="validation")
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        paths.append(path)
    return tuple(paths)


__all__ = ["export_schemas"]
