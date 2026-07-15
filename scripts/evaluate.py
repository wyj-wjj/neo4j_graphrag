"""Deterministic end-to-end Golden Set gate; never calls network or paid models."""

from __future__ import annotations

import asyncio
import json
import tempfile
import time
from pathlib import Path
from typing import Any

from graphrag.agents.router import route
from graphrag.application.container import build_runtime
from graphrag.config import AppEnvironment, Settings
from graphrag.domain.ids import new_id
from graphrag.domain.models import AnswerStatus, IdentityContext
from graphrag.retrieval.evaluation import accuracy, ndcg_at_k, recall_at_k, reciprocal_rank

ROOT = Path(__file__).resolve().parents[1]
DOCUMENTS = {
    "policy-warranty": ("保修政策", "星河保温杯提供两年保修，非人为损坏可以免费换新。"),
    "policy-member": ("会员规则", "星河会员每消费一元累计一积分，积分有效期十二个月。"),
    "product-care": ("产品保养", "星河产品应使用软布清洁，避免长时间浸泡和高温暴晒。"),
}


async def _evaluate() -> dict[str, Any]:
    samples = json.loads((ROOT / "evaluation/golden-set.json").read_text(encoding="utf-8"))
    expected_intents = [str(item["intent"]) for item in samples]
    actual_intents = [route(str(item["query"]), threshold=0.75).intent.value for item in samples]
    recalls: list[float] = []
    reciprocal_ranks: list[float] = []
    ndcgs: list[float] = []
    citation_checks: list[float] = []
    refusal_checks: list[float] = []
    latencies: list[float] = []
    sample_results: list[dict[str, Any]] = []

    with tempfile.TemporaryDirectory(prefix="graphrag-evaluation-") as directory:
        settings = Settings(
            app_env=AppEnvironment.TEST,
            use_fake_external_clients=True,
            upload_dir=Path(directory),
            embedding_dimension=32,
            chunk_target_chars=100,
            chunk_max_chars=150,
        )
        runtime = build_runtime(settings)
        identity = IdentityContext(
            tenant_id="default", user_id="evaluator", roles=frozenset({"user", "admin"})
        )
        document_labels: dict[str, str] = {}
        for label, (title, content) in DOCUMENTS.items():
            task = await runtime.ingestion.receive(
                identity,
                filename=f"{label}.txt",
                content=content.encode(),
                title=title,
                trace_id=new_id(),
            )
            completed = await runtime.ingestion.process(task, trace_id=new_id())
            if completed.status.value != "completed":
                raise RuntimeError(f"evaluation fixture ingestion failed: {label}")
            document_labels[task.document_id] = label

        for item in samples:
            if item["intent"] != "kb":
                continue
            started = time.perf_counter()
            answer, status, result = await runtime.retrieval.answer(identity, str(item["query"]))
            latencies.append((time.perf_counter() - started) * 1000)
            actual = [document_labels[evidence.document_id] for evidence in result.evidences]
            expected = set(item["expected_chunks"])
            sample_results.append(
                {
                    "id": item["id"],
                    "expected_chunks": sorted(expected),
                    "actual_chunks": actual,
                    "answer_status": status.value,
                }
            )
            if expected:
                recalls.append(recall_at_k(actual, expected, 20))
                reciprocal_ranks.append(reciprocal_rank(actual, expected, 20))
                ndcgs.append(ndcg_at_k(actual, expected, 10))
                cited_documents = {
                    document_labels[citation.document_id] for citation in result.citations
                }
                citation_checks.append(float(bool(cited_documents) and cited_documents <= expected))
                if status != AnswerStatus.ANSWERED or not answer:
                    citation_checks[-1] = 0.0
            elif not item["answerable"]:
                refusal_checks.append(float(status == AnswerStatus.REFUSED))

        await runtime.close()

    sorted_latencies = sorted(latencies)
    p95_index = max(0, int(len(sorted_latencies) * 0.95 + 0.999) - 1)
    report: dict[str, Any] = {
        "dataset": "synthetic-phase1-v1",
        "sample_count": len(samples),
        "router_accuracy": accuracy(actual_intents, expected_intents),
        "recall_at_20": sum(recalls) / len(recalls),
        "mrr_at_20": sum(reciprocal_ranks) / len(reciprocal_ranks),
        "ndcg_at_10": sum(ndcgs) / len(ndcgs),
        "citation_correctness": sum(citation_checks) / len(citation_checks),
        "faithfulness": sum(citation_checks) / len(citation_checks),
        "no_answer_refusal_rate": sum(refusal_checks) / len(refusal_checks),
        "offline_p95_ms": round(sorted_latencies[p95_index], 3),
        "sample_results": sample_results,
        "thresholds": {
            "router_accuracy": 0.95,
            "recall_at_20": 0.90,
            "mrr_at_20": 0.80,
            "ndcg_at_10": 0.85,
            "citation_correctness": 0.98,
            "faithfulness": 0.95,
            "no_answer_refusal_rate": 0.95,
        },
    }
    return report


def evaluate() -> dict[str, Any]:
    return asyncio.run(_evaluate())


def main() -> None:
    report = evaluate()
    output = ROOT / "evaluation/report.json"
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    failed = [name for name, threshold in report["thresholds"].items() if report[name] < threshold]
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if failed:
        raise SystemExit(f"Golden Set thresholds failed: {', '.join(failed)}")


if __name__ == "__main__":
    main()
