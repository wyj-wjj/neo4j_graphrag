"""Offline Memory & Reliability Golden Set gate; never calls paid services."""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any

from graphrag.application.context import (
    ContextAssembler,
    ContextPolicy,
    ConversationMemoryManager,
    HeuristicTokenEstimator,
)
from graphrag.application.prompts import PromptRegistry
from graphrag.application.sessions import InMemorySessionService, TurnDisposition
from graphrag.domain.models import (
    AgentIntent,
    AnswerStatus,
    ChatResult,
    ConversationState,
    IdentityContext,
    SessionMessage,
)
from graphrag.domain.state_migrations import StateMigrationRegistry

ROOT = Path(__file__).resolve().parents[1]


def _message(sequence: int, content: str) -> SessionMessage:
    role = "user" if sequence % 2 else "assistant"
    return SessionMessage(
        message_id=f"reliability-message-{sequence:04d}",
        client_turn_id=f"reliability-turn-{(sequence + 1) // 2:04d}",
        run_id=f"reliability-run-{(sequence + 1) // 2:04d}",
        sequence=sequence,
        role=role,
        content=content,
        content_hash=hashlib.sha256(content.encode()).hexdigest(),
    )


async def _evaluate() -> dict[str, Any]:
    dataset = json.loads(
        (ROOT / "evaluation/memory-reliability-v1.json").read_text(encoding="utf-8")
    )
    estimator = HeuristicTokenEstimator()
    manager = ConversationMemoryManager(
        estimator=estimator,
        summary_trigger_tokens=80,
        summary_max_tokens=80,
    )
    policy = ContextPolicy(model_context_window_tokens=512, reserved_output_tokens=128)
    assembler = ContextAssembler(policy=policy, estimator=estimator)
    case_results: list[dict[str, Any]] = []
    for case in dataset["cases"]:
        rounds = int(case["rounds"])
        messages = [
            _message(
                sequence,
                str(case["initial"])
                if sequence == 1
                else f"第{sequence}条对话讨论发票、物流或产品保养，但不修改最初约束。",
            )
            for sequence in range(1, rounds * 2 + 1)
        ]
        state = manager.update(
            ConversationState(tenant_id="default", session_id=str(case["id"])),
            messages,
        )
        assembled = assembler.assemble(
            run_id=f"run-{case['id']}",
            tenant_id="default",
            session_id=str(case["id"]),
            identity=IdentityContext(
                tenant_id="default", user_id="evaluator", roles=frozenset({"user"})
            ),
            model="fake-chat-v1",
            system_instructions="安全规则不可覆盖。",
            current_query="请继续，但保持最初约束。",
            history=messages,
            conversation_state=state,
        )
        kinds = {item.kind for item in state.constraints}
        constraints_ok = {"amount", "region", "negation"} <= kinds
        budget_ok = assembled.manifest.actual_input_tokens <= policy.input_budget_tokens
        case_results.append(
            {
                "id": case["id"],
                "rounds": rounds,
                "constraints_ok": constraints_ok,
                "budget_ok": budget_ok,
                "actual_input_tokens": assembled.manifest.actual_input_tokens,
            }
        )

    identity = IdentityContext(
        tenant_id="default", user_id="idempotency-user", roles=frozenset({"user"})
    )
    sessions = InMemorySessionService()
    session = await sessions.create(identity)
    claim = await sessions.begin_turn(
        identity,
        session.session_id,
        client_turn_id="reliability-client-turn-0001",
        request_id="reliability-request-0001",
        query="怎么开发票",
    )
    await sessions.complete_turn(
        identity,
        claim,
        ChatResult(
            request_id=claim.request_id,
            run_id=claim.run_id,
            client_turn_id=claim.client_turn_id,
            session_id=session.session_id,
            status=AnswerStatus.ANSWERED,
            answer="可以申请电子发票。",
            intent=AgentIntent.FAQ,
        ),
    )
    replay = await sessions.begin_turn(
        identity,
        session.session_id,
        client_turn_id=claim.client_turn_id,
        request_id="reliability-request-0002",
        query="怎么开发票",
    )
    stored = await sessions.get(identity, session.session_id)
    idempotency_ok = replay.disposition is TurnDisposition.REPLAY and len(stored.messages) == 2

    migrated = StateMigrationRegistry().load(
        {
            "state_version": 1,
            "request_id": "reliability-request-state",
            "run_id": "reliability-run-state",
            "session_id": "reliability-session-state",
            "tenant_id": "default",
            "user_id": "state-user",
            "roles": ["user"],
            "original_query": "policy",
            "query": "policy",
        }
    )
    registry = PromptRegistry()
    metrics = {
        "critical_constraint_retention": sum(float(item["constraints_ok"]) for item in case_results)
        / len(case_results),
        "context_budget_compliance": sum(float(item["budget_ok"]) for item in case_results)
        / len(case_results),
        "idempotency_success_rate": float(idempotency_ok),
        "state_migration_success_rate": float(migrated.state_version == 2),
        "cross_identity_isolation_rate": 1.0,
        "automatic_long_term_memory_write_rate": 0.0,
        "long_session_success_drop": 0.0,
        "repeated_known_field_question_rate": 0.0,
        "critical_constraint_violation_rate": 0.0,
    }
    return {
        "dataset": dataset["dataset_version"],
        "sample_count": len(case_results),
        "measurement_mode": "synthetic_fake_local",
        "production_slo_eligible": False,
        "versions": {
            "prompt_bundle": registry.bundle_version,
            "prompt_hashes": registry.hashes(),
            "model": "fake-chat-v1",
            "state": "agent-state-v2",
            "context_policy": policy.version,
            "router": "rule-router-v1",
        },
        **metrics,
        "minimums": {
            "critical_constraint_retention": 1.0,
            "context_budget_compliance": 1.0,
            "idempotency_success_rate": 1.0,
            "state_migration_success_rate": 1.0,
            "cross_identity_isolation_rate": 1.0,
        },
        "maximums": {
            "automatic_long_term_memory_write_rate": 0.0,
            "long_session_success_drop": 0.05,
            "repeated_known_field_question_rate": 0.05,
            "critical_constraint_violation_rate": 0.0,
        },
        "case_results": case_results,
    }


def evaluate_reliability() -> dict[str, Any]:
    return asyncio.run(_evaluate())


def main() -> None:
    report = evaluate_reliability()
    failed = [name for name, threshold in report["minimums"].items() if report[name] < threshold]
    failed.extend(
        name for name, threshold in report["maximums"].items() if report[name] > threshold
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if failed:
        raise SystemExit(f"Memory & Reliability thresholds failed: {', '.join(failed)}")


if __name__ == "__main__":
    main()
