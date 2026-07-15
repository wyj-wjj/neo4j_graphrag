"""Deterministic phase-1.5 Safety Port implementation."""

from __future__ import annotations

import re

from graphrag.domain.models import (
    AgentIntent,
    AnswerStatus,
    ChatResult,
    SafetyAssessment,
    SourceKind,
    TrustDomain,
)

_INJECTION = re.compile(
    r"忽略.{0,12}(?:系统|指令)|绕过.{0,12}(?:权限|审批)|"
    r"ignore.{0,20}(?:system|instruction)|reveal.{0,20}(?:secret|prompt)",
    re.IGNORECASE,
)
_SECRET = re.compile(
    r"(?:sk-[A-Za-z0-9_-]{8,}|Bearer\s+[A-Za-z0-9._-]{8,}|"
    r"api[_-]?key\s*[=:]\s*\S+|\b\d{17}[0-9Xx]\b)",
    re.IGNORECASE,
)


class RuleBasedSafety:
    policy_version = "safety-policy-v1"

    async def inspect(self, text: str, *, trust_domain: TrustDomain) -> SafetyAssessment:
        flags: list[str] = []
        if _INJECTION.search(text):
            flags.append("prompt_injection")
        if _SECRET.search(text):
            flags.append("possible_secret")
        blocked = trust_domain is TrustDomain.MODEL_OUTPUT and "possible_secret" in flags
        return SafetyAssessment(
            policy_version=self.policy_version,
            trust_domain=trust_domain,
            allowed=not blocked,
            flags=tuple(flags),
            action="block"
            if blocked
            else (
                "treat_as_data"
                if flags or trust_domain != TrustDomain.IMMUTABLE_IDENTITY
                else "allow"
            ),
        )

    async def validate_answer(
        self,
        result: ChatResult,
        *,
        trust_domain: TrustDomain = TrustDomain.MODEL_OUTPUT,
    ) -> SafetyAssessment:
        base = await self.inspect(result.answer, trust_domain=trust_domain)
        flags = list(base.flags)
        citation_ids = [item.citation_id for item in result.citations]
        if len(citation_ids) != len(set(citation_ids)):
            flags.append("duplicate_citation")
        if (
            result.intent is AgentIntent.KB
            and result.status is AnswerStatus.ANSWERED
            and not result.citations
        ):
            flags.append("missing_enterprise_citation")
        if result.source is SourceKind.FAKE and "Fake" not in result.answer:
            flags.append("unmarked_fake_result")
        blocked_flags = {
            "possible_secret",
            "duplicate_citation",
            "missing_enterprise_citation",
            "unmarked_fake_result",
        }
        allowed = not bool(blocked_flags.intersection(flags))
        return SafetyAssessment(
            policy_version=self.policy_version,
            trust_domain=trust_domain,
            allowed=allowed,
            flags=tuple(dict.fromkeys(flags)),
            action="allow" if allowed and not flags else ("treat_as_data" if allowed else "block"),
        )
