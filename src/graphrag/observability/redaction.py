"""Central redaction used before logs, traces, audits and error responses."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

_PATTERNS = (
    (re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)"), "[PHONE]"),
    (re.compile(r"(?<!\d)\d{17}[\dXx](?!\d)"), "[ID]"),
    (re.compile(r"(?i)bearer\s+[A-Za-z0-9._~+/-]+=*"), "Bearer [TOKEN]"),
    (re.compile(r"(?i)(api[_-]?key|secret|password|token)\s*[:=]\s*[^\s,;]+"), r"\1=[REDACTED]"),
    (re.compile(r"(?i)sk-[A-Za-z0-9_-]{8,}"), "[KEY]"),
    (re.compile(r"(?i)(订单号|order[_ -]?id)\s*[:：]?\s*[A-Za-z0-9_-]{4,}"), r"\1:[ORDER]"),
)


def redact_text(value: str) -> str:
    result = value
    for pattern, replacement in _PATTERNS:
        result = pattern.sub(replacement, result)
    return result


def redact(value: Any) -> Any:
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, Mapping):
        return {str(key): redact(item) for key, item in value.items()}
    if isinstance(value, list | tuple | set | frozenset):
        return [redact(item) for item in value]
    return value
