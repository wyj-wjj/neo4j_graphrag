"""Deterministic input normalization and conservative conversational rewrite."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Sequence

from graphrag.domain.errors import ValidationError
from graphrag.domain.models import ChatMessage

_CONTROL_PATTERN = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_ONLY_PUNCTUATION = re.compile(r"^[\W_]+$", re.UNICODE)
_REFERENTIAL = re.compile(
    r"^(这个|那个|它|这项|那项|怎么办|怎么弄|why|how about it)", re.IGNORECASE
)


def normalize_query(query: str, *, max_chars: int = 4000) -> str:
    value = unicodedata.normalize("NFKC", query).strip()
    value = re.sub(r"\s+", " ", value)
    if not value:
        raise ValidationError("问题不能为空")
    if len(value) > max_chars:
        raise ValidationError("问题超过长度限制")
    if _CONTROL_PATTERN.search(value):
        raise ValidationError("问题包含非法控制字符")
    if _ONLY_PUNCTUATION.fullmatch(value):
        raise ValidationError("问题必须包含有效文字")
    return value


def rewrite_query(query: str, history: Sequence[ChatMessage]) -> tuple[str, float]:
    normalized = normalize_query(query)
    if not history or not _REFERENTIAL.search(normalized):
        return normalized, 1.0
    previous = next(
        (message.content for message in reversed(history) if message.role == "user"), None
    )
    if previous is None:
        return normalized, 0.6
    return f"关于“{previous[:300]}”，用户继续问：{normalized}", 0.8
