"""Versioned Prompt registry with content-integrity verification."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from graphrag.domain.errors import ConfigurationError, NotFoundError


@dataclass(frozen=True, slots=True)
class PromptDefinition:
    prompt_id: str
    version: str
    content: str
    sha256: str


class PromptRegistry:
    def __init__(self, directory: Path | None = None) -> None:
        self.directory = directory or Path(__file__).resolve().parents[1] / "prompts"
        raw = json.loads((self.directory / "manifest.json").read_text(encoding="utf-8"))
        if not isinstance(raw, dict) or not isinstance(raw.get("prompts"), dict):
            raise ConfigurationError("Prompt manifest 格式无效")
        self.bundle_version = str(raw.get("bundle_version", ""))
        if not self.bundle_version:
            raise ConfigurationError("Prompt bundle_version 缺失")
        self._prompts: dict[str, PromptDefinition] = {}
        for prompt_id, item in raw["prompts"].items():
            if not isinstance(item, dict):
                raise ConfigurationError("Prompt 条目格式无效")
            self._load(str(prompt_id), item)

    def _load(self, prompt_id: str, item: dict[str, Any]) -> None:
        filename = str(item.get("file", ""))
        path = (self.directory / filename).resolve()
        if path.parent != self.directory.resolve():
            raise ConfigurationError("Prompt 文件路径越界")
        content = path.read_text(encoding="utf-8").strip()
        digest = hashlib.sha256(content.encode()).hexdigest()
        expected = str(item.get("sha256", ""))
        if digest != expected:
            raise ConfigurationError(f"Prompt 内容 hash 不匹配：{prompt_id}")
        self._prompts[prompt_id] = PromptDefinition(
            prompt_id=prompt_id,
            version=str(item.get("version", "")),
            content=content,
            sha256=digest,
        )

    def get(self, prompt_id: str) -> PromptDefinition:
        try:
            return self._prompts[prompt_id]
        except KeyError as exc:
            raise NotFoundError("Prompt 不存在") from exc

    def hashes(self) -> dict[str, str]:
        return {key: item.sha256 for key, item in sorted(self._prompts.items())}
