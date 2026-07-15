"""Structure-aware parent/child chunking with stable content hashes."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass

from graphrag.domain.models import ParsedDocument


@dataclass(frozen=True, slots=True)
class ChunkDraft:
    content: str
    ordinal: int
    title_path: tuple[str, ...]
    source_location: str
    parent_content: str | None = None

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(self.content.encode("utf-8")).hexdigest()


class StructureAwareChunker:
    def __init__(self, *, target_chars: int, max_chars: int, overlap_ratio: float) -> None:
        self.target_chars = target_chars
        self.max_chars = max_chars
        self.overlap_chars = int(target_chars * overlap_ratio)

    def split(self, parsed: ParsedDocument) -> list[ChunkDraft]:
        drafts: list[ChunkDraft] = []
        current: list[str] = []
        current_title: tuple[str, ...] = ()
        current_locations: list[str] = []

        def flush() -> None:
            if not current:
                return
            content = "\n".join(current).strip()
            if content:
                drafts.extend(self._split_oversized(content, current_title, current_locations))
            current.clear()
            current_locations.clear()

        for page in parsed.pages:
            text = page.text.strip()
            if not text:
                continue
            if page.block_type == "heading":
                flush()
                current_title = page.title_path or (text,)
                current.append(text)
                current_locations.append(f"page:{page.page_number}")
                continue
            if page.block_type == "table":
                flush()
                drafts.append(
                    ChunkDraft(
                        content=text,
                        ordinal=len(drafts),
                        title_path=page.title_path or current_title,
                        source_location=f"page:{page.page_number}:table",
                    )
                )
                continue
            projected = len("\n".join([*current, text]))
            if current and projected > self.target_chars:
                flush()
            current.append(text)
            current_locations.append(f"page:{page.page_number}")
        flush()
        return [
            ChunkDraft(
                content=item.content,
                ordinal=index,
                title_path=item.title_path,
                source_location=item.source_location,
                parent_content=item.parent_content,
            )
            for index, item in enumerate(drafts)
            if item.content.strip()
        ]

    def _split_oversized(
        self, content: str, title_path: tuple[str, ...], locations: Sequence[str]
    ) -> list[ChunkDraft]:
        if len(content) <= self.max_chars:
            return [
                ChunkDraft(
                    content=content,
                    ordinal=0,
                    title_path=title_path,
                    source_location=",".join(dict.fromkeys(locations)),
                )
            ]
        result: list[ChunkDraft] = []
        start = 0
        step = max(1, self.max_chars - self.overlap_chars)
        while start < len(content):
            end = min(len(content), start + self.max_chars)
            piece = content[start:end].strip()
            if piece:
                result.append(
                    ChunkDraft(
                        content=piece,
                        ordinal=len(result),
                        title_path=title_path,
                        source_location=",".join(dict.fromkeys(locations)),
                        parent_content=content,
                    )
                )
            if end == len(content):
                break
            start += step
        return result
