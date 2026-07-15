"""Read-only knowledge quality diagnostics; never mutates business content."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass

from graphrag.domain.models import (
    DocumentStatus,
    KnowledgeIssue,
    KnowledgeQualityReport,
    utc_now,
)
from graphrag.domain.ports import KnowledgeRepositoryPort


@dataclass(slots=True)
class KnowledgeQualityService:
    repository: KnowledgeRepositoryPort

    async def report(self, tenant_id: str) -> KnowledgeQualityReport:
        documents = await self.repository.list_documents(tenant_id, offset=0, limit=10_000)
        issues: list[KnowledgeIssue] = []
        chunks_by_hash: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
        active_versions_by_title: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
        now = utc_now()

        for document in documents:
            versions = await self.repository.list_versions(tenant_id, document.document_id)
            for version in versions:
                chunks = await self.repository.list_chunks_for_version(
                    tenant_id, version.version_id
                )
                if version.valid_until is not None and version.valid_until <= now:
                    issues.append(
                        KnowledgeIssue(
                            issue_type="expired",
                            document_ids=(document.document_id,),
                            version_ids=(version.version_id,),
                            reason="文档版本已过有效期",
                        )
                    )
                if document.status is DocumentStatus.ACTIVE and version.valid_until is None:
                    active_versions_by_title[self._title(document.title)].append(
                        (document.document_id, version.version_id, version.content_hash)
                    )
                for chunk in chunks:
                    if chunk.chunk_kind == "child":
                        chunks_by_hash[chunk.content_hash].append(
                            (document.document_id, version.version_id, chunk.chunk_id)
                        )
                        if len(re.sub(r"\s+", "", chunk.content)) < 20:
                            issues.append(
                                KnowledgeIssue(
                                    issue_type="low_quality",
                                    document_ids=(document.document_id,),
                                    version_ids=(version.version_id,),
                                    chunk_ids=(chunk.chunk_id,),
                                    reason="Chunk 有效文本过短",
                                )
                            )

        for rows in chunks_by_hash.values():
            document_ids = tuple(sorted({row[0] for row in rows}))
            if len(document_ids) > 1:
                issues.append(
                    KnowledgeIssue(
                        issue_type="duplicate",
                        document_ids=document_ids,
                        version_ids=tuple(sorted({row[1] for row in rows})),
                        chunk_ids=tuple(sorted({row[2] for row in rows})),
                        reason="多个文档包含相同 Chunk 内容",
                    )
                )
        for rows in active_versions_by_title.values():
            hashes = {row[2] for row in rows}
            if len(rows) > 1 and len(hashes) > 1:
                issues.append(
                    KnowledgeIssue(
                        issue_type="conflict",
                        document_ids=tuple(sorted({row[0] for row in rows})),
                        version_ids=tuple(sorted({row[1] for row in rows})),
                        reason="同名有效文档版本内容不一致，需要人工确认权威版本",
                    )
                )
        return KnowledgeQualityReport(
            tenant_id=tenant_id,
            issues=tuple(sorted(issues, key=lambda item: (item.issue_type, item.issue_id))),
        )

    @staticmethod
    def _title(value: str) -> str:
        return re.sub(r"\s+", "", value).casefold()
