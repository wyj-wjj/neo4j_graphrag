"""Concurrent GraphRAG retrieval with mandatory MySQL authorization recheck."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, replace

from graphrag.application.context import ContextAssembler
from graphrag.application.prompts import PromptRegistry
from graphrag.config import Settings
from graphrag.domain.errors import DependencyError
from graphrag.domain.models import (
    AnswerStatus,
    ChatMessage,
    Citation,
    ContextManifest,
    ConversationState,
    Evidence,
    IdentityContext,
    SearchCandidate,
    SessionMessage,
)
from graphrag.domain.ports import (
    ChatModelPort,
    EmbeddingPort,
    EntityExtractorPort,
    GraphStorePort,
    KnowledgeRepositoryPort,
    RerankerPort,
    TracePort,
    VectorStorePort,
)
from graphrag.retrieval.query import rewrite_query
from graphrag.retrieval.rrf import reciprocal_rank_fusion


@dataclass(frozen=True, slots=True)
class RetrievalResult:
    query: str
    evidences: tuple[Evidence, ...]
    citations: tuple[Citation, ...]
    branch_status: dict[str, str]
    context_manifest: ContextManifest | None = None


@dataclass(slots=True)
class GraphRAGPipeline:
    settings: Settings
    repository: KnowledgeRepositoryPort
    vector_store: VectorStorePort
    graph_store: GraphStorePort
    embedding: EmbeddingPort
    extractor: EntityExtractorPort
    reranker: RerankerPort
    chat_model: ChatModelPort
    trace: TracePort
    context_assembler: ContextAssembler
    prompt_registry: PromptRegistry

    async def retrieve(
        self,
        identity: IdentityContext,
        query: str,
        *,
        history: Sequence[ChatMessage] = (),
    ) -> RetrievalResult:
        rewritten, _confidence = rewrite_query(query, history)
        branches = await asyncio.gather(
            self._dense(identity, rewritten),
            self._graph(identity, rewritten),
            self._keyword(identity, rewritten),
            return_exceptions=True,
        )
        names = ("dense", "graph", "keyword")
        rankings: list[list[SearchCandidate]] = []
        status: dict[str, str] = {}
        for name, result in zip(names, branches, strict=True):
            if isinstance(result, BaseException):
                status[name] = f"failed:{type(result).__name__}"
                rankings.append([])
            else:
                status[name] = "succeeded"
                rankings.append(result)
        if not any(rankings):
            if all(value.startswith("failed") for value in status.values()):
                raise DependencyError("retrieval", "所有检索分支均不可用")
            return RetrievalResult(rewritten, (), (), status)

        fused = reciprocal_rank_fusion(rankings, k=self.settings.rag_rrf_k)
        fused_ids = [item[0] for item in fused]
        try:
            authorized = await self.repository.authorize_chunks(identity, fused_ids)
        except Exception as exc:
            raise DependencyError("authorization", "权限复核不可用，已拒绝返回知识") from exc
        if not authorized:
            return RetrievalResult(rewritten, (), (), status)
        fused_score = {item[0]: item[1] for item in fused}
        fused_sources = {item[0]: item[2] for item in fused}
        ordered = sorted(authorized, key=lambda item: (-fused_score[item.chunk_id], item.chunk_id))
        parent_ids = [item.parent_chunk_id for item in ordered if item.parent_chunk_id is not None]
        try:
            authorized_parents = await self.repository.authorize_chunks(identity, parent_ids)
        except Exception as exc:
            raise DependencyError("authorization", "权限复核不可用，已拒绝返回知识") from exc
        parent_by_id = {item.chunk_id: item for item in authorized_parents}

        if self.settings.rerank_enabled:
            try:
                with self.trace.span(
                    "retrieval.rerank",
                    {
                        "tenant_id": identity.tenant_id,
                        "candidate_count": len(ordered),
                        "model": self.settings.rerank_model,
                    },
                ):
                    reranked = await self.reranker.rerank(
                        rewritten,
                        [(item.chunk_id, item.content) for item in ordered],
                        timeout=self.settings.rerank_timeout_seconds,
                    )
                rerank_score = {item.candidate_id: item.score for item in reranked}
                best_score = max(rerank_score.values(), default=0.0)
                relevance_floor = max(
                    self.settings.rag_min_rerank_score,
                    best_score * self.settings.rag_rerank_relative_threshold,
                )
                ordered = [
                    item
                    for item in ordered
                    if rerank_score.get(item.chunk_id, 0.0) >= relevance_floor
                ]
                ordered.sort(
                    key=lambda item: (
                        -rerank_score.get(item.chunk_id, float("-inf")),
                        -fused_score[item.chunk_id],
                        item.chunk_id,
                    )
                )
                status["rerank"] = "succeeded"
            except Exception as exc:
                status["rerank"] = f"fallback:{type(exc).__name__}"
        else:
            status["rerank"] = "disabled"

        evidences: list[Evidence] = []
        citations: list[Citation] = []
        for chunk in ordered[: self.settings.rag_final_top_k]:
            context_chunk = parent_by_id.get(chunk.parent_chunk_id or "", chunk)
            document = await self.repository.get_document(identity.tenant_id, chunk.document_id)
            version = await self.repository.get_version(identity.tenant_id, chunk.version_id)
            if document is None or version is None:
                continue
            evidence = Evidence(
                chunk_id=chunk.chunk_id,
                document_id=chunk.document_id,
                document_title=document.title,
                document_version=version.version,
                updated_at=document.updated_at,
                source_location=chunk.source_location,
                content=context_chunk.content,
                score=fused_score[chunk.chunk_id],
                sources=fused_sources[chunk.chunk_id],
                valid_from=version.valid_from,
                valid_until=version.valid_until,
                conflict_group=f"title:{self._normalize_title(document.title)}",
            )
            citation = Citation(
                citation_id=f"C{len(citations) + 1}",
                chunk_id=chunk.chunk_id,
                document_id=chunk.document_id,
                document_title=document.title,
                document_version=version.version,
                updated_at=document.updated_at,
                source_location=chunk.source_location,
            )
            evidences.append(evidence)
            citations.append(citation)
        return RetrievalResult(rewritten, tuple(evidences), tuple(citations), status)

    async def answer(
        self,
        identity: IdentityContext,
        query: str,
        *,
        history: Sequence[ChatMessage] = (),
        conversation_state: ConversationState | None = None,
        run_id: str = "retrieval-debug",
        session_id: str = "retrieval-debug",
        on_delta: Callable[[str], Awaitable[None]] | None = None,
    ) -> tuple[str, AnswerStatus, RetrievalResult]:
        result = await self.retrieve(identity, query, history=history)
        memory = conversation_state or ConversationState(
            tenant_id=identity.tenant_id,
            session_id=session_id,
        )
        assembled = self.context_assembler.assemble(
            run_id=run_id,
            tenant_id=identity.tenant_id,
            session_id=session_id,
            identity=identity,
            model=self.settings.chat_model,
            system_instructions=self.prompt_registry.get("rag-answer").content,
            current_query=query,
            history=tuple(item for item in history if isinstance(item, SessionMessage)),
            conversation_state=memory,
            evidences=result.evidences,
        )
        selected_ids = {item.chunk_id for item in assembled.selected_evidences}
        citation_by_chunk = {item.chunk_id: item for item in result.citations}
        selected_citations = tuple(
            citation_by_chunk[item.chunk_id].model_copy(update={"citation_id": f"C{index}"})
            for index, item in enumerate(assembled.selected_evidences, start=1)
            if item.chunk_id in citation_by_chunk
        )
        result = replace(
            result,
            evidences=tuple(item for item in result.evidences if item.chunk_id in selected_ids),
            citations=selected_citations,
            context_manifest=assembled.manifest,
        )
        if not result.evidences:
            return "现有已授权知识中没有足够证据回答这个问题。", AnswerStatus.REFUSED, result
        conflicts = self._conflict_groups(result.evidences)
        if conflicts:
            result = replace(
                result,
                branch_status={**result.branch_status, "conflict": ",".join(conflicts)},
            )
            return (
                "已授权知识中存在相互冲突的有效证据，无法安全给出确定结论，请联系知识管理员。",
                AnswerStatus.REFUSED,
                result,
            )
        parts: list[str] = []
        with self.trace.span(
            "model.generate",
            {
                "tenant_id": identity.tenant_id,
                "model": self.settings.chat_model,
                "evidence_count": len(result.evidences),
                "stream": True,
            },
        ):
            async with asyncio.timeout(self.settings.generation_timeout_seconds):
                async for delta in self.chat_model.stream(
                    assembled.messages,
                    model=self.settings.chat_model,
                    timeout=self.settings.generation_timeout_seconds,
                ):
                    if not delta.content:
                        continue
                    parts.append(delta.content)
                    if on_delta is not None:
                        await on_delta(delta.content)
        content = "".join(parts)
        citation_suffix = " ".join(f"[{item.citation_id}]" for item in result.citations)
        separator = " " if content and citation_suffix else ""
        if citation_suffix and on_delta is not None:
            await on_delta(f"{separator}{citation_suffix}")
        return f"{content}{separator}{citation_suffix}", AnswerStatus.ANSWERED, result

    async def _dense(self, identity: IdentityContext, query: str) -> list[SearchCandidate]:
        with self.trace.span("retrieval.dense", {"tenant_id": identity.tenant_id}):
            result = await self.embedding.embed(
                [query], input_type="query", timeout=self.settings.embedding_timeout_seconds
            )
            return await self.vector_store.search(
                identity.tenant_id, result.vectors[0], top_k=self.settings.rag_retrieval_top_k
            )

    async def _graph(self, identity: IdentityContext, query: str) -> list[SearchCandidate]:
        with self.trace.span("retrieval.graph", {"tenant_id": identity.tenant_id}):
            entities = await self.extractor.query_entities(
                query, timeout=self.settings.router_timeout_seconds
            )
            if not entities:
                return []
            return await self.graph_store.search(
                identity.tenant_id,
                entities,
                max_hops=self.settings.rag_graph_max_hops,
                top_k=self.settings.rag_retrieval_top_k,
            )

    async def _keyword(self, identity: IdentityContext, query: str) -> list[SearchCandidate]:
        with self.trace.span("retrieval.keyword", {"tenant_id": identity.tenant_id}):
            return await self.repository.keyword_search(
                identity, query, top_k=self.settings.rag_retrieval_top_k
            )

    @staticmethod
    def _normalize_title(value: str) -> str:
        return re.sub(r"\s+", "", value).casefold()

    @staticmethod
    def _conflict_groups(evidences: Sequence[Evidence]) -> tuple[str, ...]:
        grouped: dict[str, set[str]] = {}
        for evidence in evidences:
            if evidence.conflict_group is not None:
                grouped.setdefault(evidence.conflict_group, set()).add(evidence.content)
        return tuple(sorted(key for key, contents in grouped.items() if len(contents) > 1))
