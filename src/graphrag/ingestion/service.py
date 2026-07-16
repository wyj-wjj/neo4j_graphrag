"""Persistent task ingestion coordinator with independent derived-index compensation."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass

from graphrag.config import Settings
from graphrag.domain.errors import NotFoundError
from graphrag.domain.events import EventEnvelope
from graphrag.domain.ids import new_id
from graphrag.domain.models import (
    ChunkRecord,
    DocumentRecord,
    DocumentVersion,
    IdentityContext,
    IndexStatus,
    IngestionStatus,
    IngestionTask,
)
from graphrag.domain.ports import (
    EmbeddingPort,
    EntityExtractorPort,
    EventPublisherPort,
    GraphStorePort,
    KnowledgeRepositoryPort,
    ObjectStorePort,
    VectorStorePort,
)
from graphrag.ingestion.chunker import StructureAwareChunker
from graphrag.ingestion.parsers import ParserRegistry
from graphrag.ingestion.validation import UploadValidator


@dataclass(slots=True)
class IngestionCoordinator:
    settings: Settings
    repository: KnowledgeRepositoryPort
    object_store: ObjectStorePort
    vector_store: VectorStorePort
    graph_store: GraphStorePort
    embedding: EmbeddingPort
    extractor: EntityExtractorPort
    publisher: EventPublisherPort
    validator: UploadValidator
    parsers: ParserRegistry
    chunker: StructureAwareChunker

    async def receive(
        self,
        identity: IdentityContext,
        *,
        filename: str,
        content: bytes,
        title: str,
        trace_id: str,
    ) -> IngestionTask:
        upload = self.validator.validate(filename, content)
        document = DocumentRecord(tenant_id=identity.tenant_id, title=title)
        object_key, digest = await self.object_store.save(
            identity.tenant_id, document.document_id, upload.filename, upload.content
        )
        version = DocumentVersion(
            document_id=document.document_id,
            tenant_id=identity.tenant_id,
            version=1,
            object_key=object_key,
            content_hash=digest,
            mime_type=upload.mime_type,
            size_bytes=len(upload.content),
        )
        task = IngestionTask(
            tenant_id=identity.tenant_id,
            document_id=document.document_id,
            version_id=version.version_id,
        )
        event = EventEnvelope(
            event_type="document.received",
            aggregate_version=version.version,
            tenant_id=identity.tenant_id,
            aggregate_id=document.document_id,
            trace_id=trace_id,
            payload_summary={"version_id": version.version_id, "mime_type": upload.mime_type},
        )
        try:
            await self.repository.create_document(document, version, task, event)
        except BaseException:
            await self.object_store.delete(identity.tenant_id, object_key)
            raise
        await self.publisher.publish(event)
        return task

    async def receive_version(
        self,
        identity: IdentityContext,
        *,
        document_id: str,
        filename: str,
        content: bytes,
        trace_id: str,
    ) -> IngestionTask:
        upload = self.validator.validate(filename, content)
        document = await self.repository.get_document(identity.tenant_id, document_id)
        if document is None:
            raise NotFoundError("文档不存在")
        versions = await self.repository.list_versions(identity.tenant_id, document_id)
        object_key, digest = await self.object_store.save(
            identity.tenant_id, document_id, upload.filename, upload.content
        )
        version = DocumentVersion(
            document_id=document_id,
            tenant_id=identity.tenant_id,
            version=max((item.version for item in versions), default=0) + 1,
            object_key=object_key,
            content_hash=digest,
            mime_type=upload.mime_type,
            size_bytes=len(upload.content),
        )
        task = IngestionTask(
            tenant_id=identity.tenant_id,
            document_id=document_id,
            version_id=version.version_id,
        )
        event = EventEnvelope(
            event_type="document.version.received",
            aggregate_version=version.version,
            tenant_id=identity.tenant_id,
            aggregate_id=document_id,
            trace_id=trace_id,
            payload_summary={"version_id": version.version_id, "version": version.version},
        )
        try:
            await self.repository.create_version(version, task, event)
        except BaseException:
            await self.object_store.delete(identity.tenant_id, object_key)
            raise
        await self.publisher.publish(event)
        return task

    async def process(self, task: IngestionTask, *, trace_id: str) -> IngestionTask:
        if task.version_id is None:
            return await self._fail(task, "missing_version", "入库任务缺少文档版本")
        version = await self.repository.get_version(task.tenant_id, task.version_id)
        if version is None:
            return await self._fail(task, "version_not_found", "文档版本不存在")
        document = await self.repository.get_document(task.tenant_id, task.document_id)
        if document is None:
            return await self._fail(task, "document_not_found", "文档不存在")
        existing_chunks = await self.repository.list_chunks_for_version(
            task.tenant_id, version.version_id
        )
        try:
            if existing_chunks:
                return await self._resume_indexes(task, existing_chunks, trace_id=trace_id)
            content = await self.object_store.read(task.tenant_id, version.object_key)
            if hashlib.sha256(content).hexdigest() != version.content_hash:
                raise ValueError("object hash mismatch")
            filename = f"source{self._extension_for_mime(version.mime_type)}"
            parsed = await self.parsers.parse(filename, content)
            parsed = await self.parsers.apply_ocr(parsed, content)
            drafts = self.chunker.split(parsed)
            if not drafts:
                raise ValueError("parser produced no chunks")
            chunks: list[ChunkRecord] = []
            parents: dict[str, ChunkRecord] = {}
            for draft in drafts:
                parent_id: str | None = None
                if draft.parent_content is not None:
                    parent_hash = hashlib.sha256(draft.parent_content.encode()).hexdigest()
                    parent = parents.get(parent_hash)
                    if parent is None:
                        parent = ChunkRecord(
                            tenant_id=task.tenant_id,
                            document_id=task.document_id,
                            version_id=version.version_id,
                            ordinal=len(chunks),
                            content=draft.parent_content,
                            content_hash=parent_hash,
                            title_path=draft.title_path,
                            source_location=draft.source_location,
                            chunk_kind="parent",
                            embedding_model=self.settings.embedding_model,
                            embedding_dimension=self.settings.embedding_dimension,
                            embedding_version=self.settings.embedding_version,
                            vector_status=IndexStatus.NOT_APPLICABLE,
                            graph_status=IndexStatus.NOT_APPLICABLE,
                        )
                        parents[parent_hash] = parent
                        chunks.append(parent)
                    parent_id = parent.chunk_id
                chunks.append(
                    ChunkRecord(
                        tenant_id=task.tenant_id,
                        document_id=task.document_id,
                        version_id=version.version_id,
                        parent_chunk_id=parent_id,
                        ordinal=len(chunks),
                        content=draft.content,
                        content_hash=draft.content_hash,
                        title_path=draft.title_path,
                        source_location=draft.source_location,
                        embedding_model=self.settings.embedding_model,
                        embedding_dimension=self.settings.embedding_dimension,
                        embedding_version=self.settings.embedding_version,
                    )
                )
            event = EventEnvelope(
                event_type="document.chunked",
                aggregate_version=version.version,
                tenant_id=task.tenant_id,
                aggregate_id=task.document_id,
                trace_id=trace_id,
                payload_summary={
                    "version_id": version.version_id,
                    "version": version.version,
                    "chunk_count": len(chunks),
                },
            )
            await self.repository.save_chunks(chunks, event)
            await self.publisher.publish(event)
        except Exception as exc:
            return await self._fail(task, "parse_failed", str(exc))

        indexable = [item for item in chunks if item.chunk_kind == "child"]
        vector_ok, graph_ok = await asyncio.gather(
            self._index_vectors(indexable),
            self._index_graph(indexable),
            return_exceptions=False,
        )
        if vector_ok and graph_ok:
            final = task.model_copy(update={"status": IngestionStatus.COMPLETED})
        elif vector_ok or graph_ok:
            final = task.model_copy(
                update={
                    "status": IngestionStatus.PARTIAL_FAILED,
                    "error_code": "derived_index_partial_failure",
                    "error_message": "一个派生索引写入失败，可安全重试",
                }
            )
        else:
            final = task.model_copy(
                update={
                    "status": IngestionStatus.FAILED,
                    "error_code": "derived_indexes_failed",
                    "error_message": "向量与图谱索引均失败，可安全重试",
                }
            )
        await self.repository.update_task(final)
        if final.status == IngestionStatus.COMPLETED:
            await self.repository.activate_version(
                task.tenant_id, task.document_id, version.version_id
            )
        await self.publisher.publish(
            EventEnvelope(
                event_type="document.indexed"
                if final.status == IngestionStatus.COMPLETED
                else "document.failed",
                aggregate_version=version.version,
                tenant_id=task.tenant_id,
                aggregate_id=task.document_id,
                trace_id=trace_id,
                payload_summary={
                    "task_id": task.task_id,
                    "version_id": version.version_id,
                    "version": version.version,
                    "status": final.status.value,
                },
            )
        )
        return final

    async def _resume_indexes(
        self, task: IngestionTask, chunks: Sequence[ChunkRecord], *, trace_id: str
    ) -> IngestionTask:
        version = (
            await self.repository.get_version(task.tenant_id, task.version_id)
            if task.version_id is not None
            else None
        )
        aggregate_version = version.version if version is not None else 1
        indexable = [item for item in chunks if item.chunk_kind == "child"]
        vector_needed = any(item.vector_status != IndexStatus.SUCCEEDED for item in indexable)
        graph_needed = any(item.graph_status != IndexStatus.SUCCEEDED for item in indexable)
        vector_ok, graph_ok = await asyncio.gather(
            self._index_vectors(indexable) if vector_needed else asyncio.sleep(0, result=True),
            self._index_graph(indexable) if graph_needed else asyncio.sleep(0, result=True),
        )
        if vector_ok and graph_ok:
            final = task.model_copy(update={"status": IngestionStatus.COMPLETED})
        elif vector_ok or graph_ok:
            final = task.model_copy(
                update={
                    "status": IngestionStatus.PARTIAL_FAILED,
                    "error_code": "derived_index_partial_failure",
                    "error_message": "一个派生索引写入失败，可安全重试",
                }
            )
        else:
            final = task.model_copy(
                update={
                    "status": IngestionStatus.FAILED,
                    "error_code": "derived_indexes_failed",
                    "error_message": "向量与图谱索引均失败，可安全重试",
                }
            )
        await self.repository.update_task(final)
        if final.status == IngestionStatus.COMPLETED and task.version_id is not None:
            await self.repository.activate_version(
                task.tenant_id, task.document_id, task.version_id
            )
        await self.publisher.publish(
            EventEnvelope(
                event_type="document.reindexed",
                aggregate_version=aggregate_version,
                tenant_id=task.tenant_id,
                aggregate_id=task.document_id,
                trace_id=trace_id,
                payload_summary={
                    "task_id": task.task_id,
                    "version_id": task.version_id,
                    "version": aggregate_version,
                    "status": final.status.value,
                },
            )
        )
        return final

    async def _index_vectors(self, chunks: Sequence[ChunkRecord]) -> bool:
        try:
            result = await self.embedding.embed(
                [item.content for item in chunks],
                input_type="document",
                timeout=self.settings.embedding_timeout_seconds,
            )
            if result.dimension != self.settings.embedding_dimension:
                raise ValueError("embedding provider returned incompatible dimension")
            await self.vector_store.upsert(chunks, result.vectors)
            await self.repository.set_index_status(
                chunks[0].tenant_id,
                [item.chunk_id for item in chunks],
                index="vector",
                succeeded=True,
            )
            return True
        except Exception as exc:
            await self.repository.set_index_status(
                chunks[0].tenant_id,
                [item.chunk_id for item in chunks],
                index="vector",
                succeeded=False,
                error=type(exc).__name__,
            )
            return False

    async def _index_graph(self, chunks: Sequence[ChunkRecord]) -> bool:
        try:
            extractions = [
                await self.extractor.extract(
                    item.content, timeout=self.settings.generation_timeout_seconds
                )
                for item in chunks
            ]
            await self.graph_store.upsert(chunks, extractions)
            await self.repository.set_index_status(
                chunks[0].tenant_id,
                [item.chunk_id for item in chunks],
                index="graph",
                succeeded=True,
            )
            return True
        except Exception as exc:
            await self.repository.set_index_status(
                chunks[0].tenant_id,
                [item.chunk_id for item in chunks],
                index="graph",
                succeeded=False,
                error=type(exc).__name__,
            )
            return False

    async def _fail(self, task: IngestionTask, code: str, message: str) -> IngestionTask:
        failed = task.model_copy(
            update={
                "status": IngestionStatus.FAILED,
                "error_code": code,
                "error_message": message[:500],
            }
        )
        await self.repository.update_task(failed)
        return failed

    @staticmethod
    def _extension_for_mime(mime_type: str) -> str:
        mapping = {
            "application/pdf": ".pdf",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
            "text/html": ".html",
            "text/plain": ".txt",
            "text/markdown": ".md",
            "image/png": ".png",
            "image/jpeg": ".jpg",
        }
        return mapping[mime_type]


class IngestionWorker:
    def __init__(self, coordinator: IngestionCoordinator, *, concurrency: int) -> None:
        self.coordinator = coordinator
        self.concurrency = concurrency
        self._stopping = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="ingestion-worker")

    async def stop(self) -> None:
        self._stopping.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def _run(self) -> None:
        semaphore = asyncio.Semaphore(self.concurrency)
        running: set[asyncio.Task[IngestionTask]] = set()
        while not self._stopping.is_set():
            pending = await self.coordinator.repository.claim_pending_tasks(
                limit=max(0, self.concurrency - len(running))
            )
            for item in pending:

                async def process(task: IngestionTask = item) -> IngestionTask:
                    async with semaphore:
                        return await self.coordinator.process(task, trace_id=new_id())

                running.add(asyncio.create_task(process()))
            completed = {task for task in running if task.done()}
            running.difference_update(completed)
            for task in completed:
                task.result()
            if not pending:
                with suppress(TimeoutError):
                    await asyncio.wait_for(self._stopping.wait(), timeout=0.2)
        if running:
            await asyncio.gather(*running)
