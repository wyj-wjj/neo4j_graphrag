"""Deterministic in-memory adapters used by tests and the offline demo."""

from __future__ import annotations

import asyncio
import math
import time
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from graphrag.domain.errors import (
    AuthorizationError,
    ConflictError,
    NotFoundError,
    ValidationError,
)
from graphrag.domain.events import EventEnvelope
from graphrag.domain.ids import new_id
from graphrag.domain.models import (
    AccessPolicy,
    ActionDraft,
    ChunkRecord,
    DocumentRecord,
    DocumentStatus,
    DocumentVersion,
    GraphExtraction,
    IdentityContext,
    IndexStatus,
    IngestionStatus,
    IngestionTask,
    LongTermMemory,
    MemoryCategory,
    MemorySettings,
    MemoryStatus,
    SafeResumeSnapshot,
    SearchCandidate,
    utc_now,
)
from graphrag.domain.state import AgentState
from graphrag.domain.state_migrations import StateMigrationRegistry


class InMemoryKnowledgeRepository:
    def __init__(self) -> None:
        self.documents: dict[str, DocumentRecord] = {}
        self.versions: dict[str, DocumentVersion] = {}
        self.chunks: dict[str, ChunkRecord] = {}
        self.tasks: dict[str, IngestionTask] = {}
        self.policies: dict[str, list[AccessPolicy]] = defaultdict(list)
        self.drafts: dict[str, ActionDraft] = {}
        self.events: dict[str, EventEnvelope] = {}
        self._claimed: set[str] = set()
        self._lock = asyncio.Lock()

    async def create_document(
        self,
        document: DocumentRecord,
        version: DocumentVersion,
        task: IngestionTask,
        event: EventEnvelope,
    ) -> None:
        async with self._lock:
            for existing in self.versions.values():
                if (
                    existing.tenant_id == version.tenant_id
                    and existing.content_hash == version.content_hash
                ):
                    raise ConflictError("同一租户中已存在相同内容的文档版本")
            self.documents[document.document_id] = document
            self.versions[version.version_id] = version
            self.tasks[task.task_id] = task
            self.events[event.event_id] = event

    async def create_version(
        self, version: DocumentVersion, task: IngestionTask, event: EventEnvelope
    ) -> None:
        async with self._lock:
            document = self.documents.get(version.document_id)
            if document is None or document.tenant_id != version.tenant_id:
                raise NotFoundError("文档不存在")
            if any(
                item.tenant_id == version.tenant_id and item.content_hash == version.content_hash
                for item in self.versions.values()
            ):
                raise ConflictError("同一租户中已存在相同内容的文档版本")
            self.versions[version.version_id] = version
            self.tasks[task.task_id] = task
            self.events[event.event_id] = event

    async def list_documents(
        self, tenant_id: str, *, offset: int, limit: int
    ) -> list[DocumentRecord]:
        rows = sorted(
            (item for item in self.documents.values() if item.tenant_id == tenant_id),
            key=lambda item: (item.created_at, item.document_id),
            reverse=True,
        )
        return rows[offset : offset + limit]

    async def list_versions(self, tenant_id: str, document_id: str) -> list[DocumentVersion]:
        return sorted(
            [
                item
                for item in self.versions.values()
                if item.tenant_id == tenant_id and item.document_id == document_id
            ],
            key=lambda item: item.version,
        )

    async def activate_version(self, tenant_id: str, document_id: str, version_id: str) -> None:
        now = utc_now()
        async with self._lock:
            for key, version in tuple(self.versions.items()):
                if (
                    version.tenant_id == tenant_id
                    and version.document_id == document_id
                    and version.version_id != version_id
                    and version.valid_until is None
                ):
                    self.versions[key] = version.model_copy(update={"valid_until": now})
            for key, chunk in tuple(self.chunks.items()):
                if (
                    chunk.tenant_id == tenant_id
                    and chunk.document_id == document_id
                    and chunk.version_id != version_id
                ):
                    self.chunks[key] = chunk.model_copy(
                        update={"status": DocumentStatus.INACTIVE, "valid_until": now}
                    )

    async def inactivate_document(
        self, tenant_id: str, document_id: str, event: EventEnvelope
    ) -> None:
        now = utc_now()
        async with self._lock:
            document = self.documents.get(document_id)
            if document is None or document.tenant_id != tenant_id:
                raise NotFoundError("文档不存在")
            self.documents[document_id] = document.model_copy(
                update={"status": DocumentStatus.INACTIVE, "updated_at": now}
            )
            for key, chunk in tuple(self.chunks.items()):
                if chunk.tenant_id == tenant_id and chunk.document_id == document_id:
                    self.chunks[key] = chunk.model_copy(
                        update={"status": DocumentStatus.INACTIVE, "valid_until": now}
                    )
            self.events[event.event_id] = event

    async def save_chunks(self, chunks: Sequence[ChunkRecord], event: EventEnvelope) -> None:
        async with self._lock:
            snapshot = dict(self.chunks)
            try:
                for chunk in chunks:
                    if chunk.chunk_id in self.chunks and self.chunks[chunk.chunk_id] != chunk:
                        raise ConflictError(f"Chunk ID 冲突：{chunk.chunk_id}")
                    self.chunks[chunk.chunk_id] = chunk
                    if not self.policies[chunk.chunk_id]:
                        self.policies[chunk.chunk_id].append(
                            AccessPolicy(
                                chunk_id=chunk.chunk_id,
                                tenant_id=chunk.tenant_id,
                                allowed_roles=frozenset({"user", "admin"}),
                            )
                        )
                self.events[event.event_id] = event
            except BaseException:
                self.chunks = snapshot
                raise

    async def update_task(self, task: IngestionTask) -> None:
        async with self._lock:
            self.tasks[task.task_id] = task.model_copy(update={"updated_at": utc_now()})
            if task.status in {
                IngestionStatus.COMPLETED,
                IngestionStatus.PARTIAL_FAILED,
                IngestionStatus.FAILED,
            }:
                self._claimed.discard(task.task_id)

    async def get_task(self, tenant_id: str, task_id: str) -> IngestionTask | None:
        task = self.tasks.get(task_id)
        return task if task is not None and task.tenant_id == tenant_id else None

    async def retry_task(self, tenant_id: str, task_id: str) -> IngestionTask:
        async with self._lock:
            task = self.tasks.get(task_id)
            if task is None or task.tenant_id != tenant_id:
                raise NotFoundError("入库任务不存在")
            if task.status not in {IngestionStatus.FAILED, IngestionStatus.PARTIAL_FAILED}:
                raise ConflictError("只有失败或部分失败的入库任务可以重试")
            retried = task.model_copy(
                update={
                    "status": IngestionStatus.RECEIVED,
                    "error_code": None,
                    "error_message": None,
                    "updated_at": utc_now(),
                }
            )
            self.tasks[task_id] = retried
            return retried

    async def get_version(self, tenant_id: str, version_id: str) -> DocumentVersion | None:
        version = self.versions.get(version_id)
        return version if version is not None and version.tenant_id == tenant_id else None

    async def get_document(self, tenant_id: str, document_id: str) -> DocumentRecord | None:
        document = self.documents.get(document_id)
        return document if document is not None and document.tenant_id == tenant_id else None

    async def claim_pending_tasks(self, *, limit: int) -> list[IngestionTask]:
        async with self._lock:
            claimed: list[IngestionTask] = []
            for task in self.tasks.values():
                if len(claimed) >= limit:
                    break
                if task.status != IngestionStatus.RECEIVED or task.task_id in self._claimed:
                    continue
                self._claimed.add(task.task_id)
                claimed_task = task.model_copy(
                    update={
                        "status": IngestionStatus.PARSING,
                        "attempt": task.attempt + 1,
                        "updated_at": utc_now(),
                    }
                )
                self.tasks[task.task_id] = claimed_task
                claimed.append(claimed_task)
            return claimed

    async def list_chunks_for_version(self, tenant_id: str, version_id: str) -> list[ChunkRecord]:
        return sorted(
            [
                chunk
                for chunk in self.chunks.values()
                if chunk.tenant_id == tenant_id and chunk.version_id == version_id
            ],
            key=lambda item: item.ordinal,
        )

    async def authorize_chunks(
        self, identity: IdentityContext, chunk_ids: Sequence[str]
    ) -> list[ChunkRecord]:
        now = utc_now()
        result: list[ChunkRecord] = []
        seen: set[str] = set()
        for chunk_id in chunk_ids:
            if chunk_id in seen:
                continue
            seen.add(chunk_id)
            chunk = self.chunks.get(chunk_id)
            if (
                chunk is None
                or chunk.tenant_id != identity.tenant_id
                or chunk.status != DocumentStatus.ACTIVE
                or chunk.valid_from > now
                or (chunk.valid_until is not None and chunk.valid_until <= now)
            ):
                continue
            policies = self.policies.get(chunk_id, [])
            denied = any(
                policy.deny and policy.applies_to(identity, now=now) for policy in policies
            )
            if (
                not denied
                and policies
                and any(policy.allows(identity, now=now) for policy in policies)
            ):
                result.append(chunk)
        return result

    async def keyword_search(
        self, identity: IdentityContext, query: str, *, top_k: int
    ) -> list[SearchCandidate]:
        tokens = {token.lower() for token in query.split() if token}
        candidates: list[tuple[str, float]] = []
        for chunk in self.chunks.values():
            if chunk.tenant_id != identity.tenant_id:
                continue
            content = chunk.content.lower()
            hits = sum(token in content for token in tokens)
            if hits:
                candidates.append((chunk.chunk_id, hits / max(len(tokens), 1)))
        candidates.sort(key=lambda item: (-item[1], item[0]))
        authorized = await self.authorize_chunks(identity, [item[0] for item in candidates])
        allowed = {item.chunk_id for item in authorized}
        return [
            SearchCandidate(chunk_id=chunk_id, score=score, source="keyword", rank=rank)
            for rank, (chunk_id, score) in enumerate(
                (item for item in candidates if item[0] in allowed), start=1
            )
        ][:top_k]

    async def set_index_status(
        self,
        tenant_id: str,
        chunk_ids: Sequence[str],
        *,
        index: str,
        succeeded: bool,
        error: str | None = None,
    ) -> None:
        if index not in {"vector", "graph"}:
            raise ValueError("index must be vector or graph")
        status = IndexStatus.SUCCEEDED if succeeded else IndexStatus.FAILED
        async with self._lock:
            for chunk_id in chunk_ids:
                chunk = self.chunks.get(chunk_id)
                if chunk is None or chunk.tenant_id != tenant_id:
                    continue
                self.chunks[chunk_id] = chunk.model_copy(
                    update={f"{index}_status": status, f"{index}_error": error}
                )

    async def save_draft(self, draft: ActionDraft) -> ActionDraft:
        async with self._lock:
            for existing in self.drafts.values():
                if (
                    existing.tenant_id == draft.tenant_id
                    and existing.idempotency_key == draft.idempotency_key
                ):
                    return existing
            self.drafts[draft.draft_id] = draft
            return draft

    async def get_draft(self, tenant_id: str, draft_id: str) -> ActionDraft | None:
        draft = self.drafts.get(draft_id)
        return draft if draft is not None and draft.tenant_id == tenant_id else None

    def add_policy(self, policy: AccessPolicy) -> None:
        self.policies[policy.chunk_id].append(policy)


@dataclass(slots=True)
class _VectorEntry:
    tenant_id: str
    document_id: str
    model: str
    dimension: int
    version: str
    vector: tuple[float, ...]


class InMemoryVectorStore:
    def __init__(self, *, dimension: int, version: str) -> None:
        self.dimension = dimension
        self.version = version
        self.entries: dict[str, _VectorEntry] = {}

    async def ensure_schema(self) -> None:
        if self.dimension <= 0:
            raise ValueError("vector dimension must be positive")

    async def upsert(
        self, chunks: Sequence[ChunkRecord], vectors: Sequence[Sequence[float]]
    ) -> None:
        if len(chunks) != len(vectors):
            raise ValueError("chunks and vectors must have equal length")
        staged: dict[str, _VectorEntry] = {}
        for chunk, vector in zip(chunks, vectors, strict=True):
            if len(vector) != self.dimension:
                raise ValueError(f"expected vector dimension {self.dimension}, got {len(vector)}")
            if (
                chunk.embedding_dimension != self.dimension
                or chunk.embedding_version != self.version
            ):
                raise ValueError("embedding model metadata is incompatible with collection")
            staged[chunk.chunk_id] = _VectorEntry(
                chunk.tenant_id,
                chunk.document_id,
                chunk.embedding_model,
                self.dimension,
                self.version,
                tuple(float(item) for item in vector),
            )
        self.entries.update(staged)

    async def search(
        self, tenant_id: str, vector: Sequence[float], *, top_k: int
    ) -> list[SearchCandidate]:
        if len(vector) != self.dimension:
            raise ValueError("query vector dimension mismatch")
        query_norm = math.sqrt(sum(value * value for value in vector)) or 1.0
        scored: list[tuple[str, float]] = []
        for chunk_id, entry in self.entries.items():
            if entry.tenant_id != tenant_id:
                continue
            entry_norm = math.sqrt(sum(value * value for value in entry.vector)) or 1.0
            score = sum(a * b for a, b in zip(vector, entry.vector, strict=True))
            scored.append((chunk_id, score / (query_norm * entry_norm)))
        scored.sort(key=lambda item: (-item[1], item[0]))
        return [
            SearchCandidate(chunk_id=chunk_id, score=score, source="dense", rank=rank)
            for rank, (chunk_id, score) in enumerate(scored[:top_k], start=1)
        ]

    async def delete(self, tenant_id: str, chunk_ids: Sequence[str]) -> None:
        for chunk_id in chunk_ids:
            entry = self.entries.get(chunk_id)
            if entry is not None and entry.tenant_id == tenant_id:
                self.entries.pop(chunk_id, None)

    async def list_ids(self, tenant_id: str) -> set[str]:
        return {key for key, value in self.entries.items() if value.tenant_id == tenant_id}


class InMemoryGraphStore:
    def __init__(self) -> None:
        self.chunk_tenant: dict[str, str] = {}
        self.chunk_version: dict[str, str] = {}
        self.entity_chunks: dict[tuple[str, str], set[str]] = defaultdict(set)
        self.relations: dict[tuple[str, str], set[str]] = defaultdict(set)

    async def ensure_schema(self) -> None:
        return None

    async def upsert(
        self, chunks: Sequence[ChunkRecord], extractions: Sequence[GraphExtraction]
    ) -> None:
        if len(chunks) != len(extractions):
            raise ValueError("chunks and extractions must have equal length")
        for chunk, extraction in zip(chunks, extractions, strict=True):
            self.chunk_tenant[chunk.chunk_id] = chunk.tenant_id
            self.chunk_version[chunk.chunk_id] = chunk.version_id
            id_to_name = {
                entity.entity_id: entity.normalized_name for entity in extraction.entities
            }
            for entity in extraction.entities:
                self.entity_chunks[(chunk.tenant_id, entity.normalized_name)].add(chunk.chunk_id)
                for alias in entity.aliases:
                    self.entity_chunks[(chunk.tenant_id, alias.casefold())].add(chunk.chunk_id)
            for relation in extraction.relations:
                source = id_to_name.get(relation.source_entity_id)
                target = id_to_name.get(relation.target_entity_id)
                if source is not None and target is not None:
                    self.relations[(chunk.tenant_id, source)].add(target)
                    self.relations[(chunk.tenant_id, target)].add(source)

    async def search(
        self, tenant_id: str, entities: Sequence[str], *, max_hops: int, top_k: int
    ) -> list[SearchCandidate]:
        if not 1 <= max_hops <= 2:
            raise ValueError("max_hops must be one or two")
        found: dict[str, tuple[int, tuple[str, ...]]] = {}
        for raw in entities:
            start = raw.casefold().strip()
            frontier: list[tuple[str, int, tuple[str, ...]]] = [(start, 0, (start,))]
            visited = {start}
            while frontier:
                name, hops, path = frontier.pop(0)
                for chunk_id in self.entity_chunks.get((tenant_id, name), set()):
                    previous = found.get(chunk_id)
                    if previous is None or hops < previous[0]:
                        found[chunk_id] = (hops, path)
                if hops >= max_hops:
                    continue
                for related in sorted(self.relations.get((tenant_id, name), set())):
                    if related not in visited:
                        visited.add(related)
                        frontier.append((related, hops + 1, (*path, related)))
        ordered = sorted(found.items(), key=lambda item: (item[1][0], item[0]))[:top_k]
        return [
            SearchCandidate(
                chunk_id=chunk_id,
                score=1.0 / (hops + 1),
                source="graph",
                rank=rank,
                path=path,
            )
            for rank, (chunk_id, (hops, path)) in enumerate(ordered, start=1)
        ]

    async def delete_document_version(self, tenant_id: str, version_id: str) -> None:
        remove = {
            chunk_id
            for chunk_id, stored_version in self.chunk_version.items()
            if stored_version == version_id and self.chunk_tenant.get(chunk_id) == tenant_id
        }
        for chunk_id in remove:
            self.chunk_tenant.pop(chunk_id, None)
            self.chunk_version.pop(chunk_id, None)
        for chunks in self.entity_chunks.values():
            chunks.difference_update(remove)

    async def list_chunk_ids(self, tenant_id: str) -> set[str]:
        return {key for key, value in self.chunk_tenant.items() if value == tenant_id}


class InMemoryCheckpointStore:
    def __init__(self) -> None:
        self.states: dict[tuple[str, str], tuple[float, str]] = {}

    async def put(self, state: AgentState, *, ttl_seconds: int) -> None:
        self.states[(state.tenant_id, state.session_id)] = (
            time.monotonic() + ttl_seconds,
            state.model_dump_json(),
        )

    async def get(self, tenant_id: str, session_id: str) -> AgentState | None:
        item = self.states.get((tenant_id, session_id))
        if item is None:
            return None
        expires, raw = item
        if expires <= time.monotonic():
            self.states.pop((tenant_id, session_id), None)
            return None
        state = StateMigrationRegistry().load_json(raw)
        if state.tenant_id != tenant_id or state.session_id != session_id:
            raise ValidationError("Checkpoint 身份边界不匹配")
        return state

    async def delete(self, tenant_id: str, session_id: str) -> None:
        self.states.pop((tenant_id, session_id), None)


class InMemorySafeResumeStore:
    def __init__(self) -> None:
        self.snapshots: dict[str, SafeResumeSnapshot] = {}
        self.by_draft: dict[tuple[str, str], str] = {}
        self._lock = asyncio.Lock()

    async def save(self, snapshot: SafeResumeSnapshot) -> SafeResumeSnapshot:
        async with self._lock:
            key = (snapshot.tenant_id, snapshot.draft_id)
            existing_id = self.by_draft.get(key)
            if existing_id is not None:
                existing = self.snapshots[existing_id]
                if existing.run_id != snapshot.run_id:
                    raise ConflictError("审批草单已关联到不同运行")
                return existing
            self.snapshots[snapshot.snapshot_id] = snapshot
            self.by_draft[key] = snapshot.snapshot_id
            return snapshot

    async def get_by_draft(self, tenant_id: str, draft_id: str) -> SafeResumeSnapshot | None:
        snapshot_id = self.by_draft.get((tenant_id, draft_id))
        if snapshot_id is None:
            return None
        snapshot = self.snapshots[snapshot_id]
        if snapshot.status == "pending" and snapshot.expires_at <= utc_now():
            snapshot = snapshot.model_copy(
                update={"status": "expired", "next_action": "none", "updated_at": utc_now()}
            )
            self.snapshots[snapshot_id] = snapshot
        return snapshot

    async def mark_resumed(
        self,
        tenant_id: str,
        snapshot_id: str,
        *,
        decision: str,
        result_hash: str,
        final_answer: str,
        recovery_source: str,
    ) -> SafeResumeSnapshot:
        async with self._lock:
            snapshot = self.snapshots.get(snapshot_id)
            if snapshot is None or snapshot.tenant_id != tenant_id:
                raise NotFoundError("安全恢复快照不存在")
            if snapshot.status == "resumed":
                if snapshot.decision != decision:
                    raise ConflictError("审批草单已由不同决定恢复")
                return snapshot
            if snapshot.status != "pending" or snapshot.expires_at <= utc_now():
                raise ConflictError("安全恢复快照不可恢复")
            updated = SafeResumeSnapshot.model_validate(
                {
                    **snapshot.model_dump(mode="python"),
                    "status": "resumed",
                    "decision": decision,
                    "next_action": "none",
                    "resume_result_hash": result_hash,
                    "final_answer": final_answer,
                    "recovery_source": recovery_source,
                    "updated_at": utc_now(),
                }
            )
            self.snapshots[snapshot_id] = updated
            return updated


class InMemoryLongTermMemoryStore:
    def __init__(self) -> None:
        self.memories: dict[str, LongTermMemory] = {}
        self.user_settings: dict[tuple[str, str], MemorySettings] = {}
        self._lock = asyncio.Lock()

    async def settings(self, identity: IdentityContext) -> MemorySettings:
        key = (identity.tenant_id, identity.user_id)
        return self.user_settings.get(
            key,
            MemorySettings(tenant_id=identity.tenant_id, user_id=identity.user_id),
        )

    async def set_enabled(self, identity: IdentityContext, *, enabled: bool) -> MemorySettings:
        updated = MemorySettings(
            tenant_id=identity.tenant_id,
            user_id=identity.user_id,
            enabled=enabled,
            auto_write_enabled=False,
        )
        async with self._lock:
            self.user_settings[(identity.tenant_id, identity.user_id)] = updated
        return updated

    async def create_confirmed(
        self,
        identity: IdentityContext,
        *,
        category: MemoryCategory,
        key: str,
        value: str,
        source_turn_id: str,
        expires_at: datetime | None,
    ) -> LongTermMemory:
        if expires_at is not None and expires_at <= utc_now():
            raise ValidationError("长期记忆过期时间必须在未来")
        async with self._lock:
            if any(
                item.tenant_id == identity.tenant_id
                and item.user_id == identity.user_id
                and item.key == key
                and item.status is MemoryStatus.ACTIVE
                for item in self.memories.values()
            ):
                raise ConflictError("同名长期记忆已存在，请使用纠正接口")
            memory = LongTermMemory(
                tenant_id=identity.tenant_id,
                user_id=identity.user_id,
                category=category,
                key=key,
                value=value,
                source_turn_id=source_turn_id,
                confirmation_method="explicit_user",
                confirmed_by=identity.user_id,
                expires_at=expires_at,
            )
            self.memories[memory.memory_id] = memory
            return memory

    async def list_active(self, identity: IdentityContext) -> list[LongTermMemory]:
        if not (await self.settings(identity)).enabled:
            return []
        now = utc_now()
        return sorted(
            (
                item
                for item in self.memories.values()
                if item.tenant_id == identity.tenant_id
                and item.user_id == identity.user_id
                and item.status is MemoryStatus.ACTIVE
                and (item.expires_at is None or item.expires_at > now)
            ),
            key=lambda item: (item.key, -item.version),
        )

    async def correct(
        self,
        identity: IdentityContext,
        memory_id: str,
        *,
        value: str,
        source_turn_id: str,
        expires_at: datetime | None,
    ) -> LongTermMemory:
        if expires_at is not None and expires_at <= utc_now():
            raise ValidationError("长期记忆过期时间必须在未来")
        async with self._lock:
            current = self._owned(identity, memory_id)
            if current.status is not MemoryStatus.ACTIVE:
                raise ConflictError("只有有效长期记忆可以纠正")
            now = utc_now()
            self.memories[memory_id] = current.model_copy(
                update={"status": MemoryStatus.CORRECTED, "updated_at": now}
            )
            corrected = current.model_copy(
                update={
                    "memory_id": new_id(),
                    "value": value,
                    "source_turn_id": source_turn_id,
                    "confirmed_by": identity.user_id,
                    "version": current.version + 1,
                    "status": MemoryStatus.ACTIVE,
                    "supersedes_memory_id": current.memory_id,
                    "expires_at": expires_at,
                    "created_at": now,
                    "updated_at": now,
                    "deleted_at": None,
                }
            )
            self.memories[corrected.memory_id] = corrected
            return corrected

    async def delete(self, identity: IdentityContext, memory_id: str) -> None:
        async with self._lock:
            current = self._owned(identity, memory_id)
            now = utc_now()
            for stored_id, stored in tuple(self.memories.items()):
                if (
                    stored.tenant_id == identity.tenant_id
                    and stored.user_id == identity.user_id
                    and stored.key == current.key
                ):
                    self.memories[stored_id] = stored.model_copy(
                        update={
                            "value": "[deleted]",
                            "status": MemoryStatus.DELETED,
                            "updated_at": now,
                            "deleted_at": now,
                        }
                    )

    def _owned(self, identity: IdentityContext, memory_id: str) -> LongTermMemory:
        memory = self.memories.get(memory_id)
        if (
            memory is None
            or memory.tenant_id != identity.tenant_id
            or memory.user_id != identity.user_id
        ):
            raise NotFoundError("长期记忆不存在")
        return memory


class InMemoryEventPublisher:
    def __init__(self) -> None:
        self.events: list[EventEnvelope] = []

    async def publish(self, event: EventEnvelope) -> None:
        if any(existing.event_id == event.event_id for existing in self.events):
            return
        self.events.append(event)


class InMemoryAudit:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, Any]]] = []

    async def record(self, event_type: str, fields: dict[str, Any]) -> None:
        self.records.append((event_type, dict(fields)))


class SimpleAuthorization:
    async def require_roles(self, identity: IdentityContext, *roles: str) -> None:
        if not identity.has_role(*roles):
            raise AuthorizationError()


class InMemoryRateLimiter:
    def __init__(self) -> None:
        self.buckets: dict[str, tuple[float, int]] = {}

    async def allow(self, key: str, *, limit: int, window_seconds: int) -> bool:
        now = time.monotonic()
        expires, count = self.buckets.get(key, (now + window_seconds, 0))
        if expires <= now:
            expires, count = now + window_seconds, 0
        if count >= limit:
            self.buckets[key] = (expires, count)
            return False
        self.buckets[key] = (expires, count + 1)
        return True


class InMemoryLeaseLock:
    def __init__(self) -> None:
        self.locks: dict[str, tuple[str, float]] = {}
        self._guard = asyncio.Lock()

    async def acquire(self, key: str, owner: str, *, lease_seconds: int) -> bool:
        async with self._guard:
            current = self.locks.get(key)
            if current is not None and current[1] > time.monotonic() and current[0] != owner:
                return False
            self.locks[key] = (owner, time.monotonic() + lease_seconds)
            return True

    async def release(self, key: str, owner: str) -> bool:
        async with self._guard:
            current = self.locks.get(key)
            if current is None or current[0] != owner:
                return False
            self.locks.pop(key, None)
            return True
