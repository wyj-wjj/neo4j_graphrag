"""Crash-safe bounded-batch writers for large synthetic datasets."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from graphrag_data_factory.deterministic import canonical_json_bytes, sha256_file
from graphrag_data_factory.models import FileManifest

CHECKPOINT_FILE_NAME = ".generation-state.json"


class StreamingCheckpointError(ValueError):
    """Raised when a generation checkpoint cannot be resumed safely."""


class ActiveFileCheckpoint(BaseModel):
    """Only bytes covered by this checkpoint are trusted after a restart."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    record_type: str
    relative_path: str
    total_records: int = Field(ge=0)
    committed_records: int = Field(ge=0)
    committed_bytes: int = Field(ge=0)
    prefix_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_progress(self) -> ActiveFileCheckpoint:
        if self.committed_records > self.total_records:
            raise ValueError("committed_records cannot exceed total_records")
        return self


class StreamingCheckpoint(BaseModel):
    """Versioned generation state stored beside an unpublished dataset tree."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    checkpoint_version: Literal["streaming-checkpoint-v1"] = "streaming-checkpoint-v1"
    dataset_id: str
    profile_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    completed_files: dict[str, FileManifest] = Field(default_factory=dict)
    active_file: ActiveFileCheckpoint | None = None


RecordProducer = Callable[[int, int], Iterable[BaseModel | dict[str, Any]]]


class ResumableJsonlWriter:
    """Write one canonical JSONL file in fsynced batches with explicit resume.

    A crash may leave bytes after the last durable checkpoint. Resume verifies the
    committed prefix and truncates only that uncommitted tail before continuing.
    Published files are never inferred from an unchecked partial file.
    """

    def __init__(
        self,
        staging_root: Path,
        *,
        dataset_id: str,
        profile_sha256: str,
        batch_size: int,
        resume: bool = False,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        self.root = staging_root.resolve()
        self.state_path = self.root / CHECKPOINT_FILE_NAME
        self.batch_size = batch_size
        self._initialize(dataset_id, profile_sha256, resume=resume)
        self._recover_completed_rename_if_needed()
        self._verify_active_checkpoint()

    @property
    def checkpoint(self) -> StreamingCheckpoint:
        return self._checkpoint

    def write_records(
        self,
        *,
        record_type: str,
        relative_path: str,
        total_records: int,
        produce: RecordProducer,
        fail_after_committed_batches: int | None = None,
    ) -> FileManifest:
        if total_records < 0:
            raise ValueError("total_records cannot be negative")
        final_path = self._safe_path(relative_path)
        partial_path = final_path.with_name(f"{final_path.name}.part")
        completed = self._checkpoint.completed_files.get(record_type)
        if completed is not None:
            self._verify_completed(completed, relative_path, total_records)
            return completed

        active = self._checkpoint.active_file
        if active is None:
            if final_path.exists() or partial_path.exists():
                raise StreamingCheckpointError(
                    f"untracked output exists for {record_type}; refusing to overwrite"
                )
            final_path.parent.mkdir(parents=True, exist_ok=True)
            active = ActiveFileCheckpoint(
                record_type=record_type,
                relative_path=relative_path,
                total_records=total_records,
                committed_records=0,
                committed_bytes=0,
                prefix_sha256=hashlib.sha256().hexdigest(),
            )
            self._set_checkpoint(active_file=active)
        else:
            if (
                active.record_type != record_type
                or active.relative_path != relative_path
                or active.total_records != total_records
            ):
                raise StreamingCheckpointError(
                    "checkpoint active file does not match the requested record stream"
                )

        digest = self._prepare_partial(partial_path, active)
        committed = active.committed_records
        batches_committed = 0
        with partial_path.open("ab") as stream:
            while committed < total_records:
                stop = min(total_records, committed + self.batch_size)
                produced = 0
                for record in produce(committed, stop):
                    if produced >= stop - committed:
                        raise StreamingCheckpointError(
                            f"producer emitted too many {record_type} records for a batch"
                        )
                    payload = (
                        record.model_dump(mode="json") if isinstance(record, BaseModel) else record
                    )
                    encoded = canonical_json_bytes(payload)
                    stream.write(encoded)
                    digest.update(encoded)
                    produced += 1
                expected = stop - committed
                if produced != expected:
                    raise StreamingCheckpointError(
                        f"producer emitted {produced} {record_type} records; expected {expected}"
                    )
                stream.flush()
                os.fsync(stream.fileno())
                committed = stop
                active = active.model_copy(
                    update={
                        "committed_records": committed,
                        "committed_bytes": stream.tell(),
                        "prefix_sha256": digest.hexdigest(),
                    }
                )
                self._set_checkpoint(active_file=active)
                batches_committed += 1
                if (
                    fail_after_committed_batches is not None
                    and batches_committed >= fail_after_committed_batches
                ):
                    raise RuntimeError("injected streaming interruption")

        os.replace(partial_path, final_path)
        self._fsync_directory(final_path.parent)
        entry = FileManifest(
            relative_path=relative_path,
            media_type="application/x-ndjson",
            record_type=record_type,
            records=total_records,
            size_bytes=final_path.stat().st_size,
            sha256=sha256_file(final_path),
        )
        completed_files = dict(self._checkpoint.completed_files)
        completed_files[record_type] = entry
        self._replace_checkpoint(completed_files=completed_files, active_file=None)
        return entry

    def remove_checkpoint(self) -> None:
        """Remove state only after the caller has built and validated a final manifest."""

        self.state_path.unlink()
        self._fsync_directory(self.root)

    def _initialize(self, dataset_id: str, profile_sha256: str, *, resume: bool) -> None:
        if self.state_path.exists():
            if not resume:
                raise StreamingCheckpointError(
                    "generation checkpoint exists; pass resume explicitly after inspection"
                )
            self._checkpoint = StreamingCheckpoint.model_validate_json(self.state_path.read_bytes())
            if (
                self._checkpoint.dataset_id != dataset_id
                or self._checkpoint.profile_sha256 != profile_sha256
            ):
                raise StreamingCheckpointError(
                    "checkpoint dataset identity or profile checksum does not match"
                )
            return
        if resume:
            raise StreamingCheckpointError("resume requested but no generation checkpoint exists")
        if self.root.exists() and any(self.root.iterdir()):
            raise StreamingCheckpointError(
                "staging directory is not empty and has no generation checkpoint"
            )
        self.root.mkdir(parents=True, exist_ok=True)
        self._checkpoint = StreamingCheckpoint(
            dataset_id=dataset_id,
            profile_sha256=profile_sha256,
        )
        self._write_checkpoint()

    def _recover_completed_rename_if_needed(self) -> None:
        active = self._checkpoint.active_file
        if active is None or active.committed_records != active.total_records:
            return
        final_path = self._safe_path(active.relative_path)
        partial_path = final_path.with_name(f"{final_path.name}.part")
        if partial_path.exists() or not final_path.is_file():
            return
        if (
            final_path.stat().st_size != active.committed_bytes
            or sha256_file(final_path) != active.prefix_sha256
        ):
            raise StreamingCheckpointError("renamed completed file does not match checkpoint")
        entry = FileManifest(
            relative_path=active.relative_path,
            media_type="application/x-ndjson",
            record_type=active.record_type,
            records=active.total_records,
            size_bytes=active.committed_bytes,
            sha256=active.prefix_sha256,
        )
        completed = dict(self._checkpoint.completed_files)
        completed[active.record_type] = entry
        self._replace_checkpoint(completed_files=completed, active_file=None)

    def _verify_active_checkpoint(self) -> None:
        active = self._checkpoint.active_file
        if active is None:
            return
        final_path = self._safe_path(active.relative_path)
        partial_path = final_path.with_name(f"{final_path.name}.part")
        self._prepare_partial(partial_path, active)

    def _prepare_partial(self, partial_path: Path, active: ActiveFileCheckpoint) -> hashlib._Hash:
        if not partial_path.exists():
            if active.committed_bytes != 0:
                raise StreamingCheckpointError("checkpointed partial file is missing")
            partial_path.touch()
        size = partial_path.stat().st_size
        if size < active.committed_bytes:
            raise StreamingCheckpointError("partial file is shorter than its committed checkpoint")
        digest = hashlib.sha256()
        remaining = active.committed_bytes
        with partial_path.open("rb") as stream:
            while remaining:
                block = stream.read(min(1024 * 1024, remaining))
                if not block:
                    break
                digest.update(block)
                remaining -= len(block)
        if remaining or digest.hexdigest() != active.prefix_sha256:
            raise StreamingCheckpointError("committed prefix checksum does not match checkpoint")
        if size > active.committed_bytes:
            with partial_path.open("r+b") as stream:
                stream.truncate(active.committed_bytes)
                stream.flush()
                os.fsync(stream.fileno())
        return digest

    def _verify_completed(
        self, entry: FileManifest, relative_path: str, total_records: int
    ) -> None:
        if entry.relative_path != relative_path or entry.records != total_records:
            raise StreamingCheckpointError("completed file metadata does not match request")
        path = self._safe_path(relative_path)
        if (
            not path.is_file()
            or path.stat().st_size != entry.size_bytes
            or sha256_file(path) != entry.sha256
        ):
            raise StreamingCheckpointError("completed file checksum does not match checkpoint")

    def _safe_path(self, relative_path: str) -> Path:
        candidate = (self.root / relative_path).resolve()
        if not candidate.is_relative_to(self.root) or candidate == self.root:
            raise StreamingCheckpointError("output path escapes the staging directory")
        return candidate

    def _set_checkpoint(self, *, active_file: ActiveFileCheckpoint | None) -> None:
        self._replace_checkpoint(
            completed_files=self._checkpoint.completed_files,
            active_file=active_file,
        )

    def _replace_checkpoint(
        self,
        *,
        completed_files: dict[str, FileManifest],
        active_file: ActiveFileCheckpoint | None,
    ) -> None:
        self._checkpoint = self._checkpoint.model_copy(
            update={"completed_files": completed_files, "active_file": active_file}
        )
        self._write_checkpoint()

    def _write_checkpoint(self) -> None:
        temporary = self.state_path.with_suffix(".json.tmp")
        payload = canonical_json_bytes(self._checkpoint.model_dump(mode="json"))
        with temporary.open("wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self.state_path)
        self._fsync_directory(self.root)

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


__all__ = [
    "ActiveFileCheckpoint",
    "ResumableJsonlWriter",
    "StreamingCheckpoint",
    "StreamingCheckpointError",
]
