"""Explicit, non-production client for exercising the application's upload API."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Literal

import httpx
from pydantic import BaseModel, ConfigDict

from graphrag_data_factory.models import PhysicalKnowledgeFileRecord
from graphrag_data_factory.validator import DatasetValidator

TERMINAL_STATUSES = frozenset({"completed", "partial_failed", "failed"})


class UploadTask(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    task_id: str
    document_id: str
    status: str
    error_code: str | None = None
    error_message: str | None = None


class UploadResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    relative_path: str
    expected_outcome: Literal["parse_success", "ocr_success", "reject_corrupt"]
    http_status: int
    task: UploadTask | None
    rejection_body: str | None


class KnowledgeUploadClient:
    """Upload a validated synthetic dataset without reading environment variables."""

    def __init__(
        self,
        *,
        base_url: str,
        bearer_token: str,
        timeout_seconds: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not bearer_token.strip():
            raise ValueError("bearer_token must be supplied explicitly")
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {bearer_token}"},
            timeout=httpx.Timeout(timeout_seconds),
            transport=transport,
        )

    async def __aenter__(self) -> KnowledgeUploadClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def upload_dataset(
        self,
        dataset_dir: Path,
        *,
        include_expected_rejections: bool = False,
        wait_for_terminal: bool = True,
        poll_interval_seconds: float = 0.05,
        max_poll_attempts: int = 200,
    ) -> tuple[UploadResult, ...]:
        root = dataset_dir.resolve()
        self._assert_synthetic_dataset(root)
        records = self._load_physical_records(root)
        results: list[UploadResult] = []
        for record in records:
            if record.expected_outcome == "reject_corrupt" and not include_expected_rejections:
                continue
            response = await self._client.post(
                "/api/v1/documents",
                data={"title": f"Synthetic fixture: {record.relative_path}"},
                files={
                    "file": (
                        Path(record.relative_path).name,
                        (root / record.relative_path).read_bytes(),
                        record.mime_type,
                    )
                },
            )
            if record.expected_outcome == "reject_corrupt":
                if response.status_code < 400:
                    raise RuntimeError(
                        f"corrupt upload unexpectedly accepted: {record.relative_path}"
                    )
                results.append(
                    UploadResult(
                        relative_path=record.relative_path,
                        expected_outcome=record.expected_outcome,
                        http_status=response.status_code,
                        task=None,
                        rejection_body=response.text[:1000],
                    )
                )
                continue
            response.raise_for_status()
            task = UploadTask.model_validate(response.json())
            if wait_for_terminal:
                task = await self._wait_for_terminal(
                    task.task_id,
                    poll_interval_seconds=poll_interval_seconds,
                    max_poll_attempts=max_poll_attempts,
                )
            results.append(
                UploadResult(
                    relative_path=record.relative_path,
                    expected_outcome=record.expected_outcome,
                    http_status=response.status_code,
                    task=task,
                    rejection_body=None,
                )
            )
        return tuple(results)

    async def _wait_for_terminal(
        self, task_id: str, *, poll_interval_seconds: float, max_poll_attempts: int
    ) -> UploadTask:
        if max_poll_attempts < 1:
            raise ValueError("max_poll_attempts must be positive")
        for attempt in range(max_poll_attempts):
            response = await self._client.get(f"/api/v1/ingestion-tasks/{task_id}")
            response.raise_for_status()
            task = UploadTask.model_validate(response.json())
            if task.status in TERMINAL_STATUSES:
                return task
            if attempt + 1 < max_poll_attempts:
                await asyncio.sleep(poll_interval_seconds)
        raise TimeoutError(f"ingestion task did not reach terminal state: {task_id}")

    @staticmethod
    def _assert_synthetic_dataset(root: Path) -> None:
        manifest = DatasetValidator().validate(root)
        if not manifest.is_synthetic or manifest.production_slo_eligible:
            raise ValueError("refusing to upload a dataset without synthetic-only scope")

    @staticmethod
    def _load_physical_records(root: Path) -> tuple[PhysicalKnowledgeFileRecord, ...]:
        path = root / "physical-knowledge-files.jsonl"
        if not path.is_file():
            raise ValueError("physical knowledge metadata is missing")
        records = tuple(
            PhysicalKnowledgeFileRecord.model_validate(json.loads(line))
            for line in path.read_text(encoding="utf-8").splitlines()
            if line
        )
        return tuple(sorted(records, key=lambda item: item.relative_path))


__all__ = ["KnowledgeUploadClient", "UploadResult", "UploadTask"]
