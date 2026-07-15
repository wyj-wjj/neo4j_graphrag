"""Tenant-isolated local object storage with path traversal protection."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

from graphrag.domain.errors import NotFoundError, ValidationError


class LocalObjectStore:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _resolve(self, tenant_id: str, object_key: str) -> Path:
        candidate = (self.root / tenant_id / object_key).resolve()
        tenant_root = (self.root / tenant_id).resolve()
        if tenant_root not in candidate.parents:
            raise ValidationError("非法对象路径")
        return candidate

    async def save(
        self, tenant_id: str, document_id: str, filename: str, content: bytes
    ) -> tuple[str, str]:
        if not content:
            raise ValidationError("上传文件不能为空")
        suffix = Path(filename).suffix.lower()
        digest = hashlib.sha256(content).hexdigest()
        object_key = f"{document_id}/{digest[:16]}/original{suffix}"
        target = self._resolve(tenant_id, object_key)
        target.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(target.write_bytes, content)
        return object_key, digest

    async def read(self, tenant_id: str, object_key: str) -> bytes:
        target = self._resolve(tenant_id, object_key)
        if not target.is_file():
            raise NotFoundError("对象不存在")
        return await asyncio.to_thread(target.read_bytes)

    async def delete(self, tenant_id: str, object_key: str) -> None:
        target = self._resolve(tenant_id, object_key)
        if target.exists():
            await asyncio.to_thread(target.unlink)
