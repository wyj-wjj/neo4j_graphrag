"""Tenant-isolated local and S3-compatible object stores."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from graphrag.domain.errors import DependencyError, NotFoundError, ValidationError


def _safe_segment(value: str, *, name: str) -> str:
    cleaned = value.strip()
    if not cleaned or cleaned in {".", ".."}:
        raise ValidationError(f"非法{name}")
    if any(character in cleaned for character in ("/", "\\", "\x00")):
        raise ValidationError(f"非法{name}")
    return cleaned


def _safe_object_key(value: str) -> str:
    cleaned = value.strip()
    path = Path(cleaned)
    if (
        not cleaned
        or path.is_absolute()
        or "\\" in cleaned
        or "\x00" in cleaned
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ValidationError("非法对象路径")
    return path.as_posix()


class LocalObjectStore:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _resolve(self, tenant_id: str, object_key: str) -> Path:
        tenant = _safe_segment(tenant_id, name="租户标识")
        key = _safe_object_key(object_key)
        candidate = (self.root / tenant / key).resolve()
        tenant_root = (self.root / tenant).resolve()
        if tenant_root not in candidate.parents:
            raise ValidationError("非法对象路径")
        return candidate

    async def save(
        self, tenant_id: str, document_id: str, filename: str, content: bytes
    ) -> tuple[str, str]:
        if not content:
            raise ValidationError("上传文件不能为空")
        document = _safe_segment(document_id, name="文档标识")
        suffix = Path(filename).suffix.lower()
        digest = hashlib.sha256(content).hexdigest()
        object_key = f"{document}/{digest[:16]}/original{suffix}"
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


class S3ClientLike(Protocol):
    def put_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def delete_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def head_bucket(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def close(self) -> None: ...


class S3ObjectStore:
    """Tenant-prefixed S3-compatible original-file store with integrity metadata."""

    def __init__(
        self,
        *,
        bucket: str,
        region: str,
        endpoint_url: str | None,
        access_key_id: str | None,
        secret_access_key: str | None,
        session_token: str | None,
        force_path_style: bool,
        verify_tls: bool,
        sse_algorithm: str | None,
        kms_key_id: str | None,
        client: S3ClientLike | None = None,
    ) -> None:
        self.bucket = _safe_segment(bucket, name="Bucket")
        self.sse_algorithm = sse_algorithm
        self.kms_key_id = kms_key_id
        if client is None:
            import boto3
            from botocore.config import Config

            client = boto3.client(
                "s3",
                region_name=region,
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
                aws_session_token=session_token,
                verify=verify_tls,
                config=Config(
                    signature_version="s3v4",
                    retries={"mode": "standard", "max_attempts": 3},
                    s3={"addressing_style": "path" if force_path_style else "auto"},
                ),
            )
        self.client = client

    async def save(
        self, tenant_id: str, document_id: str, filename: str, content: bytes
    ) -> tuple[str, str]:
        if not content:
            raise ValidationError("上传文件不能为空")
        tenant = _safe_segment(tenant_id, name="租户标识")
        document = _safe_segment(document_id, name="文档标识")
        suffix = Path(filename).suffix.lower()
        digest = hashlib.sha256(content).hexdigest()
        object_key = f"{document}/{digest[:16]}/original{suffix}"
        parameters: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": self._physical_key(tenant, object_key),
            "Body": content,
            "ContentLength": len(content),
            "Metadata": {"tenant-id": tenant, "sha256": digest},
        }
        if self.sse_algorithm:
            parameters["ServerSideEncryption"] = self.sse_algorithm
        if self.sse_algorithm == "aws:kms" and self.kms_key_id:
            parameters["SSEKMSKeyId"] = self.kms_key_id
        try:
            await asyncio.to_thread(self.client.put_object, **parameters)
        except Exception as exc:
            raise DependencyError("s3", "对象写入失败") from exc
        return object_key, digest

    async def read(self, tenant_id: str, object_key: str) -> bytes:
        tenant = _safe_segment(tenant_id, name="租户标识")
        key = self._physical_key(tenant, object_key)
        try:
            response = await asyncio.to_thread(
                self.client.get_object,
                Bucket=self.bucket,
                Key=key,
            )
            body = response["Body"]
            try:
                content = bytes(await asyncio.to_thread(body.read))
            finally:
                close = getattr(body, "close", None)
                if close is not None:
                    await asyncio.to_thread(close)
        except Exception as exc:
            if self._not_found(exc):
                raise NotFoundError("对象不存在") from exc
            raise DependencyError("s3", "对象读取失败") from exc
        metadata = {
            str(name).lower(): str(value) for name, value in response.get("Metadata", {}).items()
        }
        expected_hash = metadata.get("sha256")
        if metadata.get("tenant-id") != tenant or not expected_hash:
            raise DependencyError("s3", "对象租户或完整性元数据缺失", retryable=False)
        if hashlib.sha256(content).hexdigest() != expected_hash:
            raise DependencyError("s3", "对象完整性校验失败", retryable=False)
        return content

    async def delete(self, tenant_id: str, object_key: str) -> None:
        tenant = _safe_segment(tenant_id, name="租户标识")
        key = self._physical_key(tenant, object_key)
        try:
            await asyncio.to_thread(
                self.client.delete_object,
                Bucket=self.bucket,
                Key=key,
            )
        except Exception as exc:
            raise DependencyError("s3", "对象删除失败") from exc

    async def health(self) -> dict[str, str]:
        try:
            await asyncio.to_thread(self.client.head_bucket, Bucket=self.bucket)
        except Exception as exc:
            raise DependencyError("s3", "对象存储 Bucket 不可用") from exc
        return {"status": "ready"}

    async def close(self) -> None:
        await asyncio.to_thread(self.client.close)

    @staticmethod
    def _not_found(exc: Exception) -> bool:
        response = getattr(exc, "response", {})
        error = response.get("Error", {}) if isinstance(response, dict) else {}
        return str(error.get("Code", "")) in {"NoSuchKey", "404", "NotFound"}

    @staticmethod
    def _physical_key(tenant_id: str, object_key: str) -> str:
        return f"{tenant_id}/{_safe_object_key(object_key)}"


__all__ = ["LocalObjectStore", "S3ClientLike", "S3ObjectStore"]
