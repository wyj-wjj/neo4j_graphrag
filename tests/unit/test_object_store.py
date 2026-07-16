from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any, ClassVar

import pytest

from graphrag.domain.errors import DependencyError, NotFoundError, ValidationError
from graphrag.infrastructure.object_store import LocalObjectStore, S3ObjectStore


class MissingObject(Exception):
    response: ClassVar[dict[str, dict[str, str]]] = {"Error": {"Code": "NoSuchKey"}}


class MemoryS3Client:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, dict[str, str]]] = {}
        self.closed = False

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.objects[kwargs["Key"]] = (bytes(kwargs["Body"]), dict(kwargs["Metadata"]))
        return {"ETag": "fake"}

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        try:
            content, metadata = self.objects[kwargs["Key"]]
        except KeyError as exc:
            raise MissingObject from exc
        return {"Body": BytesIO(content), "Metadata": metadata}

    def delete_object(self, **kwargs: Any) -> dict[str, Any]:
        self.objects.pop(kwargs["Key"], None)
        return {}

    def head_bucket(self, **kwargs: Any) -> dict[str, Any]:
        assert kwargs["Bucket"] == "knowledge-originals"
        return {}

    def close(self) -> None:
        self.closed = True


def _store(client: MemoryS3Client) -> S3ObjectStore:
    return S3ObjectStore(
        bucket="knowledge-originals",
        region="us-east-1",
        endpoint_url="http://minio:9000",
        access_key_id=None,
        secret_access_key=None,
        session_token=None,
        force_path_style=True,
        verify_tls=True,
        sse_algorithm="AES256",
        kms_key_id=None,
        client=client,
    )


@pytest.mark.asyncio
async def test_s3_object_store_is_tenant_prefixed_integrity_checked_and_idempotent() -> None:
    client = MemoryS3Client()
    store = _store(client)

    key, digest = await store.save("tenant-a", "document-a", "policy.txt", b"policy")
    repeated_key, repeated_digest = await store.save(
        "tenant-a", "document-a", "policy.txt", b"policy"
    )

    assert key == repeated_key
    assert digest == repeated_digest
    assert set(client.objects) == {f"tenant-a/{key}"}
    assert await store.read("tenant-a", key) == b"policy"
    assert await store.health() == {"status": "ready"}
    with pytest.raises(NotFoundError):
        await store.read("tenant-b", key)
    await store.delete("tenant-a", key)
    await store.delete("tenant-a", key)
    with pytest.raises(NotFoundError):
        await store.read("tenant-a", key)
    await store.close()
    assert client.closed is True


@pytest.mark.asyncio
async def test_s3_object_store_rejects_corrupt_content_and_paths() -> None:
    client = MemoryS3Client()
    store = _store(client)
    key, _ = await store.save("tenant-a", "document-a", "policy.txt", b"policy")
    _, metadata = client.objects[f"tenant-a/{key}"]
    client.objects[f"tenant-a/{key}"] = (b"tampered", metadata)

    with pytest.raises(DependencyError, match="完整性"):
        await store.read("tenant-a", key)
    with pytest.raises(ValidationError):
        await store.read("../tenant-b", key)
    with pytest.raises(ValidationError):
        await store.read("tenant-a", "../secret")


@pytest.mark.asyncio
async def test_local_object_store_rejects_tenant_escape(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path)
    with pytest.raises(ValidationError):
        await store.save("..", "document-a", "policy.txt", b"policy")
