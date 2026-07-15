"""Upload validation, content sniffing and archive expansion limits."""

from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path

from graphrag.domain.errors import ValidationError


@dataclass(frozen=True, slots=True)
class ValidatedUpload:
    filename: str
    extension: str
    mime_type: str
    content: bytes


_MIME_BY_EXTENSION = {
    ".pdf": "application/pdf",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    ".html": "text/html",
    ".htm": "text/html",
    ".txt": "text/plain",
    ".md": "text/markdown",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
}


class UploadValidator:
    def __init__(
        self,
        *,
        allowed_extensions: frozenset[str],
        max_bytes: int,
        max_uncompressed_bytes: int = 100 * 1024 * 1024,
        max_archive_entries: int = 10_000,
    ) -> None:
        self.allowed_extensions = allowed_extensions
        self.max_bytes = max_bytes
        self.max_uncompressed_bytes = max_uncompressed_bytes
        self.max_archive_entries = max_archive_entries

    def validate(self, filename: str, content: bytes) -> ValidatedUpload:
        clean_name = Path(filename).name
        if clean_name != filename or not clean_name or re.search(r"[\x00-\x1f]", clean_name):
            raise ValidationError("文件名包含非法路径或控制字符")
        extension = Path(clean_name).suffix.lower()
        if extension not in self.allowed_extensions or extension not in _MIME_BY_EXTENSION:
            raise ValidationError("不支持的文件类型")
        if not content:
            raise ValidationError("上传文件不能为空")
        if len(content) > self.max_bytes:
            raise ValidationError("上传文件超过大小限制")
        self._validate_signature(extension, content)
        if extension in {".docx", ".xlsx", ".pptx"}:
            self._validate_archive(content)
        return ValidatedUpload(clean_name, extension, _MIME_BY_EXTENSION[extension], content)

    def _validate_signature(self, extension: str, content: bytes) -> None:
        signatures = {
            ".pdf": (b"%PDF-",),
            ".png": (b"\x89PNG\r\n\x1a\n",),
            ".jpg": (b"\xff\xd8\xff",),
            ".jpeg": (b"\xff\xd8\xff",),
            ".docx": (b"PK\x03\x04",),
            ".xlsx": (b"PK\x03\x04",),
            ".pptx": (b"PK\x03\x04",),
        }
        expected = signatures.get(extension)
        if expected is not None and not any(content.startswith(item) for item in expected):
            raise ValidationError("文件扩展名与实际内容不匹配")
        if extension in {".html", ".htm", ".txt", ".md"}:
            try:
                content[:4096].decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ValidationError("文本文件必须使用 UTF-8 编码") from exc

    def _validate_archive(self, content: bytes) -> None:
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                infos = archive.infolist()
                if len(infos) > self.max_archive_entries:
                    raise ValidationError("压缩文件条目数量超过限制")
                uncompressed = sum(item.file_size for item in infos)
                compressed = sum(max(item.compress_size, 1) for item in infos)
                if uncompressed > self.max_uncompressed_bytes or uncompressed / compressed > 100:
                    raise ValidationError("压缩文件展开体积或压缩比超过安全限制")
                if any(".." in Path(item.filename).parts for item in infos):
                    raise ValidationError("压缩文件包含路径穿越条目")
        except zipfile.BadZipFile as exc:
            raise ValidationError("Office 文件结构损坏") from exc
