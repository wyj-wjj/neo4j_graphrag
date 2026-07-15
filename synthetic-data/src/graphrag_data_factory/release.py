"""Download and atomically install immutable synthetic-data release assets."""

from __future__ import annotations

import hashlib
import os
import shutil
import tarfile
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Final

from graphrag_data_factory.constants import DATASET_VERSION

RELEASE_TAG: Final = "synthetic-commerce-v1-data-v3"
RELEASE_DOWNLOAD_BASE_URL: Final = (
    "https://github.com/wyj-wjj/neo4j_graphrag/releases/download/" + RELEASE_TAG
)


@dataclass(frozen=True, slots=True)
class ReleaseAsset:
    profile: str
    filename: str
    archive_sha256: str
    archive_size_bytes: int
    manifest_sha256: str


RELEASE_ASSETS: Final[dict[str, ReleaseAsset]] = {
    "dev-standard": ReleaseAsset(
        profile="dev-standard",
        filename="synthetic-commerce-v1-dev-standard.tar.gz",
        archive_sha256="4bcbc5f6a0792a6383a4a5a3eaf78490084d72d18cb99758c8111b7d85a61237",
        archive_size_bytes=445_969_395,
        manifest_sha256="5bbea39339499703796fe5d9d01fdc8541f4414d11ae3b6f3ad65f30baef7fe2",
    ),
    "failure-lab": ReleaseAsset(
        profile="failure-lab",
        filename="synthetic-commerce-v1-failure-lab.tar.gz",
        archive_sha256="877e7b225532d70972e46343b97c9079c379f47f1fe74a11acac23c461b01ffd",
        archive_size_bytes=197_895_810,
        manifest_sha256="2c64f88dc89625a4b44f504e62c7730cfe6caad003a365c19ac34f1b55d98a60",
    ),
}

DatasetValidator = Callable[[Path], object]


class ReleaseDataError(ValueError):
    """Raised when a release asset is missing, unsafe, or fails integrity checks."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def install_release_archive(
    archive_path: Path,
    asset: ReleaseAsset,
    *,
    output_root: Path,
    replace: bool = False,
    validator: DatasetValidator | None = None,
) -> Path:
    """Verify, safely extract, validate, and atomically publish one profile."""

    archive = archive_path.resolve()
    _verify_archive(archive, asset)
    root = output_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    target = root / asset.profile
    if target.exists() and not replace:
        _verify_installed_dataset(target, asset, validator)
        return target

    with tempfile.TemporaryDirectory(prefix=f".{asset.profile}.release-", dir=root) as temporary:
        temporary_root = Path(temporary)
        extracted_root = temporary_root / "extracted"
        extracted_root.mkdir()
        with tarfile.open(archive, mode="r:gz") as bundle:
            members = bundle.getmembers()
            _validate_members(members, asset.profile)
            bundle.extractall(extracted_root, members=members, filter="data")
        candidate = extracted_root / asset.profile
        _verify_installed_dataset(candidate, asset, validator)
        _publish_directory(candidate, target, replace=replace)
    return target


def download_release_asset(
    asset: ReleaseAsset,
    *,
    output_root: Path,
    release_base_url: str = RELEASE_DOWNLOAD_BASE_URL,
    replace: bool = False,
    validator: DatasetValidator | None = None,
) -> Path:
    """Download a pinned public release asset and install it atomically."""

    root = output_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    target = root / asset.profile
    if target.exists() and not replace:
        _verify_installed_dataset(target, asset, validator)
        return target

    url = f"{release_base_url.rstrip('/')}/{asset.filename}"
    _validate_download_url(url)
    request = urllib.request.Request(  # noqa: S310 - URL scheme is restricted below.
        url,
        headers={"User-Agent": "neo4j-graphrag-synthetic-data-fetcher/1"},
    )
    with tempfile.TemporaryDirectory(prefix=f".{asset.profile}.download-", dir=root) as temporary:
        archive = Path(temporary) / asset.filename
        try:
            with (
                urllib.request.urlopen(  # noqa: S310 - URL scheme is restricted below.
                    request,
                    timeout=60,
                ) as response,
                archive.open("xb") as stream,
            ):
                shutil.copyfileobj(response, stream, length=1024 * 1024)
                stream.flush()
                os.fsync(stream.fileno())
        except (OSError, urllib.error.URLError) as error:
            raise ReleaseDataError(f"failed to download {asset.profile} release asset") from error
        return install_release_archive(
            archive,
            asset,
            output_root=root,
            replace=replace,
            validator=validator,
        )


def fetch_release_profiles(
    profiles: Iterable[str],
    *,
    output_root: Path,
    release_base_url: str = RELEASE_DOWNLOAD_BASE_URL,
    replace: bool = False,
    validator: DatasetValidator | None = None,
) -> tuple[Path, ...]:
    requested = tuple(profiles)
    unknown = sorted(set(requested) - RELEASE_ASSETS.keys())
    if unknown:
        raise ReleaseDataError(f"unknown release profiles: {', '.join(unknown)}")
    return tuple(
        download_release_asset(
            RELEASE_ASSETS[profile],
            output_root=output_root,
            release_base_url=release_base_url,
            replace=replace,
            validator=validator,
        )
        for profile in requested
    )


def default_release_output_root(project_root: Path) -> Path:
    return project_root / "generated" / DATASET_VERSION


def _validate_download_url(url: str) -> None:
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ReleaseDataError("release asset URL must use credential-free HTTPS")


def _verify_archive(archive: Path, asset: ReleaseAsset) -> None:
    if not archive.is_file():
        raise ReleaseDataError(f"release archive does not exist: {archive}")
    actual_size = archive.stat().st_size
    if actual_size != asset.archive_size_bytes:
        raise ReleaseDataError(
            f"archive size mismatch for {asset.profile}: "
            f"expected {asset.archive_size_bytes}, got {actual_size}"
        )
    actual_digest = sha256_file(archive)
    if actual_digest != asset.archive_sha256:
        raise ReleaseDataError(f"archive checksum mismatch for {asset.profile}")


def _validate_members(members: list[tarfile.TarInfo], expected_root: str) -> None:
    if not members:
        raise ReleaseDataError("release archive is empty")
    for member in members:
        path = PurePosixPath(member.name)
        if path.is_absolute() or not path.parts or ".." in path.parts:
            raise ReleaseDataError(f"unsafe archive member: {member.name}")
        if path.parts[0] != expected_root:
            raise ReleaseDataError(f"archive member is outside {expected_root}: {member.name}")
        if not (member.isdir() or member.isfile()):
            raise ReleaseDataError(f"unsupported archive member type: {member.name}")


def _verify_installed_dataset(
    dataset_root: Path,
    asset: ReleaseAsset,
    validator: DatasetValidator | None,
) -> None:
    if not dataset_root.is_dir():
        raise ReleaseDataError(f"dataset directory is missing: {dataset_root}")
    manifest = dataset_root / "manifest.json"
    if not manifest.is_file() or sha256_file(manifest) != asset.manifest_sha256:
        raise ReleaseDataError(f"manifest checksum mismatch for {asset.profile}")
    if validator is not None:
        validator(dataset_root)


def _publish_directory(candidate: Path, target: Path, *, replace: bool) -> None:
    if not target.exists():
        os.replace(candidate, target)
        return
    if not replace:
        raise ReleaseDataError(f"dataset already exists: {target}")
    backup = target.parent / f".{target.name}.release-backup"
    if backup.exists():
        raise ReleaseDataError(f"release backup already exists: {backup}")
    os.replace(target, backup)
    try:
        os.replace(candidate, target)
    except BaseException:
        if not target.exists() and backup.exists():
            os.replace(backup, target)
        raise
    shutil.rmtree(backup)


__all__ = [
    "RELEASE_ASSETS",
    "RELEASE_DOWNLOAD_BASE_URL",
    "RELEASE_TAG",
    "ReleaseAsset",
    "ReleaseDataError",
    "default_release_output_root",
    "download_release_asset",
    "fetch_release_profiles",
    "install_release_archive",
    "sha256_file",
]
