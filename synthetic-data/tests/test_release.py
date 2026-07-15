from __future__ import annotations

import hashlib
import io
import tarfile
from pathlib import Path

import pytest

from graphrag_data_factory.release import (
    RELEASE_ASSETS,
    ReleaseAsset,
    ReleaseDataError,
    download_release_asset,
    install_release_archive,
    sha256_file,
)


def _asset_for_archive(archive: Path, *, profile: str, manifest_sha256: str) -> ReleaseAsset:
    return ReleaseAsset(
        profile=profile,
        filename=archive.name,
        archive_sha256=sha256_file(archive),
        archive_size_bytes=archive.stat().st_size,
        manifest_sha256=manifest_sha256,
    )


def _write_archive(archive: Path, *, profile: str, unsafe_member: str | None = None) -> bytes:
    manifest = b'{"is_synthetic":true,"status":"validated"}\n'
    with tarfile.open(archive, "w:gz") as bundle:
        root = tarfile.TarInfo(profile)
        root.type = tarfile.DIRTYPE
        bundle.addfile(root)
        manifest_info = tarfile.TarInfo(f"{profile}/manifest.json")
        manifest_info.size = len(manifest)
        bundle.addfile(manifest_info, io.BytesIO(manifest))
        data = b"synthetic fixture\n"
        data_info = tarfile.TarInfo(unsafe_member or f"{profile}/records.jsonl")
        data_info.size = len(data)
        bundle.addfile(data_info, io.BytesIO(data))
    return manifest


@pytest.mark.contract
def test_release_assets_pin_the_materialized_profiles() -> None:
    assert set(RELEASE_ASSETS) == {"dev-standard", "failure-lab"}
    assert RELEASE_ASSETS["dev-standard"].archive_size_bytes == 445_969_397
    assert RELEASE_ASSETS["failure-lab"].archive_size_bytes == 197_895_809
    assert RELEASE_ASSETS["dev-standard"].manifest_sha256.startswith("5bbea393")
    assert RELEASE_ASSETS["failure-lab"].manifest_sha256.startswith("2c64f88d")


@pytest.mark.contract
def test_release_archive_is_verified_and_atomically_installed(tmp_path: Path) -> None:
    archive = tmp_path / "profile.tar.gz"
    manifest = _write_archive(archive, profile="dev-standard")
    asset = _asset_for_archive(
        archive,
        profile="dev-standard",
        manifest_sha256=hashlib.sha256(manifest).hexdigest(),
    )
    validated: list[Path] = []
    installed = install_release_archive(
        archive,
        asset,
        output_root=tmp_path / "generated",
        validator=validated.append,
    )
    assert installed.is_dir()
    assert (installed / "records.jsonl").read_text(encoding="utf-8") == "synthetic fixture\n"
    assert validated and validated[0].name == "dev-standard"


@pytest.mark.fault
def test_release_archive_rejects_checksum_mismatch(tmp_path: Path) -> None:
    archive = tmp_path / "profile.tar.gz"
    manifest = _write_archive(archive, profile="failure-lab")
    asset = _asset_for_archive(
        archive,
        profile="failure-lab",
        manifest_sha256=hashlib.sha256(manifest).hexdigest(),
    )
    archive.write_bytes(archive.read_bytes() + b"tampered")
    with pytest.raises(ReleaseDataError, match="size mismatch"):
        install_release_archive(archive, asset, output_root=tmp_path / "generated")


@pytest.mark.fault
def test_release_archive_rejects_path_traversal(tmp_path: Path) -> None:
    archive = tmp_path / "unsafe.tar.gz"
    manifest = _write_archive(
        archive,
        profile="failure-lab",
        unsafe_member="failure-lab/../escape.txt",
    )
    asset = _asset_for_archive(
        archive,
        profile="failure-lab",
        manifest_sha256=hashlib.sha256(manifest).hexdigest(),
    )
    with pytest.raises(ReleaseDataError, match="unsafe archive member"):
        install_release_archive(archive, asset, output_root=tmp_path / "generated")


@pytest.mark.fault
def test_release_download_rejects_non_https_base_url(tmp_path: Path) -> None:
    asset = RELEASE_ASSETS["dev-standard"]
    with pytest.raises(ReleaseDataError, match="credential-free HTTPS"):
        download_release_asset(
            asset,
            output_root=tmp_path / "generated",
            release_base_url="file:///tmp/untrusted",
        )
