"""Deterministic primitives shared by all data domains."""

from __future__ import annotations

import hashlib
import json
import random
import uuid
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

import yaml

from graphrag_data_factory.constants import DATASET_VERSION, SUPPORTED_PROFILES
from graphrag_data_factory.models import DatasetProfile

MONEY_QUANTUM = Decimal("0.01")
RATE_QUANTUM = Decimal("0.0001")
ID_NAMESPACE = uuid.UUID("7d7cda32-f034-55b8-8c4d-8c48ba3d7fd5")


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize without locale, whitespace or dictionary-order ambiguity."""

    serialized = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return (serialized + "\n").encode()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def derive_seed(root_seed: int, domain: str) -> int:
    material = f"{DATASET_VERSION}:{root_seed}:{domain}".encode()
    return int.from_bytes(hashlib.sha256(material).digest()[:8], "big")


def seed_id(root_seed: int, domain: str) -> str:
    material = f"{DATASET_VERSION}:{root_seed}:{domain}".encode()
    return hashlib.sha256(material).hexdigest()[:16]


def deterministic_id(dataset_id: str, domain: str, logical_key: str) -> str:
    return str(uuid.uuid5(ID_NAMESPACE, f"{dataset_id}:{domain}:{logical_key}"))


def record_random(root_seed: int, domain: str, logical_key: str) -> random.Random:
    # This PRNG creates reproducible fixtures and is never used for secrets or authentication.
    return random.Random(derive_seed(root_seed, f"{domain}:{logical_key}"))  # noqa: S311


def utc_time(base: datetime, *, minutes: int = 0, seconds: int = 0) -> datetime:
    if base.tzinfo is None or base.utcoffset() is None:
        raise ValueError("base time must be timezone-aware")
    return (base.astimezone(UTC) + timedelta(minutes=minutes, seconds=seconds)).replace(
        microsecond=0
    )


def money(value: Decimal | str | int) -> Decimal:
    if isinstance(value, float):
        raise TypeError("float is forbidden for money")
    return Decimal(value).quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)


def load_profile(name: str, *, profiles_dir: Path | None = None) -> tuple[DatasetProfile, Path]:
    if name not in SUPPORTED_PROFILES:
        raise ValueError(f"unsupported profile: {name}")
    directory = profiles_dir or project_root() / "profiles"
    path = directory / f"{name}.yaml"
    if not path.is_file():
        raise FileNotFoundError(f"profile file does not exist: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("profile root must be a mapping")
    profile = DatasetProfile.model_validate(raw)
    if profile.name != name or path.stem != name:
        raise ValueError("profile name does not match its file name")
    return profile, path
