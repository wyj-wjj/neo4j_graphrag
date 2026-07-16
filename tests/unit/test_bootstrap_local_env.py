from __future__ import annotations

from pathlib import Path

from scripts.bootstrap_local_env import _render


def _values(rendered: str) -> dict[str, str]:
    return {
        key: value
        for line in rendered.splitlines()
        if line and not line.startswith("#")
        for key, separator, value in (line.partition("="),)
        if separator
    }


def test_local_environment_bootstrap_replaces_secrets_without_inventing_model_key() -> None:
    template = Path(".env.example").read_text(encoding="utf-8")

    first = _values(_render(template))
    second = _values(_render(template))

    assert first["USE_FAKE_EXTERNAL_CLIENTS"] == "false"
    assert first["KAFKA_ENABLED"] == first["OUTBOX_RELAY_ENABLED"] == "true"
    assert first["OBJECT_STORE_BACKEND"] == "s3"
    assert first["LLM_API_KEY"] == ""
    assert first["MYSQL_APP_PASSWORD"] in first["DATABASE_URL"]
    assert first["REDIS_PASSWORD"] in first["REDIS_URL"]
    assert first["OBJECT_STORE_ACCESS_KEY"] == first["S3_ACCESS_KEY_ID"]
    assert first["OBJECT_STORE_SECRET_KEY"] == first["S3_SECRET_ACCESS_KEY"]
    assert first["JWT_DEV_SECRET"] != second["JWT_DEV_SECRET"]
    assert all(value != "replace-me" for value in first.values())
