from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from graphrag.api.app import create_app
from graphrag.config import AppEnvironment, Settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        app_env=AppEnvironment.TEST,
        use_fake_external_clients=True,
        upload_dir=tmp_path / "uploads",
        jwt_dev_secret=SecretStr("test-jwt-secret-material-is-long-enough"),
        fake_approval_secret=SecretStr("test-approval-secret-material-long-enough"),
        embedding_dimension=32,
        chunk_target_chars=100,
        chunk_max_chars=150,
        chat_rate_limit_per_minute=100,
        upload_rate_limit_per_minute=100,
    )


@pytest.fixture
def client(settings: Settings) -> Iterator[TestClient]:
    with TestClient(create_app(settings)) as test_client:
        yield test_client


def issue_token(
    client: TestClient, *, user_id: str = "demo-user", roles: list[str] | None = None
) -> str:
    response = client.post(
        "/api/v1/auth/dev-token",
        json={"user_id": user_id, "roles": roles or ["user"]},
    )
    assert response.status_code == 200
    return str(response.json()["access_token"])


@pytest.fixture
def user_headers(client: TestClient) -> dict[str, str]:
    return {"Authorization": f"Bearer {issue_token(client)}"}


@pytest.fixture
def admin_headers(client: TestClient) -> dict[str, str]:
    token = issue_token(client, roles=["user", "admin"])
    return {"Authorization": f"Bearer {token}"}
