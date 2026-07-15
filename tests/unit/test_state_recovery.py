from __future__ import annotations

import json
import time

import pytest

from graphrag.domain.errors import ValidationError
from graphrag.domain.ids import new_id
from graphrag.domain.state import AgentState
from graphrag.domain.state_migrations import StateMigrationRegistry
from graphrag.infrastructure.memory import InMemoryCheckpointStore


def legacy_v1_payload() -> dict[str, object]:
    return {
        "state_version": 1,
        "request_id": new_id(),
        "run_id": new_id(),
        "session_id": new_id(),
        "tenant_id": "default",
        "user_id": "user-1",
        "roles": ["user"],
        "original_query": "policy",
        "query": "policy",
    }


def test_state_registry_migrates_v1_fixture_to_current_version() -> None:
    migrated = StateMigrationRegistry().load(legacy_v1_payload())
    assert isinstance(migrated, AgentState)
    assert migrated.state_version == StateMigrationRegistry.CURRENT_VERSION
    assert migrated.conversation_state is not None
    assert migrated.conversation_state.tenant_id == migrated.tenant_id
    assert migrated.approval_status is None


def test_state_registry_fails_closed_for_unknown_or_malformed_versions() -> None:
    future = legacy_v1_payload() | {"state_version": 99}
    with pytest.raises(ValidationError, match="不支持"):
        StateMigrationRegistry().load(future)
    malformed = legacy_v1_payload()
    malformed.pop("tenant_id")
    with pytest.raises(ValidationError, match="迁移失败"):
        StateMigrationRegistry().load(malformed)


@pytest.mark.asyncio
async def test_checkpoint_adapter_migrates_legacy_state_and_rejects_future_state() -> None:
    store = InMemoryCheckpointStore()
    legacy = legacy_v1_payload()
    key = (str(legacy["tenant_id"]), str(legacy["session_id"]))
    store.states[key] = (time.monotonic() + 60, json.dumps(legacy))
    loaded = await store.get(*key)
    assert loaded is not None and loaded.state_version == 2

    store.states[key] = (
        time.monotonic() + 60,
        json.dumps(legacy | {"state_version": 99}),
    )
    with pytest.raises(ValidationError, match="不支持"):
        await store.get(*key)
