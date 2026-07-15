from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import SecretStr
from pydantic import ValidationError as PydanticValidationError

from graphrag.config import AppEnvironment, Settings
from graphrag.domain.events import EventEnvelope
from graphrag.domain.ids import new_id, validate_id
from graphrag.domain.models import AgentIntent, IdentityContext, utc_now
from graphrag.domain.state import AgentState


def test_uuid7_round_trip_and_invalid_version() -> None:
    value = new_id()
    assert validate_id(value) == value
    with pytest.raises(ValueError):
        validate_id("00000000-0000-4000-8000-000000000000")


def test_settings_reject_dangerous_production_and_bad_thresholds() -> None:
    with pytest.raises(PydanticValidationError):
        Settings(app_env=AppEnvironment.PROD, use_fake_external_clients=True)
    with pytest.raises(PydanticValidationError):
        Settings(chunk_target_chars=700, chunk_max_chars=600)
    with pytest.raises(PydanticValidationError):
        Settings(jwt_dev_secret=SecretStr("short"))
    with pytest.raises(PydanticValidationError, match="automatic long-term memory"):
        Settings(long_term_memory_auto_write_enabled=True)


def test_production_requires_asymmetric_auth_and_real_credentials() -> None:
    with pytest.raises(PydanticValidationError):
        Settings(
            app_env=AppEnvironment.PROD,
            use_fake_external_clients=False,
            database_url="mysql+aiomysql://app@db/app",
            jwt_algorithm="HS256",
        )


def test_event_round_trip_and_strict_contract() -> None:
    event = EventEnvelope(
        event_type="document.received",
        tenant_id="default",
        aggregate_id=new_id(),
        trace_id=new_id(),
    )
    assert EventEnvelope.model_validate_json(event.model_dump_json()) == event
    with pytest.raises(PydanticValidationError):
        EventEnvelope(
            event_type="unknown",  # type: ignore[arg-type]
            tenant_id="default",
            aggregate_id=new_id(),
            trace_id=new_id(),
        )


def test_agent_state_protects_owned_fields_and_serializes() -> None:
    state = AgentState(
        request_id=new_id(),
        run_id=new_id(),
        session_id=new_id(),
        tenant_id="default",
        user_id="user-1",
        roles=frozenset({"user"}),
        original_query="政策是什么",
        query="政策是什么",
    )
    updated = state.update_for_agent(AgentIntent.KB, final_answer="答案", confidence=0.8)
    assert updated.iteration == 1
    assert updated.visited_agents == (AgentIntent.KB,)
    assert AgentState.model_validate_json(updated.model_dump_json()) == updated
    with pytest.raises(ValueError):
        state.update_for_agent(AgentIntent.KB, tenant_id="other")


def test_identity_and_time_ranges_are_strict() -> None:
    identity = IdentityContext(tenant_id="default", user_id="u1", roles=frozenset({"user"}))
    assert identity.has_role("user")
    now = utc_now()
    assert now.tzinfo is not None
    assert now + timedelta(seconds=1) > now
