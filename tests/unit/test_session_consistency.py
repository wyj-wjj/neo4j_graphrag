from __future__ import annotations

import asyncio

import pytest

from graphrag.application.sessions import InMemorySessionService, TurnClaim, TurnDisposition
from graphrag.domain.errors import ConflictError
from graphrag.domain.models import (
    AgentIntent,
    AnswerStatus,
    ChatResult,
    IdentityContext,
    SourceKind,
)


def identity(user_id: str = "owner") -> IdentityContext:
    return IdentityContext(
        tenant_id="default",
        user_id=user_id,
        roles=frozenset({"user"}),
    )


def result_for(claim: TurnClaim, session_id: str, answer: str = "answer") -> ChatResult:
    return ChatResult(
        request_id=claim.request_id,
        run_id=claim.run_id,
        client_turn_id=claim.client_turn_id,
        session_id=session_id,
        status=AnswerStatus.ANSWERED,
        answer=answer,
        intent=AgentIntent.FAQ,
        source=SourceKind.REAL,
    )


@pytest.mark.asyncio
async def test_duplicate_completed_turn_replays_without_duplicate_messages() -> None:
    service = InMemorySessionService()
    owner = identity()
    session = await service.create(owner)

    first = await service.begin_turn(
        owner,
        session.session_id,
        client_turn_id="client-turn-0001",
        request_id="request-0001",
        query="question",
    )
    assert first.disposition is TurnDisposition.STARTED
    completed = result_for(first, session.session_id)
    await service.complete_turn(owner, first, completed)

    replay = await service.begin_turn(
        owner,
        session.session_id,
        client_turn_id="client-turn-0001",
        request_id="request-0002",
        query="question",
    )
    stored = await service.get(owner, session.session_id)
    assert replay.disposition is TurnDisposition.REPLAY
    assert replay.result == completed
    assert [message.sequence for message in stored.messages] == [1, 2]
    assert stored.revision == 2
    assert stored.active_run_id is None


@pytest.mark.asyncio
async def test_client_turn_id_cannot_be_reused_for_different_input() -> None:
    service = InMemorySessionService()
    owner = identity()
    session = await service.create(owner)
    await service.begin_turn(
        owner,
        session.session_id,
        client_turn_id="client-turn-0001",
        request_id="request-0001",
        query="first input",
    )

    with pytest.raises(ConflictError, match="client_turn_id"):
        await service.begin_turn(
            owner,
            session.session_id,
            client_turn_id="client-turn-0001",
            request_id="request-0002",
            query="changed input",
        )


@pytest.mark.asyncio
async def test_one_active_run_per_session_but_other_sessions_are_independent() -> None:
    service = InMemorySessionService()
    owner = identity()
    first_session = await service.create(owner)
    second_session = await service.create(owner)
    first = await service.begin_turn(
        owner,
        first_session.session_id,
        client_turn_id="client-turn-0001",
        request_id="request-0001",
        query="first",
    )

    duplicate = await service.begin_turn(
        owner,
        first_session.session_id,
        client_turn_id="client-turn-0001",
        request_id="request-0002",
        query="first",
    )
    assert duplicate.disposition is TurnDisposition.IN_PROGRESS

    with pytest.raises(ConflictError, match="正在处理"):
        await service.begin_turn(
            owner,
            first_session.session_id,
            client_turn_id="client-turn-0002",
            request_id="request-0003",
            query="second",
        )

    other = await service.begin_turn(
        owner,
        second_session.session_id,
        client_turn_id="client-turn-0003",
        request_id="request-0004",
        query="parallel",
    )
    assert first.disposition is TurnDisposition.STARTED
    assert other.disposition is TurnDisposition.STARTED


@pytest.mark.asyncio
async def test_cancelled_turn_can_retry_without_adding_duplicate_user_message() -> None:
    service = InMemorySessionService()
    owner = identity()
    session = await service.create(owner)
    first = await service.begin_turn(
        owner,
        session.session_id,
        client_turn_id="client-turn-0001",
        request_id="request-0001",
        query="question",
    )
    await service.abort_turn(owner, first, error_code="cancelled")

    retry = await service.begin_turn(
        owner,
        session.session_id,
        client_turn_id="client-turn-0001",
        request_id="request-0002",
        query="question",
    )
    assert retry.disposition is TurnDisposition.STARTED
    assert retry.run_id == first.run_id
    assert len((await service.get(owner, session.session_id)).messages) == 1


@pytest.mark.asyncio
async def test_concurrent_begin_has_exactly_one_started_turn() -> None:
    service = InMemorySessionService()
    owner = identity()
    session = await service.create(owner)

    async def begin(index: int) -> TurnDisposition | str:
        try:
            claim = await service.begin_turn(
                owner,
                session.session_id,
                client_turn_id=f"client-turn-{index:04d}",
                request_id=f"request-{index:04d}",
                query=f"question {index}",
            )
            return claim.disposition
        except ConflictError:
            return "conflict"

    outcomes = await asyncio.gather(*(begin(index) for index in range(8)))
    assert outcomes.count(TurnDisposition.STARTED) == 1
    assert outcomes.count("conflict") == 7
