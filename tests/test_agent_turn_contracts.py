"""Product/API-facing agent turn contracts."""

from datetime import UTC, datetime

import pytest

from agent.turns.models import (
    AgentPrincipal,
    TurnDelivery,
    TurnMemoryScope,
    TurnOrigin,
    TurnRequest,
)
from session.manager import SessionManager
from session.ports import TranscriptStore


def test_cloud_turn_identity_is_independent_from_transport() -> None:
    request = TurnRequest(
        conversation_id="conversation-42",
        content="hello",
        principal=AgentPrincipal("user-7"),
        origin=TurnOrigin("api", "cloud-api", "request-99"),
        memory_scope=TurnMemoryScope("user", "user-7"),
    )

    assert request.delivery is None
    assert request.conversation_id != request.origin.external_thread_id
    assert request.memory_scope.subject_id == request.principal.subject_id


def test_local_turn_can_declare_a_delivery_target() -> None:
    request = TurnRequest(
        conversation_id="telegram:123",
        content="hello",
        principal=AgentPrincipal("123"),
        origin=TurnOrigin("channel", "telegram", "123"),
        memory_scope=TurnMemoryScope("telegram", "123"),
        delivery=TurnDelivery("telegram", "123"),
        submitted_at=datetime.now(UTC),
    )

    assert request.delivery == TurnDelivery("telegram", "123")


def test_turn_rejects_blank_identity_and_naive_time() -> None:
    with pytest.raises(ValueError, match="conversation_id"):
        TurnRequest(
            conversation_id=" ",
            content="hello",
            principal=AgentPrincipal("user-7"),
            origin=TurnOrigin("api", "cloud-api", "request-99"),
            memory_scope=TurnMemoryScope("user", "user-7"),
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        TurnRequest(
            conversation_id="conversation-42",
            content="hello",
            principal=AgentPrincipal("user-7"),
            origin=TurnOrigin("api", "cloud-api", "request-99"),
            memory_scope=TurnMemoryScope("user", "user-7"),
            submitted_at=datetime.now(),
        )


def test_local_session_manager_implements_transcript_store(tmp_path) -> None:
    manager = SessionManager(tmp_path)
    try:
        assert isinstance(manager, TranscriptStore)
    finally:
        manager.close()
