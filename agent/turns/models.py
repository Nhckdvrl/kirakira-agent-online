"""Channel-independent contracts for one passive agent turn.

The local runtime historically used ``InboundMessage`` as both a channel envelope
and the agent-core request. That makes a Cloud API invent channel identities just
to call the agent. These contracts keep product conversation identity, caller
identity, origin, memory scope, and optional delivery target explicit.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal


PrincipalKind = Literal["user", "service", "system", "agent"]
OriginKind = Literal["channel", "api", "control", "scheduler", "subagent"]


@dataclass(frozen=True)
class AgentPrincipal:
    subject_id: str
    kind: PrincipalKind = "user"
    display_name: str | None = None

    def __post_init__(self) -> None:
        if not self.subject_id.strip():
            raise ValueError("principal subject_id must not be blank")


@dataclass(frozen=True)
class TurnOrigin:
    kind: OriginKind
    name: str
    external_thread_id: str

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.external_thread_id.strip():
            raise ValueError("turn origin name and external_thread_id must not be blank")


@dataclass(frozen=True)
class TurnMemoryScope:
    namespace: str
    subject_id: str


@dataclass(frozen=True)
class TurnDelivery:
    channel: str
    chat_id: str


@dataclass(frozen=True)
class TurnRequest:
    conversation_id: str
    content: str
    principal: AgentPrincipal
    origin: TurnOrigin
    memory_scope: TurnMemoryScope
    delivery: TurnDelivery | None = None
    submitted_at: datetime = field(default_factory=lambda: datetime.now().astimezone())
    media: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.conversation_id.strip():
            raise ValueError("conversation_id must not be blank")
        if self.submitted_at.tzinfo is None:
            raise ValueError("submitted_at must be timezone-aware")


@dataclass(frozen=True)
class TurnResult:
    conversation_id: str
    content: str
    thinking: str = ""
    media: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
