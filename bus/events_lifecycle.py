"""Lifecycle contracts for the passive reply pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from bus.events import InboundMessage, OutboundMessage


JsonDict = Dict[str, Any]


@dataclass
class TurnState:
    msg: InboundMessage
    session_key: str
    dispatch_outbound: bool = True
    session: Any = None
    extra_metadata: JsonDict = field(default_factory=dict)


@dataclass
class BeforeTurnCtx:
    session_key: str
    channel: str
    chat_id: str
    content: str
    timestamp: datetime
    retrieved_memory_block: str
    history_messages: Tuple[JsonDict, ...]
    skill_names: List[str] = field(default_factory=list)
    abort: bool = False
    abort_reply: str = ""
    extra_hints: List[str] = field(default_factory=list)
    extra_metadata: JsonDict = field(default_factory=dict)


@dataclass
class BeforeReasoningCtx:
    session_key: str
    channel: str
    chat_id: str
    content: str
    timestamp: datetime
    skill_names: List[str]
    retrieved_memory_block: str
    extra_hints: List[str] = field(default_factory=list)
    abort: bool = False
    abort_reply: str = ""


@dataclass
class PromptRenderCtx:
    session_key: str
    channel: str
    chat_id: str
    content: str
    media: Optional[List[str]]
    timestamp: datetime
    history: List[JsonDict]
    skill_names: Optional[List[str]]
    retrieved_memory_block: str
    extra_hints: List[str] = field(default_factory=list)
    system_sections_top: List[Any] = field(default_factory=list)
    system_sections_bottom: List[Any] = field(default_factory=list)
    disabled_sections: set[str] = field(default_factory=set)
    turn_injection_prompt: str = ""


@dataclass(frozen=True)
class ContextPrepared:
    session_key: str
    channel: str
    chat_id: str
    attempt: int
    plan_name: str
    history_messages: int
    disabled_sections: Tuple[str, ...]
    estimated_tokens: int
    estimate_quality: str
    input_budget: int
    context_frame_chars: int
    sections: Tuple[JsonDict, ...]


@dataclass(frozen=True)
class ContextBudgetUpdated:
    """Post-commit context baseline for the next turn."""

    session_key: str
    channel: str
    chat_id: str
    history_messages: int
    history_tokens_estimate: int
    estimate_quality: str
    selected_plan: str
    last_prompt_tokens_estimate: int
    input_budget: int
    model_usage: JsonDict


@dataclass
class BeforeStepCtx:
    session_key: str
    channel: str
    chat_id: str
    iteration: int
    input_tokens_estimate: int
    visible_tool_names: Tuple[str, ...]
    extra_hints: List[str] = field(default_factory=list)
    early_stop: bool = False
    early_stop_reply: str = ""


@dataclass(frozen=True)
class AfterStepCtx:
    session_key: str
    channel: str
    chat_id: str
    iteration: int
    context_tokens_estimate: int
    tools_called: Tuple[str, ...]
    partial_reply: str
    tools_used_so_far: Tuple[str, ...]
    tool_chain_partial: Tuple[JsonDict, ...]
    partial_thinking: str
    has_more: bool


@dataclass
class AfterReasoningCtx:
    session_key: str
    channel: str
    chat_id: str
    tools_used: Tuple[str, ...]
    thinking: str
    tool_chain: Tuple[JsonDict, ...]
    reply: str
    media: List[str] = field(default_factory=list)
    outbound_metadata: JsonDict = field(default_factory=dict)


@dataclass
class AfterReasoningResult:
    ctx: AfterReasoningCtx
    outbound: OutboundMessage


@dataclass
class AfterTurnCtx:
    session_key: str
    channel: str
    chat_id: str
    reply: str
    tools_used: Tuple[str, ...]
    thinking: str
    will_dispatch: bool
    extra_metadata: JsonDict = field(default_factory=dict)


@dataclass(frozen=True, init=False)
class TurnCommitted:
    """Frozen post-turn payload aligned with Reference."""

    session_key: str
    channel: str
    chat_id: str
    input_message: str
    persisted_user_message: str
    assistant_response: str
    tools_used: Tuple[str, ...]
    turn_id: str = ""
    assistant_message_id: str | None = None
    thinking: str | None = None
    raw_reply: str | None = None
    meme_tag: str | None = None
    meme_media_count: int | None = None
    tool_chain_raw: Tuple[JsonDict, ...] = ()
    tool_call_groups: Tuple[JsonDict, ...] = ()
    timestamp: datetime | None = None
    post_reply_budget: JsonDict = field(default_factory=dict)
    react_stats: JsonDict = field(default_factory=dict)
    extra: JsonDict = field(default_factory=dict)
    model_usage: JsonDict = field(default_factory=dict)

    def __init__(
        self,
        *,
        session_key: str,
        channel: str,
        chat_id: str,
        input_message: str = "",
        persisted_user_message: str = "",
        assistant_response: str = "",
        tools_used: Tuple[str, ...] = (),
        user_content: str = "",
        assistant_reply: str = "",
        turn_id: str = "",
        assistant_message_id: str | None = None,
        thinking: str | None = None,
        raw_reply: str | None = None,
        meme_tag: str | None = None,
        meme_media_count: int | None = None,
        tool_chain_raw: Tuple[JsonDict, ...] = (),
        tool_call_groups: Tuple[JsonDict, ...] = (),
        timestamp: datetime | None = None,
        post_reply_budget: JsonDict | None = None,
        react_stats: JsonDict | None = None,
        extra: JsonDict | None = None,
        model_usage: JsonDict | None = None,
    ) -> None:
        object.__setattr__(self, "session_key", session_key)
        object.__setattr__(self, "channel", channel)
        object.__setattr__(self, "chat_id", chat_id)
        object.__setattr__(self, "input_message", input_message or user_content)
        object.__setattr__(
            self, "persisted_user_message", persisted_user_message or input_message or user_content
        )
        object.__setattr__(self, "assistant_response", assistant_response or assistant_reply)
        object.__setattr__(self, "tools_used", tuple(tools_used))
        object.__setattr__(self, "turn_id", turn_id)
        object.__setattr__(self, "assistant_message_id", assistant_message_id)
        object.__setattr__(self, "thinking", thinking)
        object.__setattr__(self, "raw_reply", raw_reply)
        object.__setattr__(self, "meme_tag", meme_tag)
        object.__setattr__(self, "meme_media_count", meme_media_count)
        object.__setattr__(self, "tool_chain_raw", tuple(tool_chain_raw))
        object.__setattr__(self, "tool_call_groups", tuple(tool_call_groups))
        object.__setattr__(self, "timestamp", timestamp)
        object.__setattr__(self, "post_reply_budget", dict(post_reply_budget or {}))
        object.__setattr__(self, "react_stats", dict(react_stats or {}))
        object.__setattr__(self, "extra", dict(extra or {}))
        object.__setattr__(self, "model_usage", dict(model_usage or {}))

    @property
    def user_content(self) -> str:
        return self.input_message

    @property
    def assistant_reply(self) -> str:
        return self.assistant_response


@dataclass(frozen=True)
class TurnStarted:
    session_key: str
    channel: str
    chat_id: str
    content: str
    timestamp: datetime


@dataclass(frozen=True)
class ToolCallStarted:
    session_key: str
    channel: str
    chat_id: str
    call_id: str
    tool_name: str
    arguments: JsonDict
    # The model iteration which produced this call.  Kept optional at the end
    # so integrations constructing lifecycle events positionally remain valid.
    iteration: int = 0


@dataclass(frozen=True)
class ToolCallCompleted:
    session_key: str
    channel: str
    chat_id: str
    call_id: str
    tool_name: str
    arguments: JsonDict
    result: str
    status: str
    iteration: int = 0

    @property
    def final_arguments(self) -> JsonDict:
        return self.arguments


@dataclass(frozen=True)
class StreamDeltaReady:
    session_key: str
    channel: str
    chat_id: str
    iteration: int
    content_delta: str = ""
    reasoning_delta: str = ""

    @property
    def thinking_delta(self) -> str:
        return self.reasoning_delta


@dataclass(frozen=True)
class TurnFinished:
    """Authoritative terminal event for every started turn.

    Consumers which render streaming output should treat ``outbound`` as the
    final, authoritative representation of the assistant message rather than
    appending it to previously received :class:`StreamDeltaReady` fragments.
    ``outbound`` is absent for failed and interrupted turns.
    """

    session_key: str
    channel: str
    chat_id: str
    status: str
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    will_dispatch: bool
    outbound: Optional[OutboundMessage] = None
    error: str = ""
