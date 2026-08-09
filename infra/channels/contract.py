from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Protocol

from agent.looping.interrupt import InterruptController
from agent.tools.message_push import MessagePushTool
from bus.event_bus import EventBus
from bus.queue import MessageBus
from core.net.http import SharedHttpResources
from infra.channels.base import AttachmentStore
from session.manager import SessionManager


class Channel(Protocol):
    name: str

    async def start(self, ctx: ChannelContext) -> None: ...
    async def stop(self) -> None: ...


@dataclass
class ChannelContext:
    bus: MessageBus
    session_manager: SessionManager
    event_bus: EventBus
    workspace: Path
    log: logging.Logger
    push_tool: MessagePushTool | None = None
    attachment_store: AttachmentStore | None = None
    http_resources: SharedHttpResources | None = None
    interrupt_controller: InterruptController | None = None
    mobile_bot_commands: list[tuple[str, str]] = field(default_factory=list)
    interrupt: Callable[[str], bool] | None = None
    memory: Any = None
