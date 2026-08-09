"""Typed message contracts for channels, the bus, and the agent loop."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List


JsonDict = Dict[str, Any]


class DeliveryStatus(str, Enum):
    """一次完整逻辑消息的渠道提交终态。"""

    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


class AttachmentKind(str, Enum):
    FILE = "file"
    IMAGE = "image"


@dataclass(frozen=True)
class ChannelAttachment:
    """渠道边界中带明确类型的单个附件。"""

    kind: AttachmentKind
    source: str
    filename: str | None = None


@dataclass(frozen=True)
class ChannelMessage:
    """提交给渠道 adapter 的完整逻辑消息。"""

    channel: str
    chat_id: str
    content: str
    attachments: tuple[ChannelAttachment, ...] = ()
    thinking: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)
    session_message_id: str | None = None
    control_turn_id: str | None = None


@dataclass(frozen=True)
class DeliveryReceipt:
    """记录渠道对完整逻辑消息的结构化提交结果。"""

    status: DeliveryStatus
    canonical_media: tuple[str, ...] = ()
    detail: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.status is DeliveryStatus.SUCCESS


@dataclass
class InboundMessage:
    channel: str
    sender: str
    chat_id: str
    content: str
    timestamp: datetime = field(default_factory=lambda: datetime.now().astimezone())
    media: List[str] = field(default_factory=list)
    metadata: JsonDict = field(default_factory=dict)

    @property
    def session_key(self) -> str:
        override = str(self.metadata.get("session_key_override") or "").strip()
        if override:
            return override
        return "%s:%s" % (self.channel, self.chat_id)

    @property
    def context_channel(self) -> str:
        return str(self.metadata.get("context_channel") or self.channel).strip()

    @property
    def context_chat_id(self) -> str:
        return str(self.metadata.get("context_chat_id") or self.chat_id).strip()


@dataclass
class OutboundMessage:
    channel: str
    chat_id: str
    content: str
    thinking: str = ""
    reply_to: str = ""
    media: List[str] = field(default_factory=list)
    metadata: JsonDict = field(default_factory=dict)
    session_message_id: str | None = None
    control_turn_id: str | None = None


def channel_message_from_outbound(
    message: OutboundMessage,
    *,
    media_kind: AttachmentKind = AttachmentKind.IMAGE,
) -> ChannelMessage:
    """把 Turn 的字符串媒体投影转换为渠道边界类型。"""

    return ChannelMessage(
        channel=message.channel,
        chat_id=message.chat_id,
        content=message.content,
        attachments=tuple(
            ChannelAttachment(media_kind, source) for source in message.media
        ),
        thinking=message.thinking or None,
        metadata=dict(message.metadata),
        session_message_id=message.session_message_id,
        control_turn_id=message.control_turn_id,
    )
