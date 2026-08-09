"""Canonical channel implementations and contracts."""

from infra.channels.contract import Channel, ChannelContext
from infra.channels.host import ChannelHost
from infra.channels.qq_channel import QQChannel
from infra.channels.qqbot_channel import QQBotChannel
from infra.channels.telegram_channel import TelegramChannel
from infra.channels.web_chat_channel import WebChannel

__all__ = [
    "Channel",
    "ChannelContext",
    "ChannelHost",
    "QQBotChannel",
    "QQChannel",
    "TelegramChannel",
    "WebChannel",
]
