"""Durable user-scoped implementation of the original message_push port."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from uuid import UUID

from agent.tools.registry import ToolRegistry
from bus.events import ChannelMessage, DeliveryReceipt, DeliveryStatus
from cloud.store import CloudStore


_current_user: ContextVar[str] = ContextVar("cloud_push_user", default="")


class CloudMessagePushTool:
    def __init__(self, store: CloudStore) -> None:
        self.store = store
        self.registry: ToolRegistry | None = None

    def attach_registry(self, registry: ToolRegistry) -> None:
        self.registry = registry

    @contextmanager
    def bind_user(self, user_id: str):
        token = _current_user.set(user_id)
        try:
            yield
        finally:
            _current_user.reset(token)

    async def dispatch(
        self, message: ChannelMessage, *, commit_role: str = ""
    ) -> DeliveryReceipt:
        del commit_role
        context = self.registry.context if self.registry is not None else {}
        user_id = _current_user.get() or str(context.get("principal_id") or "")
        conversation_id = str(context.get("chat_id") or "")
        if not user_id or not conversation_id:
            return DeliveryReceipt(DeliveryStatus.FAILED, detail="Cloud identity is missing")
        try:
            await self.store.enqueue_channel_push(
                UUID(user_id),
                UUID(conversation_id),
                provider=message.channel,
                external_chat_id=message.chat_id,
                content=message.content,
                media=[item.source for item in message.attachments],
            )
        except Exception as exc:
            return DeliveryReceipt(DeliveryStatus.FAILED, detail=str(exc))
        return DeliveryReceipt(
            DeliveryStatus.SUCCESS,
            canonical_media=tuple(item.source for item in message.attachments),
            detail="accepted by durable channel outbox",
        )
