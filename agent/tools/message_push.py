from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from bus.events import ChannelMessage, DeliveryReceipt, DeliveryStatus

logger = logging.getLogger(__name__)


class ChannelRegistration:
    def __init__(self, tool: "MessagePushTool", channel: str, token: object) -> None:
        self._tool = tool
        self._channel = channel
        self._token = token
        self._active = True

    def close(self) -> None:
        if not self._active:
            return
        self._active = False
        self._tool.unregister_channel(self._channel, self._token)


class MessagePushTool:
    """完整逻辑消息的渠道 adapter registry。"""

    def __init__(self, chat_lane=None) -> None:
        self._adapters: dict[
            str, Callable[[ChannelMessage], Awaitable[DeliveryReceipt]]
        ] = {}
        self._registration_tokens: dict[str, object] = {}
        self._chat_lane = chat_lane

    @property
    def channels(self) -> dict[str, Callable[[ChannelMessage], Awaitable[DeliveryReceipt]]]:
        return dict(self._adapters)

    def register_channel(
        self,
        channel: str,
        deliver: Callable[[ChannelMessage], Awaitable[DeliveryReceipt]],
    ) -> ChannelRegistration:
        if channel in self._adapters:
            raise RuntimeError(f"message_push 渠道名称重复: {channel}")
        self._adapters[channel] = deliver
        token = object()
        self._registration_tokens[channel] = token
        logger.debug("message_push: 注册渠道 %r", channel)
        return ChannelRegistration(self, channel, token)

    def unregister_channel(self, channel: str, token: object) -> None:
        if self._registration_tokens.get(channel) is not token:
            return
        self._registration_tokens.pop(channel, None)
        self._adapters.pop(channel, None)

    async def dispatch(
        self,
        message: ChannelMessage,
        *,
        commit_role: str = "",
    ) -> DeliveryReceipt:
        adapter = self._adapters.get(message.channel)
        if adapter is None:
            return DeliveryReceipt(
                DeliveryStatus.FAILED,
                detail=(
                    f"渠道 {message.channel!r} 未注册，可用渠道："
                    f"{list(self._adapters) or ['（无）']}"
                ),
            )

        async def deliver() -> DeliveryReceipt:
            receipt = await adapter(message)
            if not isinstance(receipt, DeliveryReceipt):
                raise TypeError("message_push channel adapter 必须返回 DeliveryReceipt")
            return receipt

        if self._chat_lane is not None and commit_role != "passive":
            return await self._chat_lane.run_non_passive(
                message.channel,
                message.chat_id,
                deliver,
            )
        return await deliver()
