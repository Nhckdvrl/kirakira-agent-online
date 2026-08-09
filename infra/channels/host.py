"""Channel host for starting and stopping configured channels."""

from __future__ import annotations

import logging
from collections.abc import Callable

from infra.channels.contract import Channel, ChannelContext

logger = logging.getLogger(__name__)


class ChannelHost:
    def __init__(self, ctx_factory: Callable[[Channel], ChannelContext]) -> None:
        self._ctx_factory = ctx_factory
        self._channels: list[Channel] = []
        self._started: list[Channel] = []

    def add(self, channel: Channel) -> None:
        self._channels.append(channel)

    async def start_all(self) -> None:
        for channel in self._channels:
            try:
                await channel.start(self._ctx_factory(channel))
                self._started.append(channel)
                print("渠道已启动: %s" % channel.name)
            except Exception as exc:
                logger.exception("渠道启动失败 %s: %s", channel.name, exc)
                try:
                    await channel.stop()
                except Exception:
                    logger.exception("渠道启动失败后的清理也失败 %s", channel.name)
                # Reference 只有完整 AppRuntime.start 成功后才发布 readiness。渠道配置
                # 错误属于启动失败，不能吞掉后让 supervisor/用户误以为服务可用。
                await self.stop_all()
                raise RuntimeError("channel %s failed to start: %s" % (channel.name, exc)) from exc

    async def stop_all(self) -> None:
        for channel in reversed(self._started):
            try:
                await channel.stop()
            except Exception as exc:
                logger.warning("渠道停止失败 %s: %s", channel.name, exc)
        self._started.clear()

    @property
    def channels(self) -> list[Channel]:
        return list(self._channels)
