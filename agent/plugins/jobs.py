"""插件后台作业(照 Reference `agent/plugins/jobs.py` 移植,MVP 深度)。

插件声明 `jobs()` 返回 PluginJobSpec;runtime 的 PluginJobHost 负责按 interval 定时
或按 EventBus 事件触发,并做去抖。插件自己不起 task,由 host 统一持有生命周期,
这样插件卸载/换代时作业能被干净取消。
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Protocol, Sequence

logger = logging.getLogger(__name__)


class PluginLlmService(Protocol):
    async def complete(
        self,
        prompt: str,
        system: str = "",
        model: str | None = None,
        max_tokens: int | None = None,
    ) -> str: ...


@dataclass(frozen=True)
class IntervalTrigger:
    seconds: int


@dataclass(frozen=True)
class EventTrigger:
    event_type: type[object]


PluginJobTrigger = IntervalTrigger | EventTrigger


@dataclass(frozen=True)
class PluginJobContext:
    plugin_id: str
    event: object | None
    reason: str
    triggered_at: datetime
    plugin_context: Any = None
    llm: PluginLlmService | None = None


PluginJobHandler = Callable[[PluginJobContext], Awaitable[None]]


@dataclass(frozen=True)
class PluginJobSpec:
    id: str
    triggers: Sequence[PluginJobTrigger]
    handler: PluginJobHandler
    debounce_seconds: int = 0


@dataclass
class _JobState:
    last_run_at: float = 0.0
    task: asyncio.Task[None] | None = None
    subscriptions: list[Any] = field(default_factory=list)


class PluginJobHost:
    """持有并驱动所有插件作业。runtime 拥有生命周期,插件只声明。"""

    def __init__(self, *, event_bus: Any = None, llm: PluginLlmService | None = None) -> None:
        self._event_bus = event_bus
        self._llm = llm
        self._states: dict[str, _JobState] = {}
        self._specs: dict[str, tuple[str, PluginJobSpec]] = {}
        self._started = False

    def register(self, plugin_id: str, spec: PluginJobSpec) -> str:
        key = f"{plugin_id}:{spec.id}"
        if key in self._specs:
            raise ValueError(f"重复的插件作业 id: {key}")
        self._specs[key] = (plugin_id, spec)
        self._states[key] = _JobState()
        if self._started:
            self._arm(key)
        return key

    @property
    def job_keys(self) -> tuple[str, ...]:
        return tuple(sorted(self._specs))

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        for key in list(self._specs):
            self._arm(key)

    def _arm(self, key: str) -> None:
        plugin_id, spec = self._specs[key]
        state = self._states[key]
        for trigger in spec.triggers:
            if isinstance(trigger, IntervalTrigger):
                state.task = asyncio.create_task(
                    self._run_interval(key, plugin_id, spec, trigger)
                )
            elif isinstance(trigger, EventTrigger) and self._event_bus is not None:
                # 事件订阅交给 host 持有,卸载时统一 close。
                subscription = self._event_bus.on(
                    trigger.event_type,
                    self._make_event_handler(key, plugin_id, spec),
                )
                state.subscriptions.append(subscription)

    def _make_event_handler(
        self, key: str, plugin_id: str, spec: PluginJobSpec
    ) -> Callable[[object], Awaitable[None]]:
        async def handle(event: object) -> None:
            await self._fire(key, plugin_id, spec, event=event, reason="event")

        return handle

    async def _run_interval(
        self,
        key: str,
        plugin_id: str,
        spec: PluginJobSpec,
        trigger: IntervalTrigger,
    ) -> None:
        delay = max(1, int(trigger.seconds))
        try:
            while True:
                await asyncio.sleep(delay)
                await self._fire(key, plugin_id, spec, event=None, reason="interval")
        except asyncio.CancelledError:
            raise

    async def _fire(
        self,
        key: str,
        plugin_id: str,
        spec: PluginJobSpec,
        *,
        event: object | None,
        reason: str,
    ) -> None:
        state = self._states[key]
        now = asyncio.get_running_loop().time()
        if spec.debounce_seconds > 0 and state.last_run_at:
            if now - state.last_run_at < spec.debounce_seconds:
                return
        state.last_run_at = now
        context = PluginJobContext(
            plugin_id=plugin_id,
            event=event,
            reason=reason,
            triggered_at=datetime.now().astimezone(),
            llm=self._llm,
        )
        try:
            await spec.handler(context)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # 单个作业失败不影响其余作业和主链路
            logger.warning("[plugin.job] %s 执行失败: %s", key, exc)

    async def aclose(self) -> None:
        self._started = False
        tasks: list[asyncio.Task[None]] = []
        for state in self._states.values():
            for subscription in state.subscriptions:
                try:
                    subscription.close()
                except Exception:  # noqa: BLE001 - 关停期失败只记录
                    logger.debug("[plugin.job] 订阅关闭失败", exc_info=True)
            state.subscriptions.clear()
            if state.task is not None and not state.task.done():
                state.task.cancel()
                tasks.append(state.task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for state in self._states.values():
            state.task = None
