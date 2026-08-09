"""Turn 结果与副作用抽象(照 Reference `agent/turns/`)。

一次 turn 的产物不只是"回复什么",还包括"发送成功后要做什么"和"发送失败后要做什么"。
kirakira 此前把这套分支手写在三处:主动链路的 alert/content 投递(try/except
OutboundDeliveryError)、Drift 的 sent/silent 修正。手写的问题是每处语义都要重新推一遍,
也无法单独测试"副作用是否在正确的分支执行"。

`TurnResult` 把它变成显式数据:先声明副作用意图,再由 owner 统一提交。这也是后续
durable outbox 的落点——提交协议集中在一处,才谈得上跨崩溃可靠投递。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, List, Literal, Optional, Protocol, runtime_checkable

from bus.queue import MessageBus, OutboundDeliveryError
from bus.events import OutboundMessage

logger = logging.getLogger(__name__)


@dataclass
class TurnOutbound:
    session_key: str
    content: str
    media: List[str] = field(default_factory=list)


@dataclass
class TurnTrace:
    source: Literal["passive", "proactive", "drift"]
    model: Optional[str] = None
    tool_calls: List[dict] = field(default_factory=list)
    retrieval: Optional[dict] = None
    extra: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class TurnSideEffect(Protocol):
    async def run(self) -> None: ...


@dataclass
class CallableSideEffect:
    """把一个协程工厂包成 TurnSideEffect,便于就地声明意图。"""

    name: str
    action: Callable[[], Awaitable[None]]

    async def run(self) -> None:
        await self.action()


@dataclass
class TurnResult:
    decision: Literal["reply", "skip"]
    outbound: Optional[TurnOutbound] = None
    evidence: List[str] = field(default_factory=list)
    trace: Optional[TurnTrace] = None
    # 通用副作用:无论发送成功/失败都执行(常用于预发送状态落地)。
    side_effects: List[TurnSideEffect] = field(default_factory=list)
    # 成功副作用:仅在 outbound 成功发送后执行。
    success_side_effects: List[TurnSideEffect] = field(default_factory=list)
    # 失败副作用:仅在 outbound 发送失败后执行。
    failure_side_effects: List[TurnSideEffect] = field(default_factory=list)


@dataclass
class OutboundDispatch:
    channel: str
    chat_id: str
    content: str
    thinking: str = ""
    metadata: dict[str, object] = field(default_factory=dict)
    media: List[str] = field(default_factory=list)


class OutboundPort(Protocol):
    async def dispatch(self, outbound: OutboundDispatch) -> bool: ...


class BusOutboundPort:
    """等待渠道真正投递成功再返回;失败返回 False 而不是抛出。

    `wait` 决定语义:主动/Drift 需要"确认送达才提交",被动回复走即发即走。
    """

    def __init__(self, bus: MessageBus, *, wait: bool = True) -> None:
        self._bus = bus
        self._wait = wait

    async def dispatch(self, outbound: OutboundDispatch) -> bool:
        message = OutboundMessage(
            channel=outbound.channel,
            chat_id=outbound.chat_id,
            content=outbound.content,
            thinking=outbound.thinking,
            metadata=dict(outbound.metadata),
            media=list(outbound.media),
        )
        try:
            if self._wait:
                await self._bus.publish_outbound_and_wait(message)
            else:
                await self._bus.publish_outbound(message)
        except OutboundDeliveryError as error:
            logger.warning(
                "[turn] outbound delivery failed channel=%s chat=%s: %s",
                outbound.channel,
                outbound.chat_id,
                error,
            )
            return False
        return True


@dataclass
class TurnCommitOutcome:
    dispatched: bool
    ran: List[str] = field(default_factory=list)
    failures: List[str] = field(default_factory=list)


async def _run_side_effects(
    effects: List[TurnSideEffect],
    outcome: TurnCommitOutcome,
) -> None:
    for effect in effects:
        name = getattr(effect, "name", type(effect).__name__)
        try:
            await effect.run()
        except Exception as error:  # noqa: BLE001 - 单个副作用失败不阻断其余
            outcome.failures.append(name)
            logger.warning("[turn] side effect failed: %s: %s", name, error)
        else:
            outcome.ran.append(name)


async def commit_turn_result(
    result: TurnResult,
    *,
    port: OutboundPort,
    channel: str = "",
    chat_id: str = "",
    metadata: Optional[dict[str, object]] = None,
) -> TurnCommitOutcome:
    """按 decision 提交一次 turn 的产物与副作用。

    顺序是刻意的:通用副作用先跑(它们常用于"发送前先落地状态,避免重复"),
    然后投递,最后按投递结果跑成功/失败副作用。单个副作用失败只记录,不影响其余,
    也不改变投递结论——投递是否成功是渠道说了算,不是副作用说了算。
    """
    outcome = TurnCommitOutcome(dispatched=False)
    await _run_side_effects(result.side_effects, outcome)

    if result.decision == "skip":
        # skip 不是失败:没有尝试投递,不跑 failure 回滚(照 Reference turns/orchestrator.py)。
        return outcome
    if result.outbound is None:
        # decision=reply 却没有产物,是调用方缺陷;按投递失败处理,让回滚副作用清理预置状态。
        await _run_side_effects(result.failure_side_effects, outcome)
        return outcome

    outcome.dispatched = await port.dispatch(
        OutboundDispatch(
            channel=channel,
            chat_id=chat_id,
            content=result.outbound.content,
            media=list(result.outbound.media),
            metadata=dict(metadata or {}),
        )
    )
    await _run_side_effects(
        result.success_side_effects if outcome.dispatched else result.failure_side_effects,
        outcome,
    )
    return outcome
