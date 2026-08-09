"""主动链路的相位模块(照 Reference `proactive_v2/lifecycle.py` + `plugins/*/modules.py`)。

原来 `ProactiveLoop._tick()` 是一条扁平顺序链,步骤之间靠代码行序耦合,插件想在中间插一步
只能改 runtime。现在每一步是一个声明 `slot` / `requires` / `produces` 的模块,顺序由
`phase.topo_sort_modules` 的依赖图决定,插件可以声明依赖后插进来。

模块只做一件事并把产物写进 `frame.slots`;提前结束用 `frame.finish(reason)` 表达
(见 frame.py 里为什么不能再用 return)。每个模块开头都检查 `frame.done`,
这样"上一步已经结束本轮"对所有模块都是同一条规则,而不是各写各的判空。

这些模块都持有 `loop`(ProactiveLoop)。这不是理想的解耦——理想形态是各模块只依赖自己
需要的服务包——但先把顺序与依赖显式化,服务拆分留到主动链路也做 DI 时一起做。
"""

from __future__ import annotations

import logging
from typing import Any, List

from bus.queue import OutboundDeliveryError
from proactive_v2.contracts import normalize_context, rank_alerts
from proactive_v2.frame import (
    SLOT_CONTEXT_TEXT,
    SLOT_FETCH_CHANNELS,
    SLOT_GATE_PASSED,
    SLOT_JUDGE_CONTEXT,
    SLOT_NEW_CONTENT,
    SLOT_PROPOSAL_ALERT,
    SLOT_PROPOSAL_CONTENT,
    SLOT_PROPOSAL_DRIFT,
    ProactiveFrame,
)
from plugins.proactive_flow.judge import format_context

logger = logging.getLogger(__name__)


class _LoopModule:
    """所有主动模块的共同部分:持有 loop,并统一处理"本轮已结束"。"""

    slot: str = ""
    requires: tuple[str, ...] = ()
    produces: tuple[str, ...] = ()

    def __init__(self, loop: Any) -> None:
        self._loop = loop

    async def run(self, frame: ProactiveFrame) -> ProactiveFrame:
        if frame.done:
            return frame
        return await self.execute(frame)

    async def execute(self, frame: ProactiveFrame) -> ProactiveFrame:  # pragma: no cover
        raise NotImplementedError


class GateModule(_LoopModule):
    """目标就绪 + 被动链路空闲。每轮先重试已落库的 source ACK,与是否被动忙无关。"""

    slot = "proactive.gate"
    produces = (SLOT_GATE_PASSED,)

    async def execute(self, frame: ProactiveFrame) -> ProactiveFrame:
        loop = self._loop
        if not loop._cfg.target_ready:
            frame.slots[SLOT_GATE_PASSED] = False
            return frame.finish("target_not_ready")
        await loop._flush_pending_acknowledgements()
        await loop._flush_pending_feedback()
        if loop._passive_busy_fn is not None and loop._passive_busy_fn(frame.session_key):
            logger.info("[proactive] 被动链路忙，跳过本轮")
            loop._state.record_decision(frame.now, "gated", "被动链路忙")
            frame.slots[SLOT_GATE_PASSED] = False
            return frame.finish("passive_busy")
        frame.slots[SLOT_GATE_PASSED] = True
        return frame


class FetchModule(_LoopModule):
    """并发拉取所有 source。单源失败由 SourceRegistry 吸收,不阻断本轮。"""

    slot = "proactive.fetch"
    requires = ("proactive.gate",)
    produces = (SLOT_FETCH_CHANNELS,)

    async def execute(self, frame: ProactiveFrame) -> ProactiveFrame:
        frame.slots[SLOT_FETCH_CHANNELS] = await self._loop._sources.fetch_all()
        return frame


class IngestModule(_LoopModule):
    """三通道去重入库;context 不入库,只作为本轮判断背景。"""

    slot = "proactive.ingest"
    requires = ("proactive.fetch",)
    produces = (SLOT_NEW_CONTENT, SLOT_CONTEXT_TEXT)

    async def execute(self, frame: ProactiveFrame) -> ProactiveFrame:
        loop = self._loop
        now = frame.now
        channels = frame.get(SLOT_FETCH_CHANNELS) or {"alert": [], "content": [], "context": []}
        loop._state.ingest("alert", channels["alert"], now)
        new_content = set(loop._state.ingest("content", channels["content"], now))
        loop._state.queue_acknowledgements(
            loop._group_acknowledgements(channels["content"]), now
        )
        await loop._flush_pending_acknowledgements()
        # 淘汰陈旧未读 content，防止从不被引用的候选无界堆积
        loop._state.expire_old("content", now, loop._cfg.content_max_age_days)
        frame.slots[SLOT_NEW_CONTENT] = new_content
        frame.slots[SLOT_CONTEXT_TEXT] = format_context(
            [normalize_context(item) for item in channels["context"]]
        )
        return frame


class JudgeContextModule(_LoopModule):
    """装配判断所需的上下文:长期记忆、近期对话、近期主动消息、规则面板。"""

    slot = "proactive.judge_context"
    requires = ("proactive.ingest",)
    produces = (SLOT_JUDGE_CONTEXT,)

    async def execute(self, frame: ProactiveFrame) -> ProactiveFrame:
        loop = self._loop
        frame.slots[SLOT_JUDGE_CONTEXT] = {
            "memory_text": loop._read_memory(),
            "recent_conversation": loop._recent_conversation(frame.session_key),
            "recent_proactive": loop._recent_proactive(frame.session_key),
            "proactive_context": loop._read_context_file(),
        }
        return frame


class AlertModule(_LoopModule):
    """alert 按严重度优先直推;还有 alert 时尽快再来一轮排空。"""

    slot = "proactive.alert"
    requires = ("proactive.judge_context",)
    produces = (SLOT_PROPOSAL_ALERT,)

    async def execute(self, frame: ProactiveFrame) -> ProactiveFrame:
        loop = self._loop
        now = frame.now
        alerts = rank_alerts(loop._state.unread("alert"))
        if not alerts:
            frame.slots[SLOT_PROPOSAL_ALERT] = None
            return frame
        ctx = frame.get(SLOT_JUDGE_CONTEXT) or {}
        try:
            await loop._push_alert(
                alerts[0],
                now,
                ctx.get("memory_text", ""),
                ctx.get("recent_conversation", ""),
                ctx.get("proactive_context", ""),
                frame.get(SLOT_CONTEXT_TEXT, ""),
                ctx.get("recent_proactive", ""),
            )
        except OutboundDeliveryError as exc:
            logger.error("[proactive] alert 渠道发送失败，保留未读: %s", exc)
            loop._state.record_decision(now, "delivery_failed", "alert: %s" % str(exc))
            frame.slots[SLOT_PROPOSAL_ALERT] = "delivery_failed"
            return frame.finish("alert_delivery_failed")
        loop._state.record_decision(
            now, "alert_pushed", str(alerts[0].get("title") or "")[:120]
        )
        if len(alerts) > 1:
            loop._wake.set()
        frame.slots[SLOT_PROPOSAL_ALERT] = "pushed"
        return frame.finish("alert_pushed")


class ContentModule(_LoopModule):
    """只有出现新内容且不在冷却期时才做兴趣判断。"""

    slot = "proactive.content"
    requires = ("proactive.alert",)
    produces = (SLOT_PROPOSAL_CONTENT,)

    async def execute(self, frame: ProactiveFrame) -> ProactiveFrame:
        loop = self._loop
        now = frame.now
        contents = loop._state.unread("content")
        has_new = bool(contents and frame.get(SLOT_NEW_CONTENT))
        if not has_new or loop._state.in_cooldown(
            frame.session_key, now, loop._cfg.delivery_cooldown_hours
        ):
            frame.slots[SLOT_PROPOSAL_CONTENT] = None
            return frame

        ctx = frame.get(SLOT_JUDGE_CONTEXT) or {}
        memory_text = ctx.get("memory_text", "")
        # 兴趣检索用候选标题做 query,把"这个人是否真的关心这类东西"的证据带进判断。
        interest = await loop._interest_hits(
            " ".join(str(item.get("title") or "")[:120] for item in contents[:5]).strip()
        )
        if interest:
            memory_text = "%s\n\n【相关长期记忆】\n%s" % (memory_text, interest)
        try:
            pushed = await loop._push_content(
                contents,
                now,
                memory_text,
                ctx.get("recent_conversation", ""),
                ctx.get("proactive_context", ""),
                frame.get(SLOT_CONTEXT_TEXT, ""),
                ctx.get("recent_proactive", ""),
            )
        except OutboundDeliveryError as exc:
            logger.error("[proactive] content 渠道发送失败，保留未读: %s", exc)
            loop._state.record_decision(now, "delivery_failed", "content: %s" % str(exc))
            frame.slots[SLOT_PROPOSAL_CONTENT] = "delivery_failed"
            return frame.finish("content_delivery_failed")
        loop._state.record_decision(
            now,
            "content_pushed" if pushed else "content_skipped",
            "候选 %d 条" % len(contents),
        )
        frame.slots[SLOT_PROPOSAL_CONTENT] = "pushed" if pushed else "skipped"
        return frame.finish("content_pushed") if pushed else frame


class DriftModule(_LoopModule):
    """三路都没推 → 交给 Drift 用空闲时间干活。

    对照 Reference 的 `DriftFlowModule`:Drift 不再是 runtime 里的一个 hook 调用,
    而是流水线上声明了依赖的一个模块。
    """

    slot = "proactive.drift"
    requires = ("proactive.content",)
    produces = (SLOT_PROPOSAL_DRIFT,)

    async def execute(self, frame: ProactiveFrame) -> ProactiveFrame:
        loop = self._loop
        drifted = False
        if loop._drift_hook is not None:
            drifted = await loop._drift_hook(frame.now, frame.session_key)
        frame.slots[SLOT_PROPOSAL_DRIFT] = "drifted" if drifted else None
        loop._state.record_decision(frame.now, "drift" if drifted else "idle", "")
        return frame.finish("drift" if drifted else "idle")


def build_default_proactive_modules(loop: Any) -> List[object]:
    """默认模块集。顺序由各自的 requires 决定,这里的排列只是可读性。"""
    return [
        GateModule(loop),
        FetchModule(loop),
        IngestModule(loop),
        JudgeContextModule(loop),
        AlertModule(loop),
        ContentModule(loop),
        DriftModule(loop),
    ]
