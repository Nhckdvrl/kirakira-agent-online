"""主动推送主循环：Gate → Fetch → Ingest → Decide → Deliver →（空则）Drift。

参考 akashic 的 `proactive_v2/loop.py` + `plugins/wake_proactive/runtime.py`，
MVP 把重型的 phase-graph kernel / snapshot 压平成一个直白的 async tick 循环，
保留两条差异化本质：电量自适应调度 + 三通道语义 + 空闲交给 Drift。
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import time
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional
from uuid import uuid4

from bus.queue import MessageBus, OutboundDeliveryError
from core.memory.engine import (
    MemoryCapability,
    MemoryQuery,
    MemoryQueryFilters,
)
from bus.events import OutboundMessage
from agent.turns.result import (
    BusOutboundPort,
    CallableSideEffect,
    TurnOutbound,
    TurnResult,
    TurnTrace,
    OutboundPort,
    commit_turn_result,
)
from agent.model_runtime.types import ModelClient
from plugins.wake_proactive import energy
from proactive_v2.config import ProactiveConfig
from proactive_v2.contracts import (
    normalize_alert,
    normalize_content,
    normalize_context,
    rank_alerts,
    rank_content,
)
from agent.lifecycle.phase import topo_sort_modules
from proactive_v2.frame import new_proactive_frame
from plugins.proactive_flow.judge import Decision, ProactiveJudge, format_context
from plugins.proactive_flow.modules import build_default_proactive_modules
from plugins.wake_proactive.sources import SourceRegistry
from plugins.wake_proactive.state import ProactiveStateStore
from session.manager import SessionManager

logger = logging.getLogger(__name__)

# maybe_run(now, session_key) -> 是否真的跑了一轮 Drift
DriftHook = Callable[[datetime, str], Awaitable[bool]]

_PROACTIVE_CONTEXT_FILE = "PROACTIVE_CONTEXT.md"
_PROACTIVE_CONTEXT_TEMPLATE = """# Proactive Context

在这里写你对主动推送的明确规则，proactive 判断器每轮都会读取并遵守。
适合写白名单、黑名单、优先级、过滤条件。这里只定义规则，不提供内容事实。
"""


class ProactiveLoop:
    def __init__(
        self,
        *,
        config: ProactiveConfig,
        bus: MessageBus,
        session_manager: SessionManager,
        model_client: ModelClient,
        sources: SourceRegistry,
        state: ProactiveStateStore,
        memory: Any | None = None,
        drift_hook: DriftHook | None = None,
        passive_busy_fn: Callable[[str], bool] | None = None,
        rng: random.Random | None = None,
        memory_services: Any = None,
        plugin_generations: Any = None,
        snapshot_store: Any = None,
        mcp_gateway: Any = None,
        outbound_port: OutboundPort | None = None,
        message_recorder: Callable[[str, str, List[str]], Awaitable[None]] | None = None,
        proactive_context_provider: Callable[[], str] | None = None,
        manage_context_file: bool = True,
    ) -> None:
        self._cfg = config
        self._bus = bus
        self._sessions = session_manager
        self._sources = sources
        self._state = state
        self._memory = memory
        # Stage 4:兴趣检索走引擎(read_only,不强化);未承重时本段为空,不影响判断链路。
        self._memory_services = memory_services
        self._drift_hook = drift_hook
        self._passive_busy_fn = passive_busy_fn
        self._rng = rng or random.Random()
        self._judge = ProactiveJudge(
            model_client,
            model=config.model,
            max_tokens=config.max_tokens,
        )
        self._running = False
        self._wake = asyncio.Event()
        self._workspace = Path(session_manager.workspace)
        # tick 的两份代际租约(与被动 turn 同一对保证,见 runtime.AgentLoop.run):
        # per-plugin 代际防模块/源在 tick 中途被 quiesce,snapshot 租约固定本轮 MCP 工具代际。
        self._plugin_generations = plugin_generations
        self._snapshot_store = snapshot_store
        self._mcp_gateway = mcp_gateway
        self._outbound_port = outbound_port or BusOutboundPort(self._bus)
        self._message_recorder = message_recorder
        self._proactive_context_provider = proactive_context_provider
        self._manage_context_file = manage_context_file
        # 主动链路是模块流水线;默认模块覆盖原有 tick 的每一步。
        # 照 Reference ProactiveKernel:装配期编译一次(排序失败 fail loud),不在每 tick 重排。
        self._modules: List[object] = topo_sort_modules(
            build_default_proactive_modules(self)
        )

    # ── 生命周期 ──────────────────────────────────────────────────

    async def run(self) -> None:
        self._running = True
        if self._manage_context_file:
            self._ensure_context_file()
        logger.info(
            "[proactive] 已启动 target=%s:%s drift=%s",
            self._cfg.channel,
            self._cfg.chat_id,
            self._cfg.drift.enabled,
        )
        await self._flush_pending_acknowledgements()
        while self._running:
            interval = self._next_interval()
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
            if not self._running:
                break
            try:
                await self._tick()
            except Exception:
                logger.exception("[proactive] tick 异常")

    def stop(self) -> None:
        self._running = False
        self._wake.set()

    @property
    def target_channel(self) -> str:
        return self._cfg.channel

    def close(self) -> None:
        self._state.close()

    async def tick_once(self) -> None:
        """按需执行一次完整 tick（供 CLI/演示手动触发，不等电量定时器）。"""
        if self._manage_context_file:
            self._ensure_context_file()
        await self._tick()

    def status(self) -> Dict[str, Any]:
        """返回当前主动链路状态快照，供 CLI/演示回看（可观测性）。"""
        now = datetime.now(timezone.utc)
        last_user_at, recent_count = self._presence(now)
        e = energy.compute_energy(last_user_at, now)
        score = energy.base_score(e, recent_count)
        last_push = self._state.last_push_at(self._cfg.session_key)
        return {
            "target": self._cfg.session_key,
            "energy": round(e, 4),
            "base_score": round(score, 4),
            "recent_msg_count": recent_count,
            "estimated_next_interval_s": self._next_interval(),
            "unread_alert": self._state.unread_count("alert"),
            "unread_content": self._state.unread_count("content"),
            "last_push_at": last_push.isoformat() if last_push else None,
            "in_cooldown": self._state.in_cooldown(
                self._cfg.session_key, now, self._cfg.delivery_cooldown_hours
            ),
            "recent_decisions": self._state.recent_decisions(10),
            "recent_ticks": self._state.recent_ticks(10),
            "sources": [s.id for s in self._sources.sources],
            "drift_enabled": self._cfg.drift.enabled,
        }

    # ── 调度（电量模型）──────────────────────────────────────────

    def _next_interval(self) -> int:
        score = self._current_base_score()
        interval = energy.next_tick_from_score(
            score,
            tick_s1=self._cfg.tick_interval_s1,
            tick_s0=self._cfg.tick_interval_s0,
            tick_jitter=self._cfg.tick_jitter,
            rng=self._rng,
        )
        logger.info(
            "[proactive] 下次 tick 间隔=%ds base_score=%.3f", interval, score
        )
        return interval

    def _current_base_score(self) -> float:
        now = datetime.now(timezone.utc)
        last_user_at, recent_count = self._presence(now)
        e = energy.compute_energy(last_user_at, now)
        return energy.base_score(e, recent_count)

    # ── tick 主链路 ──────────────────────────────────────────────

    @asynccontextmanager
    async def _plugin_generation_lease(self):
        """未接插件代际注册表时是空操作,便于测试与最小构造(同被动侧)。"""
        registry = self._plugin_generations
        if registry is None:
            yield ()
            return
        async with registry.lease_committed() as leased:
            yield leased

    async def _tick(self) -> None:
        """跑一遍模块流水线。

        模块顺序在装配期已按 `requires` 依赖图编译好(不是注册行序);提前结束由
        `frame.terminal` 表达,而不是 return——因此插件可以声明依赖后插进中间,
        不必改这个函数。

        整个 tick 持有 per-plugin 代际租约与 runtime snapshot 租约:tick 进行中发生
        插件换代/热重载,本轮仍用开始时的模块集合与工具代际跑完,新代际下一轮生效。
        """
        modules = list(self._modules)
        async with self._plugin_generation_lease():
            store = self._snapshot_store
            if store is None or store.current is None:
                await self._run_modules(modules)
                return
            lease = store.lease()
            if self._mcp_gateway is not None:
                self._mcp_gateway.pin_snapshot(lease.snapshot)
            try:
                await self._run_modules(modules)
            finally:
                if self._mcp_gateway is not None:
                    self._mcp_gateway.pin_snapshot(None)
                await lease.release()

    async def _run_modules(self, modules: List[object]) -> None:
        frame = new_proactive_frame(self._cfg.session_key)
        tick_id = "tick_%s" % uuid4().hex[:16]
        state = getattr(self, "_state", None)
        if state is None or not hasattr(state, "record_tick_start"):
            for module in modules:
                frame = await module.run(frame)
            return
        state.record_tick_start(tick_id, frame.session_key, frame.now)
        step_count = 0
        try:
            for step_index, module in enumerate(modules):
                started = time.monotonic()
                slot = str(getattr(module, "slot", type(module).__name__))
                was_done = frame.done
                try:
                    frame = await module.run(frame)
                except BaseException as exc:
                    state.record_tick_step(
                        tick_id,
                        step_index,
                        slot,
                        "failed",
                        int((time.monotonic() - started) * 1000),
                        terminal=frame.terminal,
                        error="%s: %s" % (type(exc).__name__, exc),
                    )
                    step_count = step_index + 1
                    raise
                state.record_tick_step(
                    tick_id,
                    step_index,
                    slot,
                    "skipped" if was_done else "completed",
                    int((time.monotonic() - started) * 1000),
                    terminal=frame.terminal,
                )
                step_count = step_index + 1
            state.record_tick_finish(
                tick_id,
                datetime.now(timezone.utc),
                frame.terminal,
                "completed",
                step_count,
            )
        except BaseException as exc:
            state.record_tick_finish(
                tick_id,
                datetime.now(timezone.utc),
                frame.terminal,
                "failed",
                step_count,
                "%s: %s" % (type(exc).__name__, exc),
            )
            raise

    def add_modules(self, modules: List[object]) -> None:
        """插件把自己的模块插进主动链路;顺序仍由 requires 决定。

        装配错误(slot 重复/成环)在这里 fail loud——坏声明只影响它自己的注册操作,
        已编译的流水线保持原样继续服务。
        """
        candidate = list(self._modules) + list(modules)
        self._modules = topo_sort_modules(candidate)

    async def _push_alert(
        self,
        alert_event: dict,
        now: datetime,
        memory_text: str,
        recent_conversation: str,
        proactive_context: str,
        context_text: str,
        recent_proactive: str = "",
    ) -> None:
        contract = normalize_alert(alert_event)
        decision = await self._judge.decide_alert(
            contract,
            memory_text=memory_text,
            recent_conversation=recent_conversation,
            proactive_context=proactive_context,
            current_context=context_text,
            recent_proactive=recent_proactive,
        )
        await self._deliver(decision, now, [contract.item_id])
        source_id = str(alert_event.get("_source") or "")
        source_event_id = str(
            alert_event.get("event_id") or alert_event.get("id") or ""
        )
        acknowledgements = (
            {source_id: [source_event_id]} if source_id and source_event_id else {}
        )
        self._state.consume_and_queue_ack(
            [contract.item_id], acknowledgements, now
        )
        await self._flush_pending_acknowledgements()

    async def _push_content(
        self,
        content_events: List[dict],
        now: datetime,
        memory_text: str,
        recent_conversation: str,
        proactive_context: str,
        context_text: str,
        recent_proactive: str = "",
    ) -> bool:
        # 新内容优先，避免总在最旧的候选上打转
        page = rank_content(content_events)[: self._cfg.content_limit]
        contracts = [normalize_content(event) for event in page]
        decision = await self._judge.decide_content(
            contracts,
            memory_text=memory_text,
            recent_conversation=recent_conversation,
            proactive_context=proactive_context,
            current_context=context_text,
            recent_proactive=recent_proactive,
        )
        if not decision.send:
            return False
        await self._deliver(decision, now, decision.cited_ids)
        # 只消费/ACK 被真正引用的内容
        cited = set(decision.cited_ids)
        selected = [c for c in contracts if c.item_id in cited]
        self._state.consume([c.item_id for c in selected], now)
        feedback_by_source: Dict[str, List[str]] = {}
        for contract in selected:
            source_id = str(contract.raw.get("_source") or "").strip()
            event_id = str(
                contract.raw.get("event_id") or contract.raw.get("id") or ""
            ).strip()
            if source_id and event_id:
                feedback_by_source.setdefault(source_id, []).append(event_id)
        for source_id, event_ids in feedback_by_source.items():
            self._state.queue_feedback(
                source_id,
                event_ids,
                "interesting",
                now,
                reason="selected as proactive evidence",
            )
        await self._flush_pending_feedback()
        return True

    # ── 交付 ────────────────────────────────────────────────────

    async def _deliver(
        self, decision: Decision, now: datetime, evidence_item_ids: List[str]
    ) -> None:
        message = decision.message.strip()
        if not message:
            raise OutboundDeliveryError("主动决策未生成可发送内容")
        delivery_id = uuid4().hex
        # 内容指纹:同一条内容跨进程重启仍得到同一个 key,这是去重的依据。
        delivery_key = hashlib.sha256(message.encode("utf-8")).hexdigest()
        if self._state.is_delivery_duplicate(
            self._cfg.session_key,
            delivery_key,
            self._cfg.delivery_cooldown_hours,
            now,
        ):
            logger.info("[proactive] 命中投递去重,跳过发送")
            self._state.record_decision(now, "delivery_deduped", message[:120])
            return

        async def mark_intent() -> None:
            # **发送前**落地投递意图:进程若在渠道成功与本地提交之间崩溃,
            # 重启后同一内容会命中去重而不会重复打扰。代价是"标记后崩溃"会漏发这一条——
            # 对主动推送而言,重复打扰比偶尔漏发更伤,所以取这一侧。
            self._state.mark_delivery(self._cfg.session_key, delivery_key, now)

        async def rollback_intent() -> None:
            # 渠道明确失败 → 撤销标记,让下一轮能重试。
            self._state.unmark_delivery(self._cfg.session_key, delivery_key)

        async def commit_delivery() -> None:
            # 只在渠道确认送达后才落地:写 Session 供后续 tick 防重复,并起推送冷却。
            if self._message_recorder is not None:
                await self._message_recorder(message, delivery_id, evidence_item_ids)
            else:
                self._record_proactive_message(message, delivery_id, evidence_item_ids)
            self._state.mark_push(self._cfg.session_key, now)

        result = TurnResult(
            decision="reply",
            outbound=TurnOutbound(session_key=self._cfg.session_key, content=message),
            evidence=list(evidence_item_ids),
            trace=TurnTrace(source="proactive", extra={"delivery_id": delivery_id}),
            side_effects=[
                CallableSideEffect(name="proactive_mark_intent", action=mark_intent)
            ],
            success_side_effects=[
                CallableSideEffect(name="proactive_commit", action=commit_delivery)
            ],
            failure_side_effects=[
                CallableSideEffect(name="proactive_rollback", action=rollback_intent)
            ],
        )
        outcome = await commit_turn_result(
            result,
            port=self._outbound_port,
            channel=self._cfg.channel,
            chat_id=self._cfg.chat_id,
            metadata={
                "proactive": True,
                "delivery_id": delivery_id,
                "evidence_item_ids": list(evidence_item_ids),
            },
        )
        if not outcome.dispatched:
            # 保持既有契约:调用方靠这个异常决定"保留未读、不消费事件"。
            raise OutboundDeliveryError("主动消息渠道发送失败")
        logger.info("[proactive] 已推送 message=%r", message[:120])

    def _record_proactive_message(
        self, message: str, delivery_id: str, evidence_item_ids: List[str]
    ) -> None:
        """把主动消息落到目标 session，供后续 tick 感知近期已推内容，避免重复。"""
        session = self._sessions.get_or_create(self._cfg.session_key)
        session.add_message(
            "assistant",
            message,
            proactive=True,
            delivery_id=delivery_id,
            evidence_item_ids=list(evidence_item_ids),
        )
        self._sessions.save(session)

    async def _flush_pending_acknowledgements(self) -> None:
        """对齐 Reference：只有源 ACK 真正成功后，才从持久队列删除。"""
        for source_id, event_ids in self._state.pending_acknowledgements().items():
            if await self._sources.ack(source_id, event_ids):
                self._state.mark_acknowledged(source_id, event_ids)

    async def _flush_pending_feedback(self) -> None:
        for item in self._state.pending_feedback():
            source_id = str(item["source_id"])
            event_id = str(item["source_event_id"])
            feedback = str(item["feedback"])
            if await self._sources.ack(source_id, [event_id], feedback=feedback):
                self._state.mark_feedback_sent(source_id, event_id, feedback)

    @staticmethod
    def _group_acknowledgements(events: List[dict]) -> Dict[str, List[str]]:
        grouped: Dict[str, List[str]] = {}
        for event in events:
            source_id = str(event.get("_source") or "").strip()
            source_event_id = str(
                event.get("event_id") or event.get("id") or ""
            ).strip()
            if source_id and source_event_id:
                grouped.setdefault(source_id, []).append(source_event_id)
        return grouped

    # ── presence / 上下文读取 ────────────────────────────────────

    def _presence(self, now: datetime) -> tuple[Optional[datetime], int]:
        """从目标 session 推导 (最后一条用户消息时间, 近期用户+助手消息条数)。"""
        try:
            session = self._sessions.get_or_create(self._cfg.session_key)
        except Exception:
            return None, 0
        last_user_at: Optional[datetime] = None
        recent_count = 0
        window_start = now.timestamp() - 24 * 3600
        for msg in session.messages:
            ts = _parse_ts(msg.get("timestamp"))
            role = msg.get("role")
            if ts is not None and ts.timestamp() >= window_start:
                if role in ("user", "assistant"):
                    recent_count += 1
            if role == "user" and ts is not None:
                if last_user_at is None or ts > last_user_at:
                    last_user_at = ts
        return last_user_at, recent_count

    def _recent_conversation(self, session_key: str, limit: int = 20) -> str:
        try:
            session = self._sessions.get_or_create(session_key)
        except Exception:
            return ""
        lines: List[str] = []
        for msg in session.messages[-limit:]:
            role = msg.get("role")
            if role not in ("user", "assistant"):
                continue
            if role == "assistant" and msg.get("proactive"):
                continue  # 主动消息不算被动对话
            content = str(msg.get("content") or "")[:300]
            if content:
                lines.append(f"{role}: {content}")
        return "\n".join(lines)[:3000]

    def _recent_proactive(self, session_key: str, limit: int = 5) -> str:
        """读取最近已推送的主动消息，喂给判断器以避免重复推送（对齐 reference 意图）。"""
        try:
            session = self._sessions.get_or_create(session_key)
        except Exception:
            return ""
        collected: List[str] = []
        for msg in reversed(session.messages):
            if msg.get("role") != "assistant" or not msg.get("proactive"):
                continue
            content = str(msg.get("content") or "")[:300]
            if content:
                collected.append(f"- {content}")
            if len(collected) >= limit:
                break
        return "\n".join(reversed(collected))

    def _read_memory(self) -> str:
        reader = getattr(self._memory, "read_long_term", None)
        if not callable(reader):
            return ""
        try:
            return str(reader() or "")
        except Exception:
            return ""

    async def _interest_hits(self, query: str, limit: int = 6) -> str:
        """按候选内容做兴趣检索(照 Reference wake_proactive/tools.py)。

        `effect="read_only"` 保证判断阶段不强化记忆;`relevance_floor="strong"` 只要高置信
        条目,避免把弱相关记忆塞进打扰判断。引擎未承重或失败时返回空串,判断链路照常。
        """
        engine = getattr(self._memory_services, "engine", None)
        if engine is None or not str(query or "").strip():
            return ""
        descriptor = getattr(engine, "DESCRIPTOR", None)
        capabilities = getattr(descriptor, "capabilities", frozenset())
        if MemoryCapability.RETRIEVE_CONTEXT_BLOCK not in capabilities:
            return ""
        try:
            result = await engine.query(
                MemoryQuery(
                    text=query,
                    intent="interest",
                    effect="read_only",
                    filters=MemoryQueryFilters(relevance_floor="strong"),
                    limit=limit,
                    # 同上:akasha 承重,默认引擎忽略。
                    timestamp=datetime.now(timezone.utc),
                )
            )
        except Exception as exc:  # noqa: BLE001 - 兴趣检索失败不阻断主动判断
            logger.warning("[proactive] 兴趣检索失败: %s", exc)
            return ""
        lines = [
            "- %s" % str(record.summary).strip()[:300]
            for record in result.records
            if str(record.summary).strip()
        ]
        return "\n".join(lines)

    def _context_file_path(self) -> Path:
        return self._workspace / _PROACTIVE_CONTEXT_FILE

    def _ensure_context_file(self) -> None:
        path = self._context_file_path()
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(_PROACTIVE_CONTEXT_TEMPLATE, encoding="utf-8")

    def _read_context_file(self) -> str:
        if self._proactive_context_provider is not None:
            try:
                return str(self._proactive_context_provider() or "").strip()
            except Exception:
                return ""
        path = self._context_file_path()
        if not path.exists():
            return ""
        try:
            return path.read_text(encoding="utf-8").strip()
        except Exception:
            return ""


def _parse_ts(value: object) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.astimezone()
    return dt
