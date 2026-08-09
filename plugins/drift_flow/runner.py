"""Drift runner：把"一轮 Drift"跑成一次 agent run。

复用 kirakira 现有的 ``Agent`` loop 与 ``build_default_registry`` 工具集，
额外挂上 ``message_push`` / ``finish_drift`` 收尾工具。SKILL.md 正文作为
system prompt，Drift Briefing 作为首条消息注入。跑完把结果落到 drift.db，
并在主事件循环上投递草稿消息。

参考 akashic 的 `plugins/drift_flow` DriftTurnPipeline，MVP 压平为直接的一次 run。
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, List, Optional, Sequence
from uuid import uuid4

from agent.core.runner import Agent
from bus.queue import MessageBus
from plugins.drift_flow.skills import DriftSkill, discover_skills, ensure_example_skill
from plugins.drift_flow.drive import sample_drift_delay_hours
from plugins.drift_flow.state import DriftStateStore
from plugins.drift_flow.tools import DriftRunContext, register_drift_tools
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
from proactive_v2.config import DriftConfig
from session.manager import SessionManager
from agent.tools.builtins import build_default_registry

logger = logging.getLogger(__name__)


class DriftRunner:
    def __init__(
        self,
        *,
        config: DriftConfig,
        workspace: Path,
        bus: MessageBus,
        session_manager: SessionManager,
        model_client: ModelClient,
        model: str,
        memory: Any | None = None,
        target_channel: str = "",
        target_chat_id: str = "",
        max_tokens: int = 4000,
        skill_roots_provider: Callable[[], Sequence[Path]] | None = None,
        state_store: Any | None = None,
        tool_registry_factory: Callable[[str], Any] | None = None,
        persist_transcripts: bool = True,
        outbound_port: OutboundPort | None = None,
        message_recorder: Callable[[str, str], Awaitable[None]] | None = None,
        manage_example_skill: bool = True,
    ) -> None:
        self._cfg = config
        self._workspace = Path(workspace)
        self._bus = bus
        self._sessions = session_manager
        self._client = model_client
        self._model = model
        self._memory = memory
        self._channel = target_channel
        self._chat_id = target_chat_id
        self._max_tokens = max_tokens
        self._skill_roots_provider = skill_roots_provider or (lambda: ())
        self._state = state_store or DriftStateStore(
            self._workspace / "drift" / "drift.db"
        )
        self._tool_registry_factory = tool_registry_factory
        self._persist_transcripts = persist_transcripts
        self._outbound_port = outbound_port or BusOutboundPort(self._bus)
        self._message_recorder = message_recorder
        self._manage_example_skill = manage_example_skill
        self._rng = random.Random()

    def close(self) -> None:
        self._state.close()

    async def maybe_run(self, now: datetime, session_key: str) -> bool:
        """满足条件则跑一轮 Drift，返回是否真的跑了。"""
        if not self._cfg.enabled:
            return False
        if self._manage_example_skill:
            ensure_example_skill(self._workspace)
        skills = discover_skills(
            self._workspace,
            extra_roots=self._skill_roots_provider(),
        )
        if not skills:
            return False
        # min_interval 仍是硬下限(安全阀);到期采样在此之上决定"闲下来了才去做"。
        if not self._state.can_run(now, self._cfg.min_interval_hours):
            return False
        if not self._hazard_due(now, session_key):
            return False

        skill = self._select_skill(skills)
        ctx = DriftRunContext(skill=skill.name)
        briefing = self._build_briefing(skill, session_key)
        logger.info("[drift] 开始 skill=%s", skill.name)

        try:
            await self._run_agent(skill, briefing, ctx, session_key)
        except Exception:
            logger.exception("[drift] agent run 失败 skill=%s", skill.name)
            self._state.record_run(
                skill=skill.name,
                now=now,
                status="paused",
                briefing="run 异常中断",
                message_result="silent",
            )
            return True

        message_result = await self._commit(ctx, session_key)
        self._state.record_run(
            skill=skill.name,
            now=now,
            status=ctx.status if ctx.finished else "paused",
            briefing=ctx.briefing or "(未填写)",
            message_result=message_result,
        )
        # run 期间只收集意图,这里统一落库:中途异常不会留下半条 journal。
        for entry in ctx.journal_entries:
            self._state.append_journal(
                skill.name,
                entry.get("entry_type", ""),
                {"note": entry.get("note", "")},
                now,
                key=entry.get("key", ""),
            )
        if ctx.scratchpad_update or ctx.next_tendency:
            self._state.save_continuum(
                skill=skill.name,
                now=now,
                scratchpad=ctx.scratchpad_update,
                next_tendency=ctx.next_tendency,
            )
        logger.info(
            "[drift] 结束 skill=%s status=%s message=%s",
            skill.name,
            ctx.status,
            message_result,
        )
        return True

    def _hazard_due(self, now: datetime, session_key: str) -> bool:
        """按 hazard 采样的到期时刻决定本轮是否尝试。

        用采样到期而不是轮询判阈:后者会让"检查得越频繁越容易触发"这种采样假象
        混进来(照 Reference:到期事件只负责开启一次判别)。
        """
        last_user_at = self._last_user_at(session_key)
        if last_user_at is None:
            # 没有任何用户消息 = 没有"空闲多久"的基准,hazard 无从计算。
            # 此时不额外设卡,交回 min_interval 判断,而不是永远不跑。
            return True
        last_drift_at = self._state.last_drift_at()
        anchor = "%s|%s" % (
            last_user_at.isoformat() if last_user_at else "",
            last_drift_at.isoformat() if last_drift_at else "",
        )
        stored = self._state.load_schedule(session_key)
        if stored is None or stored["timer_anchor"] != anchor:
            # 用户又说话了 / 刚跑过 Drift → 锚点变化,按新的空闲状态重新采样
            idle_hours = (
                max(0.0, (now - last_user_at).total_seconds() / 3600)
                if last_user_at is not None
                else 0.0
            )
            recent_drift = (
                math.exp(-max(0.0, (now - last_drift_at).total_seconds()) / (6 * 3600))
                if last_drift_at is not None
                else 0.0
            )
            delay = sample_drift_delay_hours(
                random_draw=self._rng.random(),
                idle_hours=idle_hours,
                recent_drift_suppression=recent_drift,
                repetition_suppression=0.0,
            )
            if not math.isfinite(delay):
                return False
            next_at = now + timedelta(hours=delay)
            self._state.save_schedule(session_key, anchor, next_at, now)
            logger.info(
                "[drift] 采样下一次到期 idle=%.1fh delay=%.2fh at=%s",
                idle_hours,
                delay,
                next_at.isoformat(),
            )
            return False
        if now < stored["next_attempt_at"]:
            return False
        # 到期并将要真的跑一轮 → 清掉排程,下一轮按新的空闲状态重新采样
        self._state.clear_schedule(session_key)
        return True

    def _last_user_at(self, session_key: str) -> Optional[datetime]:
        """最近一条用户消息时间;没有则返回 None(视作没有空闲基准)。"""
        try:
            session = self._sessions.get_or_create(session_key)
        except Exception:
            return None
        for message in reversed(session.messages):
            if message.get("role") != "user":
                continue
            raw = str(message.get("timestamp") or "")
            if not raw:
                continue
            try:
                parsed = datetime.fromisoformat(raw)
            except ValueError:
                continue
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        return None

    def _select_skill(self, skills: List[DriftSkill]) -> DriftSkill:
        """每轮重新比较，选最久没跑过的 skill（从未跑过的优先）。

        排序键用 run_at 的 ISO 串：从未跑过 → "" 最小 → 最先选；跑过的按最早 run_at 优先。
        """
        last_run = self._state.last_run_at_by_skill()
        return min(
            skills,
            key=lambda s: last_run[s.name].isoformat() if s.name in last_run else "",
        )

    async def _run_agent(
        self,
        skill: DriftSkill,
        briefing: str,
        ctx: DriftRunContext,
        session_key: str,
    ) -> None:
        """在工作线程里同步跑一次 agent run。"""
        registry = (
            self._tool_registry_factory(session_key)
            if self._tool_registry_factory is not None
            else build_default_registry(
                self._workspace,
                memory=self._memory,
                session_manager=self._sessions,
                bus=self._bus,
            )
        )
        register_drift_tools(registry, ctx)
        token = registry.set_context(
            channel=self._channel,
            chat_id=self._chat_id,
            session_key=session_key,
        )
        try:
            agent = Agent(
                model_client=self._client,
                tool_registry=registry,
                model=self._model,
                workdir=self._workspace,
                system=skill.body,
                max_tokens=self._max_tokens,
                persist_transcripts=self._persist_transcripts,
            )
            # 照 Reference drift 主循环(plugins/drift_flow/runtime.py):每步必须调工具,
            # 预算最后一步具名强制 finish_drift,finish 执行后立即收束——收尾由服务端
            # tool_choice 保证,不再只靠 briefing 里的一句提示。
            await agent.arun(
                [{"role": "user", "content": briefing}],
                max_rounds=self._cfg.max_steps,
                tool_choice="required",
                final_tool_choice={
                    "type": "function",
                    "function": {"name": "finish_drift"},
                },
                stop_tools={"finish_drift"},
            )
        finally:
            registry.reset_context(token)
            await registry.shutdown()

    async def _commit(self, ctx: DriftRunContext, session_key: str) -> str:
        """把 Drift 草稿投递，并按 Reference 修正为 sent / silent。

        用 TurnResult 显式声明副作用:写 Session 只挂在 success 分支,
        所以"渠道没送到就不留痕"这条语义由提交器保证,不再依赖手写分支。
        """
        if not (ctx.message_pushed and ctx.draft_message and self._channel and self._chat_id):
            return "silent"
        delivery_id = uuid4().hex
        message = ctx.draft_message

        async def record_session() -> None:
            if self._message_recorder is not None:
                await self._message_recorder(message, delivery_id)
                return
            session = self._sessions.get_or_create(session_key)
            session.add_message(
                "assistant",
                message,
                proactive=True,
                drift=True,
                delivery_id=delivery_id,
            )
            self._sessions.save(session)

        result = TurnResult(
            decision="reply",
            outbound=TurnOutbound(session_key=session_key, content=message),
            trace=TurnTrace(source="drift", extra={"delivery_id": delivery_id}),
            success_side_effects=[
                CallableSideEffect(name="drift_record_session", action=record_session)
            ],
        )
        outcome = await commit_turn_result(
            result,
            port=self._outbound_port,
            channel=self._channel,
            chat_id=self._chat_id,
            metadata={"proactive": True, "drift": True, "delivery_id": delivery_id},
        )
        return "sent" if outcome.dispatched else "silent"

    def _build_briefing(self, skill: DriftSkill, session_key: str) -> str:
        """拼一份 Drift Briefing：记忆 + 近期上下文 + 本 skill 连续性 + 最近 run。"""
        sections: List[str] = [
            "你现在处于 Drift 空闲模式：没有需要主动推送的内容，利用这段时间按下面的"
            "技能指南（已作为 system prompt）执行一个后台小任务。执行结束前必须调用 finish_drift。",
        ]
        memory_text = self._read_memory()
        if memory_text:
            sections.append("【长期记忆】\n" + memory_text.strip()[:4000])
        recent_context = self._read_recent_context()
        if recent_context:
            sections.append("【近期上下文】\n" + recent_context.strip()[:2000])
        journal = self._state.load_journal(skill.name, limit=8)
        if journal:
            lines = [
                "- [%s] %s" % (e["entry_type"], str(e["payload"].get("note") or "")[:200])
                for e in journal
            ]
            sections.append("【本技能 journal】\n" + "\n".join(lines))
        observations = self._state.recent_self_observations(limit=6)
        if observations:
            lines = [
                "- %s: %s" % (o["skill"], str(o["payload"].get("note") or "")[:200])
                for o in observations
            ]
            sections.append("【最近自我观察】\n" + "\n".join(lines))
        continuum = self._state.get_continuum(skill.name)
        if continuum.get("scratchpad") or continuum.get("next_tendency"):
            sections.append(
                "【本技能前情】\n"
                + f"scratchpad: {continuum.get('scratchpad') or '（无）'}\n"
                + f"上轮倾向: {continuum.get('next_tendency') or '（无）'}"
            )
        recent_runs = self._state.recent_runs(limit=5)
        if recent_runs:
            lines = [
                f"- {r['run_at']} {r['skill']} [{r['status']}] {r['briefing']}"
                for r in recent_runs
            ]
            sections.append("【最近 Drift 记录】\n" + "\n".join(lines))
        return "\n\n".join(sections)

    def _read_memory(self) -> str:
        reader = getattr(self._memory, "read_long_term", None)
        if not callable(reader):
            return ""
        try:
            return str(reader() or "")
        except Exception:
            return ""

    def _read_recent_context(self) -> str:
        reader = getattr(self._memory, "read_recent_context", None)
        if not callable(reader):
            return ""
        try:
            return str(reader() or "")
        except Exception:
            return ""
