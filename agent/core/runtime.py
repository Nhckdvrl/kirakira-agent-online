"""Akashic-style passive runtime: AgentLoop, PassiveTurnPipeline, and Reasoner."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
from collections import OrderedDict
from datetime import datetime
import inspect
import json
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from bus.queue import MessageBus
from agent.prompting.context_builder import ContextBuilder
from agent.model_runtime.context_policy import (
    build_runtime_context_budget,
    estimate_context_tokens,
)
from bus.event_bus import EventBus
from bus.events import InboundMessage, OutboundMessage
from agent.turns.models import TurnRequest, TurnResult
from bus.events_lifecycle import (
    AfterReasoningCtx,
    AfterReasoningResult,
    AfterStepCtx,
    AfterTurnCtx,
    BeforeReasoningCtx,
    BeforeStepCtx,
    BeforeTurnCtx,
    ContextBudgetUpdated,
    ContextPrepared,
    PromptRenderCtx,
    TurnCommitted,
    TurnFinished,
    TurnStarted,
    ToolCallCompleted,
    ToolCallStarted,
    StreamDeltaReady,
    TurnState,
)
from core.memory.legacy import MemoryRuntime
from core.memory.engine import (
    MemoryCapability as _CoreMemCapability,
    MemoryQuery as _CoreMemQuery,
    MemoryScope as _CoreMemScope,
)
from core.memory.services import MemoryServices
from agent.looping.ports import ContextServices, SessionServices
from agent.model_runtime.types import ContentSafetyError, ContextLengthError, ModelClient
from agent.model_runtime.query_compaction import (
    ContextCompactionError,
    QueryCompactor,
)
from agent.model_runtime.usage import aggregate_usage, usage_from_mapping
from core.schema import ModelResponse, ToolCall, ToolResult, assistant_message_from_response, tool_result_message
from session.ports import TranscriptStore
from agent.plugins.snapshot import (
    RuntimeSnapshotStore,
    SnapshotToolView,
    bind_runtime_snapshot,
    get_current_runtime_snapshot,
    reset_runtime_snapshot,
)
from agent.tool_hooks import ToolExecutionRequest, ToolExecutor, ToolHook
from agent.tools.registry import ToolRegistry
if TYPE_CHECKING:
    from infra.channels.host import ChannelHost
from agent.prompting import (
    DEFAULT_CONTEXT_TRIM_PLANS,
    PromptSectionRender,
    build_context_frame_content,
    build_context_frame_message,
)
from agent.retrieval.default_pipeline import RetrievalRequest

logger = logging.getLogger(__name__)
JsonDict = Dict[str, Any]


@dataclass
class RuntimeConfig:
    model: str
    context_window: int = 0
    effective_context_percent: float = 0.9
    max_iterations: int = 10
    max_tokens: int = 8192
    history_window: int = 40
    model_timeout_seconds: float = 120.0
    repeated_tool_call_limit: int = 3
    stream: bool = True


@dataclass
class ReasonerResult:
    reply: str
    tools_used: List[str] = field(default_factory=list)
    tool_chain: List[JsonDict] = field(default_factory=list)
    thinking: str = ""
    context_trace: JsonDict = field(default_factory=dict)
    # 本轮是否有工具声明需要用户确认;None 表示没有。
    mobile_attention: Optional[str] = None
    react_compaction: JsonDict | None = None


async def _run_plugin_modules(modules: List[object], ctx: Any) -> Any:
    current = ctx
    for module in modules:
        runner = getattr(module, "run", None)
        if runner is None and callable(module):
            runner = module
        if runner is None:
            continue
        result = runner(current)
        if inspect.isawaitable(result):
            result = await result
        if result is not None:
            current = result
    return current


def _turn_phase_modules(static: List[object], field: str) -> List[object]:
    """Compose process capabilities with the per-turn leased generation."""

    snapshot = get_current_runtime_snapshot()
    dynamic = list(getattr(snapshot, field, ())) if snapshot is not None else []
    return [*static, *dynamic]


class DefaultReasoner:
    def __init__(
        self,
        *,
        model_client: ModelClient,
        tools: ToolRegistry,
        config: RuntimeConfig,
        context: ContextBuilder,
        event_bus: EventBus,
    ) -> None:
        self.model_client = model_client
        self.tools = tools
        self.config = config
        self.context = context
        self.event_bus = event_bus
        self._tool_executor = ToolExecutor()
        self._prompt_render_modules: List[object] = []
        self._before_step_modules: List[object] = []
        self._after_step_modules: List[object] = []
        self._unlocked_tools: Dict[str, OrderedDict[str, None]] = {}

    def add_tool_hooks(self, hooks: List[ToolHook]) -> None:
        self._tool_executor.add_hooks(hooks)

    def add_prompt_render_plugin_modules(self, modules: List[object]) -> None:
        self._prompt_render_modules.extend(modules)

    def add_before_step_plugin_modules(self, modules: List[object]) -> None:
        self._before_step_modules.extend(modules)

    def add_after_step_plugin_modules(self, modules: List[object]) -> None:
        self._after_step_modules.extend(modules)

    async def run_turn(
        self,
        *,
        msg: InboundMessage,
        session_key: str,
        history: List[JsonDict],
        retrieved_memory_block: str,
        skill_names: Optional[List[str]],
        extra_hints: Optional[List[str]],
        disabled_tools: Optional[set[str]] = None,
        max_iterations_override: Optional[int] = None,
    ) -> ReasonerResult:
        source_history = list(history)
        attempts = self._build_attempt_plans(len(source_history))
        retry_trace: JsonDict = {"attempts": [], "selected_plan": None}
        tools = SnapshotToolView(self.tools, get_current_runtime_snapshot())
        disabled = set(disabled_tools or ())
        unlocked = set(self._unlocked_tools.get(session_key, {}).keys())
        visible_specs = [
            spec
            for spec in tools.visible_specs(unlocked)
            if spec.name not in disabled
        ]
        for attempt, plan in enumerate(attempts):
            history_for_attempt = self._slice_history(
                source_history, int(plan["history_window"])
            )
            render_ctx = PromptRenderCtx(
                session_key=session_key,
                channel=msg.context_channel,
                chat_id=msg.context_chat_id,
                content=msg.content,
                media=msg.media,
                timestamp=msg.timestamp,
                history=history_for_attempt,
                skill_names=skill_names,
                retrieved_memory_block=retrieved_memory_block,
                extra_hints=list(extra_hints or []),
                disabled_sections=set(plan["disabled_sections"]),
                turn_injection_prompt=self._build_deferred_tools_hint(
                    tools, unlocked
                ),
            )
            render_ctx = await self.event_bus.emit(render_ctx)
            render_ctx = await _run_plugin_modules(
                _turn_phase_modules(
                    self._prompt_render_modules, "prompt_render_modules"
                ),
                render_ctx,
            )
            rendered = self.context.render(
                channel=render_ctx.channel,
                chat_id=render_ctx.chat_id,
                content=render_ctx.content,
                media=render_ctx.media,
                timestamp=render_ctx.timestamp,
                history=render_ctx.history,
                retrieved_memory_block=render_ctx.retrieved_memory_block,
                skill_names=render_ctx.skill_names,
                extra_hints=render_ctx.extra_hints,
                system_sections_top=render_ctx.system_sections_top,
                system_sections_bottom=render_ctx.system_sections_bottom,
                disabled_sections=render_ctx.disabled_sections,
                turn_injection_prompt=render_ctx.turn_injection_prompt,
            )
            estimated = estimate_context_tokens(rendered.messages, visible_specs)
            input_budget = 0
            if self.config.context_window:
                input_budget = build_runtime_context_budget(
                    self.config.context_window,
                    self.config.effective_context_percent,
                    self.config.max_tokens,
                ).input_budget
            sections = tuple(
                {
                    "name": item.name,
                    "chars": item.chars,
                    "est_tokens": item.est_tokens,
                    "is_static": item.is_static,
                    "cache_hit": item.cache_hit,
                }
                for item in rendered.debug_breakdown
            )
            attempt_trace = {
                "name": plan["name"],
                "history_window": plan["history_window"],
                "history_messages": len(history_for_attempt),
                "disabled_sections": sorted(render_ctx.disabled_sections),
                "estimated_tokens": estimated,
                "estimate_quality": "approximate",
                "input_budget": input_budget,
                "sections": list(sections),
            }
            retry_trace["attempts"].append(attempt_trace)
            await self.event_bus.fanout(
                ContextPrepared(
                    session_key=session_key,
                    channel=msg.channel,
                    chat_id=msg.chat_id,
                    attempt=attempt,
                    plan_name=str(plan["name"]),
                    history_messages=len(history_for_attempt),
                    disabled_sections=tuple(sorted(render_ctx.disabled_sections)),
                    estimated_tokens=estimated,
                    estimate_quality="approximate",
                    input_budget=input_budget,
                    context_frame_chars=len(rendered.context_frame),
                    sections=sections,
                )
            )
            try:
                result = await self.run(
                    rendered.messages,
                    session_key=session_key,
                    channel=msg.channel,
                    chat_id=msg.chat_id,
                    request_text=msg.content,
                    disabled_tools=disabled_tools,
                    max_iterations_override=max_iterations_override,
                )
            except (ContextLengthError, ContentSafetyError) as exc:
                attempt_trace["error"] = type(exc).__name__
                if attempt + 1 < len(attempts):
                    continue
                reply = (
                    "上下文过长，已尝试分级精简但仍无法处理；请新建会话或缩小请求。"
                    if isinstance(exc, ContextLengthError)
                    else "当前上下文触发了内容安全限制，无法继续处理。"
                )
                retry_trace["selected_plan"] = plan["name"]
                retry_trace["trimmed_sections"] = sorted(
                    render_ctx.disabled_sections
                )
                return ReasonerResult(reply=reply, context_trace=retry_trace)
            retry_trace["selected_plan"] = plan["name"]
            retry_trace["trimmed_sections"] = sorted(render_ctx.disabled_sections)
            retry_trace["react_stats"] = dict(
                result.context_trace.get("react_stats") or {}
            )
            result.context_trace = retry_trace
            return result
        return ReasonerResult("上下文准备失败。", context_trace=retry_trace)

    async def run(
        self,
        messages: List[JsonDict],
        *,
        session_key: str,
        channel: str,
        chat_id: str,
        request_text: str,
        disabled_tools: Optional[set[str]] = None,
        max_iterations_override: Optional[int] = None,
    ) -> ReasonerResult:
        tools_used: List[str] = []
        tool_chain: List[JsonDict] = []
        final_reply = ""
        final_thinking = ""
        # 整轮只解析一次工具视图：本 turn 看到的 MCP 工具由 turn 开始时锁定的快照决定。
        tools = SnapshotToolView(self.tools, get_current_runtime_snapshot())
        disabled = set(disabled_tools or ())
        unlocked = set(self._unlocked_tools.get(session_key, {}).keys())
        repeated_calls: Dict[str, int] = {}
        react_input_samples: List[int] = []
        model_usages: List[JsonDict] = []
        empty_thinking_retry_used = False
        mobile_attention: Optional[str] = None
        iteration = 0
        iteration_limit = (
            self.config.max_iterations
            if max_iterations_override is None
            else max(1, int(max_iterations_override))
        )
        context_window = max(0, int(self.config.context_window))
        compaction_hard_limit = context_window
        if context_window:
            compaction_hard_limit = build_runtime_context_budget(
                context_window,
                self.config.effective_context_percent,
                self.config.max_tokens,
            ).input_budget
        compaction_specs: List[Any] = []
        compactor = QueryCompactor(
            base_messages=messages,
            context_window=context_window,
            soft_limit_tokens=(
                int(compaction_hard_limit * 0.74) if compaction_hard_limit else 0
            ),
            hard_limit_tokens=compaction_hard_limit,
            scope_id=session_key,
            estimate=lambda items: estimate_context_tokens(items, compaction_specs),
        )

        async def summarize_compaction(prompt: str) -> str:
            summary_response = await self._complete_model(
                [{"role": "user", "content": prompt}],
                [],
                session_key=session_key,
                channel=channel,
                chat_id=chat_id,
                iteration=-1,
            )
            if summary_response.tool_calls:
                return ""
            model_usages.append(dict(summary_response.usage or {}))
            return summary_response.text

        while iteration_limit <= 0 or iteration < iteration_limit:
            visible_specs = [
                spec
                for spec in tools.visible_specs(unlocked)
                if spec.name not in disabled
            ]
            visible_names = tuple(spec.name for spec in visible_specs)
            compaction_specs[:] = visible_specs
            await compactor.prepare(
                messages,
                summarize=summarize_compaction,
            )
            batch_start = len(messages)
            before_step = BeforeStepCtx(
                session_key=session_key,
                channel=channel,
                chat_id=chat_id,
                iteration=iteration,
                input_tokens_estimate=estimate_context_tokens(messages, visible_specs),
                visible_tool_names=visible_names,
            )
            before_step = await self.event_bus.emit(before_step)
            before_step = await _run_plugin_modules(
                _turn_phase_modules(self._before_step_modules, "before_step_modules"),
                before_step,
            )
            if before_step.early_stop:
                return ReasonerResult(
                    reply=before_step.early_stop_reply,
                    tools_used=tools_used,
                    tool_chain=tool_chain,
                    thinking=final_thinking,
                    mobile_attention=mobile_attention,
                    react_compaction=compactor.persistence_payload(),
                )
            if before_step.extra_hints:
                messages.append(
                    build_context_frame_message(
                        build_context_frame_content(
                            [
                                PromptSectionRender(
                                    "plugin_hints",
                                    "\n".join(before_step.extra_hints),
                                    False,
                                )
                            ]
                        )
                    )
                )

            react_input_samples.append(before_step.input_tokens_estimate)
            try:
                response = await self._complete_model(
                    messages,
                    visible_specs,
                    session_key=session_key,
                    channel=channel,
                    chat_id=chat_id,
                    iteration=iteration,
                )
            except ContextLengthError as overflow:
                pending_count = len(messages) - batch_start
                try:
                    compacted = await compactor.prepare(
                        messages,
                        summarize=summarize_compaction,
                        pending_start=batch_start,
                        trigger="context_overflow",
                        force=True,
                    )
                except ContextCompactionError:
                    raise overflow
                if not compacted:
                    raise overflow
                batch_start = len(messages) - pending_count
                response = await self._complete_model(
                    messages,
                    visible_specs,
                    session_key=session_key,
                    channel=channel,
                    chat_id=chat_id,
                    iteration=iteration,
                )
            model_usages.append(dict(response.usage or {}))
            final_reply = response.text or ""
            final_thinking = response.reasoning_content or final_thinking
            messages.append(assistant_message_from_response(response))
            if not response.tool_calls:
                if not final_reply.strip() and response.reasoning_content and not empty_thinking_retry_used:
                    empty_thinking_retry_used = True
                    messages.append(
                        {
                            "role": "user",
                            "content": "<system-reminder>请输出给用户看的最终回复，不要只返回思考过程。</system-reminder>",
                        }
                    )
                    iteration += 1
                    continue
                await self._after_step(
                    session_key, channel, chat_id, iteration, messages, (), final_reply,
                    tools_used, tool_chain, final_thinking, has_more=False,
                )
                self._remember_unlocked(session_key, unlocked)
                return ReasonerResult(
                    final_reply or "模型没有返回可展示的回复。",
                    tools_used,
                    tool_chain,
                    final_thinking,
                    context_trace={
                        "react_stats": self._react_stats(
                            react_input_samples, model_usages
                        )
                    },
                    mobile_attention=mobile_attention,
                    react_compaction=compactor.persistence_payload(),
                )

            group = {
                "text": response.text or "",
                "reasoning_content": response.reasoning_content or "",
                "calls": [],
            }
            for call_index, call in enumerate(response.tool_calls):
                signature = "%s:%s" % (
                    call.name,
                    json.dumps(call.arguments, ensure_ascii=False, sort_keys=True, default=str),
                )
                repeated_calls[signature] = repeated_calls.get(signature, 0) + 1
                if call.name in disabled:
                    result = await self._deny_tool(
                        call,
                        session_key,
                        channel,
                        chat_id,
                        iteration,
                        "Error: Tool '%s' is disabled for this turn" % call.name,
                    )
                elif tools.is_deferred(call.name) and call.name not in unlocked:
                    result = await self._deny_tool(
                        call,
                        session_key,
                        channel,
                        chat_id,
                        iteration,
                        "Error: Deferred tool '%s' is not loaded; call tool_search with select:%s"
                        % (call.name, call.name),
                    )
                elif repeated_calls[signature] > max(1, self.config.repeated_tool_call_limit):
                    result = await self._deny_tool(
                        call,
                        session_key,
                        channel,
                        chat_id,
                        iteration,
                        "Error: Repeated identical tool call blocked by loop guard",
                    )
                else:
                    result = await self._execute_tool(
                        call,
                        session_key,
                        channel,
                        chat_id,
                        request_text,
                        iteration,
                        tools,
                        call_index,
                    )
                tools_used.append(call.name)
                if result.get("mobile_attention") is not None:
                    mobile_attention = str(result["mobile_attention"])
                group["calls"].append(
                    {
                        "call_id": call.id,
                        "name": call.name,
                        "arguments": result["arguments"],
                        "result": result["content"],
                        "status": result["status"],
                    }
                )
                messages.append(
                    tool_result_message(
                        ToolResult(
                            tool_call_id=call.id,
                            content=result["content"],
                            is_error=result["status"] != "success",
                        )
                    )
                )
                if call.name == "tool_search" and result["status"] == "success":
                    unlocked.update(self._unlocked_from_search(result["content"]))
                    self._remember_unlocked(session_key, unlocked)
                elif result["status"] == "success" and tools.is_deferred(call.name):
                    unlocked.add(call.name)
                    self._remember_unlocked(session_key, unlocked)
            tool_chain.append(group)
            compactor.record_completed_batch(messages[batch_start:])
            await self._after_step(
                session_key,
                channel,
                chat_id,
                iteration,
                messages,
                tuple(c.name for c in response.tool_calls),
                final_reply,
                tools_used,
                tool_chain,
                final_thinking,
                has_more=True,
            )
            iteration += 1
        self._remember_unlocked(session_key, unlocked)
        summary_reply = ""
        if messages:
            summary_messages = [
                *messages,
                {
                    "role": "user",
                    "content": (
                        "<system-reminder>工具执行预算已结束。请基于已经获得的结果，"
                        "直接向用户给出阶段性回复：说明已完成什么、关键结果、尚缺什么和下一步。"
                        "不要继续调用工具，也不要暴露内部调用 ID。</system-reminder>"
                    ),
                },
            ]
            try:
                summary = await self._complete_model(
                    summary_messages,
                    [],
                    session_key=session_key,
                    channel=channel,
                    chat_id=chat_id,
                    iteration=iteration,
                )
                summary_reply = summary.text.strip()
                final_thinking = summary.reasoning_content or final_thinking
                model_usages.append(dict(summary.usage or {}))
            except Exception:
                logger.exception("failed to generate tool-budget summary")
        return ReasonerResult(
            reply=summary_reply
            or final_reply
            or "工具执行预算已结束；已有结果已保留，可以在下一轮继续。",
            tools_used=tools_used,
            tool_chain=tool_chain,
            thinking=final_thinking,
            context_trace={
                "react_stats": self._react_stats(react_input_samples, model_usages)
            },
            mobile_attention=mobile_attention,
            react_compaction=compactor.persistence_payload(),
        )

    @staticmethod
    def _react_stats(input_samples: List[int], usages: List[JsonDict]) -> JsonDict:
        totals = aggregate_usage([usage_from_mapping(item) for item in usages])
        return {
            "iteration_count": len(input_samples),
            "input_token_estimates": list(input_samples),
            "max_input_tokens_estimate": max(input_samples, default=0),
            "model_usage": totals.to_dict(),
            # Backward-compatible shortcut used by the control-plane adapter.
            "request_count": totals.request_count,
        }

    @staticmethod
    def _unlocked_from_search(content: str) -> set[str]:
        try:
            payload = json.loads(content)
        except ValueError:
            return set()
        raw = payload.get("unlocked", []) if isinstance(payload, dict) else []
        return {str(item) for item in raw if isinstance(item, str) and item}

    def _remember_unlocked(self, session_key: str, names: set[str]) -> None:
        lru = self._unlocked_tools.setdefault(session_key, OrderedDict())
        for name in sorted(names):
            if name in lru:
                lru.move_to_end(name)
            else:
                lru[name] = None
            while len(lru) > 5:
                lru.popitem(last=False)

    @staticmethod
    def _slice_history(history: List[JsonDict], window: int) -> List[JsonDict]:
        if window <= 0:
            return []
        selected = list(history if window >= len(history) else history[-window:])
        first_user = next(
            (
                index
                for index, message in enumerate(selected)
                if message.get("role") == "user"
            ),
            None,
        )
        return selected[first_user:] if first_user is not None else []

    @staticmethod
    def _build_attempt_plans(total_history: int) -> List[JsonDict]:
        attempts: List[JsonDict] = []
        seen: set[Tuple[Tuple[str, ...], int]] = set()
        for trim_plan in DEFAULT_CONTEXT_TRIM_PLANS:
            disabled = set(trim_plan.drop_sections)
            key = (tuple(sorted(disabled)), total_history)
            if key not in seen:
                seen.add(key)
                attempts.append(
                    {
                        "name": trim_plan.name,
                        "disabled_sections": disabled,
                        "history_window": total_history,
                    }
                )
        final_disabled = set(DEFAULT_CONTEXT_TRIM_PLANS[-1].drop_sections)
        for ratio in (0.5, 0.0):
            window = int(total_history * ratio)
            key = (tuple(sorted(final_disabled)), window)
            if key not in seen:
                seen.add(key)
                attempts.append(
                    {
                        "name": "trim_retrieved_memory_history_%d" % int(ratio * 100),
                        "disabled_sections": set(final_disabled),
                        "history_window": window,
                    }
                )
        return attempts

    @staticmethod
    def _build_deferred_tools_hint(tools: Any, unlocked: set[str]) -> str:
        names = [
            name
            for name in tools.names()
            if tools.is_deferred(name) and name not in unlocked
        ]
        if not names:
            return ""
        return (
            "【未加载工具目录（知道名字但 schema 未暴露）】\n"
            + ", ".join(names)
            + "\n加载方式：已知名字时调用 tool_search(query=\"select:工具名\")；"
            "按能力查找时调用 tool_search(query=\"关键词\")。"
        )

    async def _complete_model(
        self,
        messages: List[JsonDict],
        specs: List[Any],
        *,
        session_key: str,
        channel: str,
        chat_id: str,
        iteration: int,
    ) -> ModelResponse:
        timeout = max(1.0, float(self.config.model_timeout_seconds))
        stream_method = getattr(self.model_client, "complete_stream", None)
        acomplete = getattr(self.model_client, "acomplete", None)
        try:
            if not self.config.stream or not callable(stream_method):
                # 异步原生客户端直接 await；同步 stub 回退到 to_thread(complete)。
                if callable(acomplete):
                    call = acomplete(
                        messages,
                        specs,
                        "",
                        self.config.model,
                        self.config.max_tokens,
                    )
                else:
                    call = asyncio.to_thread(
                        self.model_client.complete,
                        messages,
                        specs,
                        "",
                        self.config.model,
                        self.config.max_tokens,
                    )
                return await asyncio.wait_for(call, timeout=timeout)
            queue: asyncio.Queue[Tuple[str, str]] = asyncio.Queue()
            loop = asyncio.get_running_loop()

            def on_delta(content: str, reasoning: str) -> None:
                loop.call_soon_threadsafe(queue.put_nowait, (content, reasoning))

            astream = getattr(self.model_client, "acomplete_stream", None)
            if callable(astream):
                # 异步原生流:worker 直接在事件循环上跑,on_delta 从循环线程回投 queue。
                worker = asyncio.create_task(
                    astream(
                        messages,
                        specs,
                        "",
                        self.config.model,
                        self.config.max_tokens,
                        on_delta,
                    )
                )
            else:
                worker = asyncio.create_task(
                    asyncio.to_thread(
                        stream_method,
                        messages,
                        specs,
                        "",
                        self.config.model,
                        self.config.max_tokens,
                        on_delta,
                    )
                )
            deadline = loop.time() + timeout
            while not worker.done() or not queue.empty():
                remaining = deadline - loop.time()
                if remaining <= 0:
                    worker.cancel()
                    raise asyncio.TimeoutError
                try:
                    content_delta, reasoning_delta = await asyncio.wait_for(
                        queue.get(), timeout=min(0.1, remaining)
                    )
                except asyncio.TimeoutError:
                    continue
                await self.event_bus.fanout(
                    StreamDeltaReady(
                        session_key=session_key,
                        channel=channel,
                        chat_id=chat_id,
                        iteration=iteration,
                        content_delta=content_delta,
                        reasoning_delta=reasoning_delta,
                    )
                )
            return await worker
        except asyncio.TimeoutError as exc:
            raise RuntimeError(
                "LLM 请求超过 %.1f 秒，已停止本轮。" % timeout
            ) from exc

    async def _execute_tool(
        self,
        call: ToolCall,
        session_key: str,
        channel: str,
        chat_id: str,
        request_text: str,
        iteration: int,
        tools: Any = None,
        call_index: int = 0,
    ) -> JsonDict:
        request = ToolExecutionRequest(
            session_key=session_key,
            channel=channel,
            chat_id=chat_id,
            tool_name=call.name,
            arguments=call.arguments,
            call_id=call.id,
            request_text=request_text,
            iteration=iteration,
            call_index=call_index,
        )
        await self.event_bus.fanout(
            ToolCallStarted(
                session_key=session_key,
                channel=channel,
                chat_id=chat_id,
                call_id=call.id,
                tool_name=call.name,
                arguments=dict(call.arguments),
                iteration=iteration,
            )
        )

        registry = tools if tools is not None else self.tools

        async def invoke(tool_name: str, arguments: JsonDict) -> ToolResult:
            return await registry.execute_async(ToolCall(call.id, tool_name, arguments))

        snapshot = get_current_runtime_snapshot()
        result = await self._tool_executor.execute(
            request,
            invoke,
            additional_hooks=list(snapshot.tool_hooks) if snapshot is not None else None,
        )
        content = result.output
        if result.extra_messages:
            content += "\n\n" + "\n".join(result.extra_messages)
        # 照 Reference passive_turn.py:1656——只有成功的工具可以声明注意力标记,
        # 失败的工具声明属于工具实现 bug,必须 fail loud 而不是静默丢弃。
        if result.mobile_attention is not None:
            if result.status != "success":
                raise RuntimeError("失败工具不能声明 mobile_attention")
            if result.mobile_attention != "confirmation":
                raise RuntimeError(
                    "无效 mobile_attention: %s" % result.mobile_attention
                )
        await self.event_bus.fanout(
            ToolCallCompleted(
                session_key=session_key,
                channel=channel,
                chat_id=chat_id,
                call_id=call.id,
                tool_name=call.name,
                arguments=dict(result.final_arguments),
                result=content,
                status=result.status,
                iteration=iteration,
            )
        )
        return {
            "content": content,
            "status": result.status,
            "arguments": result.final_arguments,
            "mobile_attention": result.mobile_attention,
        }

    async def _deny_tool(
        self,
        call: ToolCall,
        session_key: str,
        channel: str,
        chat_id: str,
        iteration: int,
        reason: str,
    ) -> JsonDict:
        started = ToolCallStarted(
            session_key=session_key,
            channel=channel,
            chat_id=chat_id,
            call_id=call.id,
            tool_name=call.name,
            arguments=dict(call.arguments),
            iteration=iteration,
        )
        await self.event_bus.fanout(started)
        await self.event_bus.fanout(
            ToolCallCompleted(
                session_key=session_key,
                channel=channel,
                chat_id=chat_id,
                call_id=call.id,
                tool_name=call.name,
                arguments=dict(call.arguments),
                result=reason,
                status="denied",
                iteration=iteration,
            )
        )
        return {
            "content": reason,
            "status": "denied",
            "arguments": dict(call.arguments),
        }

    async def _after_step(
        self,
        session_key: str,
        channel: str,
        chat_id: str,
        iteration: int,
        messages: List[JsonDict],
        tools_called: Tuple[str, ...],
        partial_reply: str,
        tools_used: List[str],
        tool_chain: List[JsonDict],
        thinking: str,
        *,
        has_more: bool,
    ) -> None:
        ctx = AfterStepCtx(
            session_key=session_key,
            channel=channel,
            chat_id=chat_id,
            iteration=iteration,
            context_tokens_estimate=max(1, len(json.dumps(messages, ensure_ascii=False)) // 3),
            tools_called=tools_called,
            partial_reply=partial_reply,
            tools_used_so_far=tuple(tools_used),
            tool_chain_partial=tuple(tool_chain),
            partial_thinking=thinking,
            has_more=has_more,
        )
        await self.event_bus.fanout(ctx)
        await _run_plugin_modules(
            _turn_phase_modules(self._after_step_modules, "after_step_modules"), ctx
        )


def _engine_can_retrieve(engine: object) -> bool:
    """引擎是否具备真实上下文检索能力。DisabledMemoryEngine 能力集为空,走旧词法路径。"""
    descriptor = getattr(engine, "DESCRIPTOR", None)
    capabilities = getattr(descriptor, "capabilities", frozenset())
    return _CoreMemCapability.RETRIEVE_CONTEXT_BLOCK in capabilities


class PassiveTurnPipeline:
    def __init__(
        self,
        *,
        bus: MessageBus,
        event_bus: EventBus,
        session_manager: TranscriptStore,
        memory: MemoryRuntime,
        tools: ToolRegistry,
        reasoner: DefaultReasoner,
        config: RuntimeConfig,
        snapshot_store: RuntimeSnapshotStore | None = None,
        memory_services: "MemoryServices | None" = None,
        plugin_generations: Any = None,
        session_services: "SessionServices | None" = None,
        context_services: "ContextServices | None" = None,
        recall_inspector: Any = None,
    ) -> None:
        self.bus = bus
        self.event_bus = event_bus
        # 检索回放记录器;未注入时不记录,主链路行为不变。
        self.recall_inspector = recall_inspector
        # 服务包优先;未注入时用具体对象兜底,便于最小构造与测试。
        # 两条路都收敛到同一个属性,pipeline 内部只认服务包。
        self.session_services = session_services or SessionServices(
            transcript_store=session_manager
        )
        self.context_services = context_services
        self.memory = memory
        # 记忆 DI 缝:runtime 只认识 MemoryServices.engine,不认识具体实现。
        self.memory_services = memory_services
        self.tools = tools
        self.reasoner = reasoner
        self.config = config
        self.snapshot_store = snapshot_store
        # 在途 turn 持有各插件当前代际的租约,热重载只换代不抽走能力。
        self.plugin_generations = plugin_generations
        self._before_turn_modules: List[object] = []
        self._before_reasoning_modules: List[object] = []
        self._after_reasoning_modules: List[object] = []
        self._after_turn_modules: List[object] = []

    @property
    def session_manager(self) -> TranscriptStore:
        """pipeline 通过服务包拿 session,换实现不必改这里的调用点。"""
        return self.session_services.session_manager

    def add_before_turn_plugin_modules(self, modules: List[object]) -> None:
        self._before_turn_modules.extend(modules)

    def add_before_reasoning_plugin_modules(self, modules: List[object]) -> None:
        self._before_reasoning_modules.extend(modules)

    def add_after_reasoning_plugin_modules(self, modules: List[object]) -> None:
        self._after_reasoning_modules.extend(modules)

    def add_after_turn_plugin_modules(self, modules: List[object]) -> None:
        self._after_turn_modules.extend(modules)

    @asynccontextmanager
    async def _plugin_generation_lease(self):
        """未接插件代际注册表时是空操作,便于测试与最小构造。"""
        registry = self.plugin_generations
        if registry is None:
            yield ()
            return
        async with registry.lease_committed() as leased:
            yield leased

    async def run(self, msg: InboundMessage, key: str, *, dispatch_outbound: bool = True) -> OutboundMessage:
        """整个 turn 锁定一份能力快照，热重载不会在 turn 中途抽走工具。"""
        started_at = datetime.now().astimezone()
        started_clock = time.perf_counter()
        try:
            async with self._plugin_generation_lease():
                if self.snapshot_store is None or self.snapshot_store.current is None:
                    outbound = await self._run_turn(
                        msg, key, dispatch_outbound=dispatch_outbound
                    )
                else:
                    lease = self.snapshot_store.lease()
                    token = bind_runtime_snapshot(lease)
                    try:
                        outbound = await self._run_turn(
                            msg, key, dispatch_outbound=dispatch_outbound
                        )
                    finally:
                        reset_runtime_snapshot(token)
                        await lease.release()
        except asyncio.CancelledError:
            await self._finish_turn(
                msg,
                key,
                status="interrupted",
                started_at=started_at,
                started_clock=started_clock,
                dispatch_outbound=dispatch_outbound,
            )
            raise
        except Exception as exc:
            await self._finish_turn(
                msg,
                key,
                status="error",
                started_at=started_at,
                started_clock=started_clock,
                dispatch_outbound=dispatch_outbound,
                error=str(exc),
            )
            raise
        await self._finish_turn(
            msg,
            key,
            status="success",
            started_at=started_at,
            started_clock=started_clock,
            dispatch_outbound=dispatch_outbound,
            outbound=outbound,
        )
        return outbound

    async def execute(self, request: TurnRequest) -> TurnResult:
        """Run through the agent core without requiring or dispatching a channel.

        ``run`` remains the local MessageBus compatibility entrypoint. Cloud/API
        callers use this method and identify the product conversation directly.
        """
        origin = request.origin
        inbound = InboundMessage(
            channel=origin.name,
            sender=request.principal.subject_id,
            chat_id=origin.external_thread_id,
            content=request.content,
            timestamp=request.submitted_at,
            media=list(request.media),
            metadata={
                **dict(request.metadata),
                "session_key_override": request.conversation_id,
                "context_channel": request.memory_scope.namespace,
                "context_chat_id": request.memory_scope.subject_id,
                "principal_id": request.principal.subject_id,
                "principal_kind": request.principal.kind,
                "turn_origin_kind": request.origin.kind,
            },
        )
        outbound = await self.run(
            inbound,
            request.conversation_id,
            dispatch_outbound=False,
        )
        return TurnResult(
            conversation_id=request.conversation_id,
            content=outbound.content,
            thinking=outbound.thinking,
            media=tuple(outbound.media),
            metadata=dict(outbound.metadata),
        )

    async def _finish_turn(
        self,
        msg: InboundMessage,
        key: str,
        *,
        status: str,
        started_at: datetime,
        started_clock: float,
        dispatch_outbound: bool,
        outbound: OutboundMessage | None = None,
        error: str = "",
    ) -> None:
        await self.event_bus.fanout(
            TurnFinished(
                session_key=key,
                channel=msg.channel,
                chat_id=msg.chat_id,
                status=status,
                started_at=started_at,
                finished_at=datetime.now().astimezone(),
                duration_seconds=max(0.0, time.perf_counter() - started_clock),
                will_dispatch=dispatch_outbound,
                outbound=outbound,
                error=error,
            )
        )

    async def _run_turn(self, msg: InboundMessage, key: str, *, dispatch_outbound: bool = True) -> OutboundMessage:
        session = self.session_manager.get_or_create(key)
        principal_id = str(msg.metadata.get("principal_id") or "").strip()
        if principal_id:
            session.metadata["principal_id"] = principal_id
        state = TurnState(msg=msg, session_key=key, dispatch_outbound=dispatch_outbound, session=session)
        await self.event_bus.fanout(
            TurnStarted(
                session_key=key,
                channel=msg.channel,
                chat_id=msg.chat_id,
                content=msg.content,
                timestamp=msg.timestamp,
            )
        )
        before_turn: BeforeTurnCtx | None = None
        if msg.content.lstrip().startswith("/"):
            before_turn = BeforeTurnCtx(
                session_key=key,
                channel=msg.channel,
                chat_id=msg.chat_id,
                content=msg.content,
                timestamp=msg.timestamp,
                retrieved_memory_block="",
                history_messages=(),
                skill_names=self._collect_skill_mentions(msg.content),
            )
            before_turn = await self.event_bus.emit(before_turn)
            before_turn = await _run_plugin_modules(
                _turn_phase_modules(self._before_turn_modules, "before_turn_modules"),
                before_turn,
            )
            state.extra_metadata.update(before_turn.extra_metadata)
            if before_turn.abort:
                return await self._dispatch_if_needed(state, before_turn.abort_reply)
            core_reply = self._core_command(msg.content)
            if core_reply is not None:
                return await self._dispatch_if_needed(state, core_reply)

        # 等上一轮归档收口再读历史:归档会推进 last_consolidated,没等会读到错位窗口。
        maintenance = self._markdown_maintenance()
        if maintenance is not None:
            await maintenance.wait_for_session(key)
        guard_reply = await self._guard_memory_context(session, key)
        if guard_reply:
            return await self._dispatch_if_needed(state, guard_reply)
        history = session.get_history(
            max_messages=self.config.history_window,
            start_index=session.last_consolidated,
        )
        retrieved = ""
        if not msg.metadata.get("skip_memory_retrieval"):
            engine = (
                self.memory_services.engine if self.memory_services is not None else None
            )
            if engine is not None and _engine_can_retrieve(engine):
                # Phase 2:检索走 DI 服务包里的引擎(异步原生,无 to_thread)。
                result = await engine.query(
                    _CoreMemQuery(
                        text=msg.content,
                        intent="context",
                        scope=_CoreMemScope(
                            session_key=key,
                            channel=msg.context_channel,
                            chat_id=msg.context_chat_id,
                            user_id=principal_id,
                        ),
                        # timestamp 对 DefaultMemoryEngine 不承重,但 akasha 用它
                        # 算图激活的时间衰减——不传会直接返回 missing_query_timestamp。
                        # 照 Reference `agent/retrieval/default_pipeline.py` 一并传。
                        timestamp=msg.timestamp,
                    )
                )
                retrieved = result.text_block
                state.extra_metadata["retrieval_trace"] = {
                    "engine": result.trace.get("engine"),
                    "intent": "context",
                    "records": len(result.records),
                }
                # 检索回放:记下"召回了什么、注入了没有",供 Dashboard 逐轮回看。
                # 观测失败绝不能影响回复,所以整段吞异常。
                if self.recall_inspector is not None:
                    try:
                        self.recall_inspector.record_context_prepare(
                            session_key=key,
                            channel=msg.context_channel,
                            chat_id=msg.context_chat_id,
                            user_text=msg.content,
                            timestamp=msg.timestamp.isoformat(),
                            records=list(result.records),
                            text_block=retrieved,
                            trace=dict(result.trace or {}),
                        )
                    except Exception:  # noqa: BLE001
                        logger.warning("[observe] 检索回放记录失败", exc_info=True)
            else:
                retrieval_result = await asyncio.to_thread(
                    self.memory.retrieve,
                    RetrievalRequest(
                        query=msg.content,
                        session_key=key,
                        channel=msg.context_channel,
                        chat_id=msg.context_chat_id,
                        history=session.get_history(
                            max_messages=max(1, len(session.messages))
                        ),
                        session_metadata=dict(session.metadata),
                        timestamp=msg.timestamp,
                    ),
                )
                retrieved = retrieval_result.block if retrieval_result is not None else ""
                if retrieval_result is not None and retrieval_result.trace is not None:
                    trace = retrieval_result.trace
                    state.extra_metadata["retrieval_trace"] = {
                        "lanes": dict(trace.lanes),
                        "fused": trace.fused,
                        "injected": trace.injected,
                        "used_vector": trace.used_vector,
                        "truncated": trace.truncated,
                    }
        skill_mentions = self._collect_skill_mentions(msg.content)
        if before_turn is None:
            before_turn = BeforeTurnCtx(
                session_key=key,
                channel=msg.channel,
                chat_id=msg.chat_id,
                content=msg.content,
                timestamp=msg.timestamp,
                retrieved_memory_block=retrieved,
                history_messages=tuple(history),
                skill_names=skill_mentions,
            )
            before_turn = await self.event_bus.emit(before_turn)
            before_turn = await _run_plugin_modules(
                _turn_phase_modules(self._before_turn_modules, "before_turn_modules"),
                before_turn,
            )
            state.extra_metadata.update(before_turn.extra_metadata)
            if before_turn.abort:
                return await self._dispatch_if_needed(state, before_turn.abort_reply)
        else:
            before_turn.history_messages = tuple(history)
            if retrieved:
                before_turn.retrieved_memory_block = "\n\n".join(
                    part
                    for part in (before_turn.retrieved_memory_block, retrieved)
                    if part
                )

        context_token = self.tools.set_context(
            session_key=key,
            channel=msg.channel,
            chat_id=msg.chat_id,
            principal_id=principal_id,
            current_timestamp=msg.timestamp.isoformat(),
        )
        try:
            before_reasoning = BeforeReasoningCtx(
                session_key=key,
                channel=msg.context_channel,
                chat_id=msg.context_chat_id,
                content=msg.content,
                timestamp=msg.timestamp,
                skill_names=list(before_turn.skill_names),
                retrieved_memory_block=before_turn.retrieved_memory_block,
                extra_hints=list(before_turn.extra_hints),
            )
            before_reasoning = await self.event_bus.emit(before_reasoning)
            before_reasoning = await _run_plugin_modules(
                _turn_phase_modules(
                    self._before_reasoning_modules, "before_reasoning_modules"
                ),
                before_reasoning,
            )
            if before_reasoning.abort:
                return await self._dispatch_if_needed(state, before_reasoning.abort_reply)

            turn = await self.reasoner.run_turn(
                msg=msg,
                session_key=key,
                history=list(before_turn.history_messages),
                retrieved_memory_block=before_reasoning.retrieved_memory_block,
                skill_names=before_reasoning.skill_names,
                extra_hints=before_reasoning.extra_hints,
                disabled_tools=self._disabled_tools(msg),
            )
            state.extra_metadata["context_trace"] = dict(turn.context_trace)
            if turn.react_compaction is not None:
                state.extra_metadata["react_compaction"] = dict(
                    turn.react_compaction
                )
        finally:
            self.tools.reset_context(context_token)
        # 入站 metadata 里的 mobile_attention 一律丢弃,只认本轮工具自己声明的
        # (照 Reference lifecycle/phases/after_reasoning.py:68)——否则渠道客户端
        # 可以伪造"需要确认"标记。
        inbound_metadata = dict(msg.metadata or {})
        inbound_metadata.pop("mobile_attention", None)
        after_ctx = AfterReasoningCtx(
            session_key=key,
            channel=msg.channel,
            chat_id=msg.chat_id,
            tools_used=tuple(turn.tools_used),
            thinking=turn.thinking,
            tool_chain=tuple(turn.tool_chain),
            reply=turn.reply,
            # 照 Reference lifecycle/phases/after_reasoning.py:把本轮做了什么带出去。
            # 控制面据此投影 toolCall item,Web 端据此渲染工具时间线。
            outbound_metadata={
                **inbound_metadata,
                "tools_used": list(turn.tools_used),
                "tool_chain": list(turn.tool_chain),
                **(
                    {"mobile_attention": turn.mobile_attention}
                    if turn.mobile_attention is not None
                    else {}
                ),
            },
        )
        after_ctx = await self.event_bus.emit(after_ctx)
        after_ctx = await _run_plugin_modules(
            _turn_phase_modules(
                self._after_reasoning_modules, "after_reasoning_modules"
            ),
            after_ctx,
        )
        outbound_metadata = dict(after_ctx.outbound_metadata)
        correlation_id = str(msg.metadata.get("client_request_id") or "").strip()
        if correlation_id:
            outbound_metadata["client_request_id"] = correlation_id
        outbound = OutboundMessage(
            channel=msg.channel,
            chat_id=msg.chat_id,
            content=after_ctx.reply,
            thinking=after_ctx.thinking,
            media=list(after_ctx.media),
            metadata=outbound_metadata,
        )
        result = AfterReasoningResult(ctx=after_ctx, outbound=outbound)
        return await self._commit_and_dispatch(state, result)

    def _markdown_maintenance(self):
        """承重的 markdown 维护器;未接服务包时返回 None,回退旧 consolidation 路径。"""
        services = self.memory_services
        markdown = getattr(services, "markdown", None) if services is not None else None
        maintenance = getattr(markdown, "maintenance", None)
        # 未绑定 session 生命周期的维护器不能驱动归档。
        if maintenance is None or getattr(maintenance, "_get_session", None) is None:
            return None
        return maintenance

    async def _guard_memory_context(self, session: Any, session_key: str) -> str:
        total = len(session.messages)
        last = max(0, min(int(session.last_consolidated or 0), total))
        pending = total - last
        minimum_new = max(5, self.config.history_window // 2)
        threshold = self.config.history_window + minimum_new
        if pending < threshold:
            return ""
        maintenance = self._markdown_maintenance()
        if maintenance is None:
            # 没有记忆服务包 = 没有归档能力(最小构造/测试)。此时既无法推进也无从等待,
            # 拒绝每一轮只会让最小构造不可用,因此放行。生产始终有维护器,保护完整生效。
            return ""
        before = int(session.last_consolidated or 0)
        # 强制归档并等待:与队列维护共用同一把 session 锁,不会互相插队。
        from core.memory.markdown import ConsolidateRequest

        try:
            await maintenance.consolidate(
                ConsolidateRequest(
                    session=session,
                    force=True,
                    scope_channel=str(session.metadata.get("channel") or ""),
                    scope_chat_id=str(session.metadata.get("chat_id") or ""),
                )
            )
        except Exception:
            logger.exception("guard consolidation failed: %s", session_key)
        else:
            if int(session.last_consolidated or 0) > before:
                await self.session_manager.save_async(session)
        if int(session.last_consolidated or 0) > before:
            return ""
        return (
            "当前会话尚未归档的上下文已达到安全阈值，自动 consolidation 未能推进。"
            "为避免静默丢失历史，本轮已停止；请稍后重试或新建会话。"
        )

    async def _commit_and_dispatch(self, state: TurnState, result: AfterReasoningResult) -> OutboundMessage:
        session = state.session
        msg = state.msg
        if not msg.metadata.get("omit_user_turn"):
            session.add_message(
                "user",
                msg.content,
                media=msg.media,
                inbound_timestamp=msg.timestamp.isoformat(),
            )
        session.add_message(
            "assistant",
            result.outbound.content,
            media=result.outbound.media,
            reasoning_content=result.outbound.thinking,
            tools_used=list(result.ctx.tools_used),
            tool_chain=list(result.ctx.tool_chain),
            **state.extra_metadata,
        )
        session.metadata.update(
            {
                "channel": msg.channel,
                "chat_id": msg.chat_id,
                "last_sender": msg.sender,
                "last_turn_at": msg.timestamp.isoformat(),
                "turn_count": int(session.metadata.get("turn_count") or 0) + 1,
                "tool_call_count": int(
                    session.metadata.get("tool_call_count") or 0
                )
                + sum(
                    len(group.get("calls") or [])
                    for group in result.ctx.tool_chain
                ),
            }
        )
        if msg.metadata.get("username"):
            session.metadata["username"] = str(msg.metadata["username"])
        context_trace = dict(state.extra_metadata.get("context_trace") or {})
        attempts = list(context_trace.get("attempts") or [])
        selected_plan = str(context_trace.get("selected_plan") or "")
        selected_attempt = next(
            (
                item
                for item in reversed(attempts)
                if str(item.get("name") or "") == selected_plan
            ),
            attempts[-1] if attempts else {},
        )
        history = session.get_history(
            max_messages=self.config.history_window,
            start_index=session.last_consolidated,
        )
        post_reply_budget = {
            "history_messages": len(history),
            "history_tokens_estimate": estimate_context_tokens(history, []),
            "estimate_quality": "approximate",
            "selected_plan": selected_plan,
            "last_prompt_tokens_estimate": int(
                selected_attempt.get("estimated_tokens") or 0
            ),
            "input_budget": int(selected_attempt.get("input_budget") or 0),
            "model_usage": dict(
                (context_trace.get("react_stats") or {}).get("model_usage") or {}
            ),
        }
        session.metadata["context_budget"] = post_reply_budget
        self.session_manager.save(session)
        await self.event_bus.fanout(
            ContextBudgetUpdated(
                session_key=state.session_key,
                channel=msg.channel,
                chat_id=msg.chat_id,
                **post_reply_budget,
            )
        )
        committed = TurnCommitted(
            session_key=state.session_key,
            channel=msg.channel,
            chat_id=msg.chat_id,
            user_content=msg.content,
            assistant_reply=result.outbound.content,
            tools_used=result.ctx.tools_used,
            extra={"principal_id": str(msg.metadata["principal_id"])}
            if msg.metadata.get("principal_id")
            else {},
        )
        await self.event_bus.fanout(committed)
        after_turn = AfterTurnCtx(
            session_key=state.session_key,
            channel=msg.channel,
            chat_id=msg.chat_id,
            reply=result.outbound.content,
            tools_used=result.ctx.tools_used,
            thinking=result.outbound.thinking,
            will_dispatch=state.dispatch_outbound,
            extra_metadata=dict(state.extra_metadata),
        )
        await self.event_bus.fanout(after_turn)
        await _run_plugin_modules(
            _turn_phase_modules(self._after_turn_modules, "after_turn_modules"),
            after_turn,
        )
        if state.dispatch_outbound:
            await self.bus.publish_outbound(result.outbound)
        return result.outbound

    async def _dispatch_if_needed(self, state: TurnState, content: str) -> OutboundMessage:
        metadata = {}
        correlation_id = str(
            state.msg.metadata.get("client_request_id") or ""
        ).strip()
        if correlation_id:
            metadata["client_request_id"] = correlation_id
        outbound = OutboundMessage(
            channel=state.msg.channel,
            chat_id=state.msg.chat_id,
            content=content,
            metadata=metadata,
        )
        if state.dispatch_outbound:
            await self.bus.publish_outbound(outbound)
        return outbound

    def _collect_skill_mentions(self, content: str) -> List[str]:
        names = set(self.reasoner.context.skills.names())
        result: List[str] = []
        for name in re.findall(r"\$([a-zA-Z0-9_:-]+)", content):
            if name in names and name not in result:
                result.append(name)
        return result

    def _core_command(self, content: str) -> str | None:
        command = content.strip().lower()
        if command == "/tools":
            # MCP 工具只挂在快照上，要连同当前代际一起列出，否则用户会以为没接上。
            snapshot = get_current_runtime_snapshot() or (
                self.snapshot_store.current if self.snapshot_store is not None else None
            )
            return "\n".join(SnapshotToolView(self.tools, snapshot).names())
        if command == "/skills":
            self.reasoner.context.skills.reload()
            return self.reasoner.context.skills.descriptions()
        if command == "/memory":
            records = self.memory.list_records()
            return json.dumps(
                {"active_count": len(records), "recent": records[:10]},
                ensure_ascii=False,
                indent=2,
            )
        return None

    @staticmethod
    def _disabled_tools(msg: InboundMessage) -> set[str]:
        raw = msg.metadata.get("disabled_tools")
        if isinstance(raw, str):
            return {raw} if raw else set()
        if isinstance(raw, (list, tuple, set)):
            return {str(item) for item in raw if str(item)}
        return set()


class AgentLoop:
    def __init__(self, *, bus: MessageBus, pipeline: PassiveTurnPipeline) -> None:
        self.bus = bus
        self.pipeline = pipeline
        self._running = False
        self._active_tasks: Dict[str, asyncio.Task[Any]] = {}
        self._tasks: set[asyncio.Task[Any]] = set()
        self._session_locks: Dict[str, asyncio.Lock] = {}
        self._turn_snapshots: Dict[str, AfterStepCtx] = {}
        event_bus = getattr(self.pipeline, "event_bus", None)
        if event_bus is not None:
            event_bus.on(AfterStepCtx, self._capture_after_step)

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                item = await asyncio.wait_for(self.bus.consume_inbound(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            key = item.session_key
            task = asyncio.create_task(
                self._process_item(item, key), name="turn:%s" % key
            )
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _process_item(self, item: InboundMessage, key: str) -> None:
        lock = self._session_locks.setdefault(key, asyncio.Lock())
        try:
            async with lock:
                task = asyncio.current_task()
                if task is not None:
                    self._active_tasks[key] = task
                admission_owner: str | None = None
                session_manager = getattr(self.pipeline, "session_manager", None)
                try:
                    if session_manager is not None and hasattr(
                        session_manager, "acquire_admission"
                    ):
                        admission_owner = session_manager.acquire_admission(key)
                    await self.pipeline.run(item, key)
                except asyncio.CancelledError:
                    logger.info("turn cancelled for session=%s", key)
                    self._persist_interrupted_turn(key, item)
                    raise
                except Exception as exc:
                    logger.exception("failed to process inbound")
                    metadata = {}
                    correlation_id = str(
                        item.metadata.get("client_request_id") or ""
                    ).strip()
                    if correlation_id:
                        metadata["client_request_id"] = correlation_id
                    await self.bus.publish_outbound(
                        OutboundMessage(
                            item.channel,
                            item.chat_id,
                            "出错：%s" % exc,
                            metadata=metadata,
                        )
                    )
                finally:
                    if admission_owner is not None:
                        try:
                            session_manager.release_admission(key, admission_owner)
                        except Exception:
                            logger.exception(
                                "failed to release session admission: %s", key
                            )
                    if self._active_tasks.get(key) is task:
                        self._active_tasks.pop(key, None)
                    self._turn_snapshots.pop(key, None)
        finally:
            await self.bus.complete_inbound(item)
            current = asyncio.current_task()
            if not lock.locked() and not any(
                task is not current
                and not task.done()
                and task.get_name() == "turn:%s" % key
                for task in self._tasks
            ):
                self._session_locks.pop(key, None)

    def stop(self) -> None:
        self._running = False

    def request_interrupt(self, session_key: str) -> bool:
        task = self._active_tasks.get(session_key)
        if task is None or task.done():
            return False
        task.cancel()
        return True

    def is_busy(self, session_key: str) -> bool:
        """该 session 是否正在处理一轮被动 turn。供主动链路避让。"""
        task = self._active_tasks.get(session_key)
        if task is not None and not task.done():
            return True
        lock = self._session_locks.get(session_key)
        return bool(lock is not None and lock.locked())

    async def shutdown(self) -> None:
        self.stop()
        pending = [task for task in self._tasks if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    def _capture_after_step(self, event: AfterStepCtx) -> None:
        self._turn_snapshots[event.session_key] = event

    def _persist_interrupted_turn(
        self, session_key: str, item: InboundMessage
    ) -> None:
        if item.metadata.get("omit_user_turn"):
            return
        session = self.pipeline.session_manager.get_or_create(session_key)
        inbound_timestamp = item.timestamp.isoformat()
        if len(session.messages) >= 2:
            possible_user = session.messages[-2]
            possible_assistant = session.messages[-1]
            if (
                possible_user.get("role") == "user"
                and possible_user.get("inbound_timestamp") == inbound_timestamp
                and possible_assistant.get("role") == "assistant"
            ):
                self.pipeline.session_manager.save(session)
                return
        session.add_message(
            "user",
            item.content,
            media=item.media,
            inbound_timestamp=inbound_timestamp,
        )
        snapshot = self._turn_snapshots.get(session_key)
        session.add_message(
            "assistant",
            "[interrupted]",
            tools_used=list(snapshot.tools_used_so_far) if snapshot else [],
            tool_chain=list(snapshot.tool_chain_partial) if snapshot else [],
            reasoning_content=snapshot.partial_thinking if snapshot else "",
            partial_reply=snapshot.partial_reply if snapshot else "",
            interrupted=True,
        )
        self.pipeline.session_manager.save(session)


@dataclass
class CoreRuntime:
    bus: MessageBus
    event_bus: EventBus
    session_manager: SessionManager
    memory: MemoryRuntime
    tools: ToolRegistry
    context: ContextBuilder
    reasoner: DefaultReasoner
    pipeline: PassiveTurnPipeline
    loop: AgentLoop
    channel_host: ChannelHost | None = None
    plugin_manager: Any | None = None
    mcp_watcher: Any | None = None
    plugin_watcher: Any | None = None
    memory_services: Any = None
    scheduler: Any | None = None
    subagents: Any | None = None
    proactive_loop: Any | None = None
    drift_runner: Any | None = None
    control_store: Any | None = None
    control_runtime: Any | None = None
    control_service: Any | None = None
    control_server: Any | None = None
    # supervisor 托管时非空;runtime_serve 等待它的 committed 事件并以 75 退出换代。
    restart_coordinator: Any | None = None

    def add_tool_hooks(self, hooks: List[ToolHook]) -> None:
        self.reasoner.add_tool_hooks(hooks)

    async def process_direct(
        self,
        content: str,
        *,
        session_key: str = "direct:local",
        channel: str = "direct",
        chat_id: str = "local",
        omit_user_turn: bool = False,
        skip_post_memory: bool = False,
        skip_memory_retrieval: bool = False,
        disabled_tools: List[str] | None = None,
    ) -> OutboundMessage:
        metadata: JsonDict = {
            "session_key_override": session_key,
            "omit_user_turn": omit_user_turn,
            "skip_post_memory": skip_post_memory,
            "skip_memory_retrieval": skip_memory_retrieval,
        }
        if disabled_tools:
            metadata["disabled_tools"] = list(disabled_tools)
        return await self.pipeline.run(
            InboundMessage(
                channel=channel,
                sender="direct_user",
                chat_id=chat_id,
                content=content,
                metadata=metadata,
            ),
            session_key,
            dispatch_outbound=False,
        )

    async def start_background(
        self, *, start_channels: bool = True
    ) -> list[asyncio.Task[Any]]:
        tasks = [
            asyncio.create_task(self.loop.run(), name="agent_loop"),
            asyncio.create_task(self.bus.dispatch_outbound(), name="bus_dispatch"),
        ]
        if start_channels and self.channel_host is not None:
            await self.channel_host.start_all()
        if self.scheduler is not None:
            tasks.append(asyncio.create_task(self.scheduler.run(), name="scheduler"))
        if self.proactive_loop is not None:
            tasks.append(
                asyncio.create_task(self.proactive_loop.run(), name="proactive_loop")
            )
        if self.mcp_watcher is not None:
            tasks.append(
                asyncio.create_task(self.mcp_watcher.run(), name="workspace_mcp_watcher")
            )
        if self.plugin_watcher is not None:
            tasks.append(
                asyncio.create_task(self.plugin_watcher.run(), name="plugin_watcher")
            )
        if self.control_server is not None:
            # 控制面自己管连接 task,不进 tasks 列表;失败不能拖垮主链路。
            try:
                await self.control_server.start()
            except Exception:
                logger.exception("control plane failed to start; continuing without it")
                self.control_server = None
        return tasks

    async def stop_background(self, tasks: list[asyncio.Task[Any]]) -> None:
        # 先停控制面:不再接受新的 programmatic turn,在途 turn 由
        # ConversationRuntime.shutdown 取消并写入终态。
        if self.control_server is not None:
            await self.control_server.stop()
        if self.control_service is not None:
            await self.control_service.shutdown()
        if self.control_runtime is not None:
            await self.control_runtime.shutdown()
        if self.subagents is not None:
            await self.subagents.shutdown()
        await self.loop.shutdown()
        if self.scheduler is not None:
            self.scheduler.stop()
        if self.proactive_loop is not None:
            self.proactive_loop.stop()
        if self.mcp_watcher is not None:
            self.mcp_watcher.stop()
        if self.plugin_watcher is not None:
            self.plugin_watcher.stop()
        drained = await self.bus.drain(timeout=10.0)
        if not drained:
            logger.warning("outbound queue did not drain before shutdown")
        self.bus.stop()
        await self.bus.shutdown()
        if self.channel_host is not None:
            await self.channel_host.stop_all()
        await self.tools.shutdown()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self.plugin_manager is not None:
            await self.plugin_manager.terminate_all()
        if self.mcp_watcher is not None:
            await self.mcp_watcher.shutdown()
        if self.drift_runner is not None:
            self.drift_runner.close()
        if self.proactive_loop is not None:
            self.proactive_loop.close()
        await self.memory.shutdown()
        # 引擎持有 coremem.db 与 embedder,必须在旧 MemoryRuntime 之后关闭。
        if self.memory_services is not None:
            try:
                await self.memory_services.aclose()
            except Exception:
                logger.exception("memory services shutdown failed")
        await self.event_bus.shutdown()
        if self.control_store is not None:
            self.control_store.close()
        self.session_manager.close()
