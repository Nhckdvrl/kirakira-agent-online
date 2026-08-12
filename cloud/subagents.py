"""Durable multi-tenant implementation of the original spawn/spawn_manage tools."""

from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager, AbstractContextManager, ExitStack
from datetime import UTC, datetime
import json
from typing import Callable, Sequence
from uuid import UUID

from agent.core.runtime import DefaultReasoner
from agent.plugins.snapshot import (
    bind_runtime_snapshot,
    get_current_runtime_lease,
    reset_runtime_snapshot,
)
from agent.subagent import SubagentManager
from agent.tools.registry import ToolRegistry, object_schema
from bus.events import InboundMessage
from cloud.logging import safe_exception_summary
from cloud.store import CloudStore, StoreStateError
from core.memory.engine import MemoryQuery, MemoryScope
from core.schema import ToolSpec


class CloudSubagentRuntime:
    def __init__(
        self,
        *,
        store: CloudStore,
        reasoner: DefaultReasoner,
        tools: ToolRegistry,
        memory_engine: object,
        worker_id: str,
        scope_binders: Sequence[Callable[[str], AbstractContextManager[object]]] = (),
        capability_scope: Callable[[str], AbstractAsyncContextManager[None]] | None = None,
    ) -> None:
        self.store = store
        self.reasoner = reasoner
        self.tools = tools
        self.memory_engine = memory_engine
        self.worker_id = worker_id[:200]
        self.scope_binders = tuple(scope_binders)
        self.capability_scope = capability_scope
        self._stop = asyncio.Event()

    def register_tools(self) -> None:
        self.tools.register(
            ToolSpec(
                "spawn",
                "Delegate a bounded task to an isolated inline or durable background subagent.",
                object_schema(
                    {
                        "task": {"type": "string"},
                        "max_iterations": {"type": "integer"},
                        "mode": {"type": "string", "enum": ["inline", "background"]},
                        "label": {"type": "string"},
                        "profile": {
                            "type": "string",
                            "enum": ["research", "scripting", "general"],
                        },
                    },
                    ["task"],
                ),
            ),
            self.spawn,
            risk="external-side-effect",
        )
        self.tools.register(
            ToolSpec(
                "spawn_manage",
                "List or cancel this user's durable background subagent tasks.",
                object_schema(
                    {
                        "action": {"type": "string", "enum": ["list", "cancel"]},
                        "task_id": {"type": "string"},
                    },
                    ["action"],
                ),
            ),
            self.manage,
            deferred=True,
            risk="write",
        )

    def _identity(self) -> tuple[UUID, UUID]:
        context = self.tools.context
        try:
            return UUID(str(context["principal_id"])), UUID(str(context["chat_id"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("Cloud subagent requires user and conversation context") from exc

    async def spawn(
        self,
        task: str,
        max_iterations: int = 8,
        mode: str = "inline",
        label: str = "",
        profile: str = "research",
    ) -> str:
        task = task.strip()
        if not task:
            return "Error: subagent task is empty"
        if mode not in {"inline", "background"}:
            return "Error: mode must be inline or background"
        if profile not in {"research", "scripting", "general"}:
            return "Error: profile must be research, scripting, or general"
        user_id, conversation_id = self._identity()
        try:
            job = await self.store.create_subagent_job(
                user_id,
                conversation_id,
                task=task,
                label=label.strip() or task[:30],
                profile=profile,
                max_iterations=max(1, min(20, int(max_iterations))),
            )
        except StoreStateError as exc:
            return f"Error: {exc}"
        if mode == "background":
            return json.dumps(
                {
                    "task_id": job.id,
                    "status": "queued",
                    "mode": "background",
                    "label": job.label,
                    "profile": profile,
                    "message": "完成后会持久化回注当前会话。",
                },
                ensure_ascii=False,
            )
        claimed = await self.store.claim_subagent_job(job.id, self.worker_id)
        status, result, metadata = await self._execute(claimed)
        await self.store.finish_subagent_job(
            job.id,
            self.worker_id,
            status=status,
            result=result,
            metadata=metadata,
            deliver=False,
        )
        return json.dumps(
            {
                "task_id": job.id,
                "status": status,
                "result": result,
                "tools_used": metadata.get("tools_used", []),
            },
            ensure_ascii=False,
        )

    async def manage(self, action: str, task_id: str = "") -> str:
        user_id, _ = self._identity()
        if action == "list":
            jobs = await self.store.list_subagent_jobs(user_id)
            return json.dumps(
                {
                    "running_count": sum(item.status in {"queued", "running"} for item in jobs),
                    "jobs": [
                        {
                            "task_id": item.id,
                            "label": item.label,
                            "profile": item.profile,
                            "status": item.status,
                            "started_at": item.started_at.isoformat() if item.started_at else "",
                        }
                        for item in jobs
                    ],
                },
                ensure_ascii=False,
            )
        if action == "cancel" and task_id.strip():
            job = await self.store.cancel_subagent_job(user_id, task_id.strip())
            return json.dumps({"task_id": job.id, "status": job.status}, ensure_ascii=False)
        return 'Error: action must be list or cancel; cancel requires task_id'

    async def run_once(self) -> bool:
        job = await self.store.claim_next_subagent_job(self.worker_id)
        if job is None:
            return False
        status = "failed"
        result = ""
        metadata: dict = {}
        try:
            if self.capability_scope is not None:
                async with self.capability_scope(str(job.user_id)):
                    status, result, metadata = await self._execute_bound(job)
            else:
                status, result, metadata = await self._execute_bound(job)
        except Exception as exc:
            result = safe_exception_summary(exc)
        await self.store.finish_subagent_job(
            job.id,
            self.worker_id,
            status=status,
            result=result,
            metadata=metadata,
            deliver=True,
        )
        return True

    async def _execute_bound(self, job) -> tuple[str, str, dict]:
        with ExitStack() as stack:
            for binder in self.scope_binders:
                stack.enter_context(binder(str(job.user_id)))
            parent = get_current_runtime_lease()

            async def execute_with_snapshot():
                if parent is None:
                    return await self._execute(job)
                child = parent.fork()
                token = bind_runtime_snapshot(child)
                try:
                    return await self._execute(job)
                finally:
                    reset_runtime_snapshot(token)
                    await child.release()

            execution = asyncio.create_task(
                execute_with_snapshot(), name=f"subagent:{job.id}"
            )
            try:
                while True:
                    done, _ = await asyncio.wait({execution}, timeout=5)
                    if done:
                        return await execution
                    if await self.store.heartbeat_subagent_job(job.id, self.worker_id):
                        execution.cancel()
                        await asyncio.gather(execution, return_exceptions=True)
                        return "cancelled", "后台子任务已由用户取消。", {}
            finally:
                if not execution.done():
                    execution.cancel()
                    await asyncio.gather(execution, return_exceptions=True)

    async def _execute(self, job) -> tuple[str, str, dict]:
        child_key = f"{job.conversation_id}:{job.id}"
        now = datetime.now(UTC)
        query = await self.memory_engine.query(
            MemoryQuery(
                text=job.task,
                intent="context",
                scope=MemoryScope(
                    session_key=str(job.conversation_id),
                    channel="cloud",
                    chat_id=str(job.conversation_id),
                    user_id=str(job.user_id),
                ),
                timestamp=now,
            )
        )
        token = self.tools.set_context(
            session_key=child_key,
            channel="subagent",
            chat_id=str(job.conversation_id),
            principal_id=str(job.user_id),
            current_timestamp=now.isoformat(),
            parent_session_key=str(job.conversation_id),
            subagent_profile=job.profile,
        )
        try:
            result = await self.reasoner.run_turn(
                msg=InboundMessage(
                    channel="subagent",
                    sender="parent_agent",
                    chat_id=job.id,
                    content=(
                        "你是主 Agent 派生的子代理。只完成下面这个边界明确的任务，"
                        "可以使用工具核实和执行；最后返回简洁、可验证的结果。\n\n任务："
                        + job.task
                    ),
                    timestamp=now,
                ),
                session_key=child_key,
                history=[],
                retrieved_memory_block=query.text_block,
                skill_names=[],
                extra_hints=["这是子代理任务，不要调用 spawn 创建更多子代理。"],
                disabled_tools=SubagentManager._disabled_tools(job.profile),
                max_iterations_override=job.max_iterations,
            )
            return (
                "completed",
                result.reply[:100_000],
                {
                    "tools_used": list(result.tools_used),
                    "tool_chain": list(result.tool_chain),
                    "thinking": result.thinking,
                    "profile": job.profile,
                },
            )
        finally:
            try:
                await self.tools.cleanup_owner(child_key)
            finally:
                self.tools.reset_context(token)

    async def run_forever(self) -> None:
        while not self._stop.is_set():
            if not await self.run_once():
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=1)
                except TimeoutError:
                    pass

    def stop(self) -> None:
        self._stop.set()
