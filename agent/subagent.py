"""User-turn-triggered isolated subagent execution."""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Any
from uuid import uuid4

from bus.events import InboundMessage
from bus.queue import MessageBus
from core.memory.legacy import MemoryRuntime
from agent.core.runtime import DefaultReasoner
from core.schema import ToolSpec
from session.manager import SessionManager
from agent.tools.registry import ToolRegistry, object_schema


_MAX_BACKGROUND_JOBS = 3
_RESULT_LIMIT = 100_000


@dataclass(frozen=True)
class RunningSubagentJob:
    task_id: str
    label: str
    task: str
    profile: str
    parent_session: str
    parent_channel: str
    parent_chat_id: str
    task_dir: str
    started_at: str
    status: str = "running"


class SubagentManager:
    def __init__(
        self,
        *,
        reasoner: DefaultReasoner,
        tools: ToolRegistry,
        sessions: SessionManager,
        memory: MemoryRuntime,
        bus: MessageBus,
    ) -> None:
        self.reasoner = reasoner
        self.tools = tools
        self.sessions = sessions
        self.memory = memory
        self.bus = bus
        self._tasks: set[asyncio.Task[None]] = set()
        self._task_by_id: dict[str, asyncio.Task[None]] = {}
        self._jobs: dict[str, RunningSubagentJob] = {}
        self._admission: set[str] = set()
        self._runs_dir = self.sessions.workspace / ".kirakira" / "subagent-runs"

    def register_tool(self) -> None:
        self.tools.register(
            ToolSpec(
                "spawn",
                "Delegate a bounded task to an isolated inline or background subagent.",
                object_schema(
                    {
                        "task": {"type": "string"},
                        "max_iterations": {
                            "type": "integer",
                            "description": "Maximum child tool-loop iterations (1-20).",
                        },
                        "mode": {
                            "type": "string",
                            "enum": ["inline", "background"],
                        },
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
        )
        self.tools.register(
            ToolSpec(
                "spawn_manage",
                "List or cancel running background subagent tasks.",
                object_schema(
                    {
                        "action": {
                            "type": "string",
                            "enum": ["list", "cancel"],
                        },
                        "task_id": {"type": "string"},
                    },
                    ["action"],
                ),
            ),
            self.manage,
            deferred=True,
        )

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
        parent = self.tools.context
        parent_session = str(parent.get("session_key") or "subagent:unknown")
        task_id = "sub_%s" % uuid4().hex[:12]
        display_label = label.strip() or task[:30]
        if profile not in {"research", "scripting", "general"}:
            return "Error: profile must be research, scripting, or general"
        if mode == "background":
            if not self._acquire_admission(task_id):
                return "Error: subagent capacity reached (active %d, limit %d)" % (
                    len(self._admission),
                    _MAX_BACKGROUND_JOBS,
                )
            parent_channel = str(parent.get("channel") or "")
            parent_chat_id = str(parent.get("chat_id") or "")
            if not parent_channel or not parent_chat_id:
                self._release_admission(task_id)
                return "Error: channel/chat context is required for background mode"
            task_dir = self._runs_dir / task_id
            task_dir.mkdir(parents=True, exist_ok=False)
            self._jobs[task_id] = RunningSubagentJob(
                task_id=task_id,
                label=display_label,
                task=task,
                profile=profile,
                parent_session=parent_session,
                parent_channel=parent_channel,
                parent_chat_id=parent_chat_id,
                task_dir=str(task_dir),
                started_at=datetime.now().astimezone().isoformat(),
            )
            try:
                background = asyncio.create_task(
                    self._run_background(
                        task_id,
                        task,
                        max_iterations,
                        label=display_label,
                        profile=profile,
                        task_dir=task_dir,
                        parent_session=parent_session,
                        parent_channel=parent_channel,
                        parent_chat_id=parent_chat_id,
                    ),
                    name="subagent:%s" % task_id,
                )
            except BaseException:
                self._jobs.pop(task_id, None)
                self._release_admission(task_id)
                raise
            self._tasks.add(background)
            self._task_by_id[task_id] = background
            background.add_done_callback(
                lambda completed, current_id=task_id: self._forget_task(
                    current_id, completed
                )
            )
            return json.dumps(
                {
                    "task_id": task_id,
                    "status": "running",
                    "mode": "background",
                    "label": display_label,
                    "profile": profile,
                    "message": "完成后会回注当前会话。",
                },
                ensure_ascii=False,
            )
        if mode != "inline":
            return "Error: mode must be inline or background"
        if not self._acquire_admission(task_id):
            return "Error: subagent capacity reached (active %d, limit %d)" % (
                len(self._admission),
                _MAX_BACKGROUND_JOBS,
            )
        try:
            result = await self._run_child(
                task_id,
                task,
                max_iterations,
                parent_session=parent_session,
                profile=profile,
            )
        finally:
            self._release_admission(task_id)
        return json.dumps(result, ensure_ascii=False)

    async def manage(self, action: str, task_id: str = "") -> str:
        if action == "list":
            return json.dumps(
                {
                    "running_count": len(self._task_by_id),
                    "jobs": [asdict(job) for job in self._jobs.values()],
                },
                ensure_ascii=False,
            )
        if action == "cancel":
            target = task_id.strip()
            if not target:
                return 'Error: task_id is required for action="cancel"'
            cancelled = await self.cancel(target)
            return json.dumps(
                {
                    "task_id": target,
                    "status": "cancel_requested" if cancelled else "not_found",
                },
                ensure_ascii=False,
            )
        return "Error: action must be list or cancel"

    async def cancel(self, task_id: str) -> bool:
        task = self._task_by_id.get(task_id)
        job = self._jobs.get(task_id)
        if task is None or task.done():
            return False
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if job is not None:
            result = {
                "task_id": task_id,
                "status": "cancelled",
                "result": "后台子任务已由用户取消。",
            }
            task_dir = Path(job.task_dir)
            self._write_result(task_dir, result)
            await self._publish_completion(
                task_id=task_id,
                label=job.label,
                profile=job.profile,
                parent_session=job.parent_session,
                parent_channel=job.parent_channel,
                parent_chat_id=job.parent_chat_id,
                result=result,
            )
        return True

    def _forget_task(self, task_id: str, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        self._task_by_id.pop(task_id, None)
        self._jobs.pop(task_id, None)
        self._release_admission(task_id)

    def _acquire_admission(self, task_id: str) -> bool:
        if len(self._admission) >= _MAX_BACKGROUND_JOBS:
            return False
        self._admission.add(task_id)
        return True

    def _release_admission(self, task_id: str) -> None:
        self._admission.discard(task_id)

    async def _run_child(
        self,
        task_id: str,
        task: str,
        max_iterations: int,
        *,
        parent_session: str,
        profile: str,
    ) -> dict[str, Any]:
        session_key = "%s:%s" % (parent_session, task_id)
        now = datetime.now().astimezone()
        prompt = (
            "你是主 Agent 派生的子代理。只完成下面这个边界明确的任务，"
            "可以使用工具核实和执行；最后返回简洁、可验证的结果。\n\n任务：%s"
            % task
        )
        message = InboundMessage(
            channel="subagent",
            sender="parent_agent",
            chat_id=task_id,
            content=prompt,
            timestamp=now,
            metadata={"parent_session_key": parent_session},
        )
        token = self.tools.set_context(
            session_key=session_key,
            channel="subagent",
            chat_id=task_id,
            current_timestamp=now.isoformat(),
            parent_session_key=parent_session,
            subagent_profile=profile,
        )
        try:
            retrieved = await asyncio.to_thread(
                self.memory.build_retrieval_block, task
            )
            result = await self.reasoner.run_turn(
                msg=message,
                session_key=session_key,
                history=[],
                retrieved_memory_block=retrieved,
                skill_names=[],
                extra_hints=["这是子代理任务，不要调用 spawn 创建更多子代理。"],
                disabled_tools=self._disabled_tools(profile),
                max_iterations_override=max(1, min(20, int(max_iterations))),
            )
        finally:
            try:
                await self.tools.cleanup_owner(session_key)
            finally:
                self.tools.reset_context(token)
        session = self.sessions.get_or_create(session_key)
        session.add_message("user", task, parent_session_key=parent_session)
        session.add_message(
            "assistant",
            result.reply,
            tools_used=result.tools_used,
            tool_chain=result.tool_chain,
            reasoning_content=result.thinking,
            parent_session_key=parent_session,
        )
        self.sessions.save(session)
        return {
            "task_id": task_id,
            "status": "completed",
            "result": result.reply,
            "tools_used": result.tools_used,
            "session_key": session_key,
        }

    async def _run_background(
        self,
        task_id: str,
        task: str,
        max_iterations: int,
        *,
        label: str,
        profile: str,
        task_dir: Path,
        parent_session: str,
        parent_channel: str,
        parent_chat_id: str,
    ) -> None:
        try:
            result = await self._run_child(
                task_id,
                task,
                max_iterations,
                parent_session=parent_session,
                profile=profile,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            result = {
                "task_id": task_id,
                "status": "failed",
                "result": str(exc),
            }
        result_text = str(result.get("result") or "")
        if len(result_text) > _RESULT_LIMIT:
            result_text = result_text[:_RESULT_LIMIT] + "\n...[result truncated]"
            result["result"] = result_text
        self._write_result(task_dir, result)
        await self._publish_completion(
            task_id=task_id,
            label=label,
            profile=profile,
            parent_session=parent_session,
            parent_channel=parent_channel,
            parent_chat_id=parent_chat_id,
            result=result,
        )

    async def _publish_completion(
        self,
        *,
        task_id: str,
        label: str,
        profile: str,
        parent_session: str,
        parent_channel: str,
        parent_chat_id: str,
        result: dict[str, Any],
    ) -> None:
        result_text = str(result.get("result") or "")
        await self.bus.publish_inbound(
            InboundMessage(
                channel=parent_channel,
                sender="subagent:%s" % task_id,
                chat_id=parent_chat_id,
                content=(
                    "<subagent-completion task_id=\"%s\" status=\"%s\">\n%s\n"
                    "</subagent-completion>\n请根据这个子任务结果继续回复用户。"
                    % (task_id, result["status"], result_text)
                ),
                metadata={
                    "session_key_override": parent_session,
                    "omit_user_turn": True,
                    "subagent_completion": True,
                    "subagent_task_id": task_id,
                    "subagent_label": label,
                    "subagent_profile": profile,
                },
            )
        )

    @staticmethod
    def _disabled_tools(profile: str) -> set[str]:
        # 禁用名单必须用真实注册名:曾写成不存在的 "mcp_add",导致 mcp_apply
        # 对所有 profile 的 subagent 实际开放(禁用是空操作)。
        common = {
            "spawn",
            "spawn_manage",
            "message_push",
            "mcp_apply",
            "mcp_remove",
            "plugin_install",
            "plugin_enable",
            "plugin_disable",
            "plugin_uninstall",
            "schedule",
            "cancel_schedule",
        }
        if profile == "research":
            return common | {"bash", "write_file", "edit_file"}
        if profile == "scripting":
            return common | {"web_fetch", "web_search", "vision"}
        return common

    @staticmethod
    def _write_result(task_dir: Path, result: dict[str, Any]) -> None:
        temp = task_dir / ".result.json.tmp"
        target = task_dir / "result.json"
        temp.write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        temp.replace(target)

    async def wait(self, timeout: float = 30.0) -> bool:
        """Wait for currently running background children without stopping them."""

        pending = [task for task in self._tasks if not task.done()]
        if not pending:
            return True
        _done, remaining = await asyncio.wait(
            pending, timeout=max(0.1, timeout)
        )
        return not remaining

    async def shutdown(self) -> None:
        """Cancel children so they cannot publish after the parent loop has stopped."""

        pending = [task for task in self._tasks if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
