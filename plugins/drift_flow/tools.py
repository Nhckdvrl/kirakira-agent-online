"""Drift run 的收尾工具：``message_push`` 与 ``finish_drift``。

两个工具都只改写本轮的 ``DriftRunContext``（同步、无副作用外泄）：
- ``message_push`` 记录一条草稿消息（最多一次），真正的投递由 runner 在 agent run
  结束后到主事件循环上完成，并按真实投递结果把 staged 修正为 sent / silent。
- ``finish_drift`` 记录 status / briefing / 连续性，标记本轮结束。

这样设计避免了在工作线程里跨事件循环访问 async 的 MessageBus。
参考 akashic 的 `plugins/wake_proactive/tools.py` 收尾语义。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from core.schema import ToolSpec
from agent.tools.registry import ToolRegistry, object_schema


@dataclass
class DriftRunContext:
    skill: str
    draft_message: str = ""
    message_pushed: bool = False
    finished: bool = False
    status: str = "completed"
    briefing: str = ""
    scratchpad_update: str = ""
    next_tendency: str = ""
    # run 期间只收集意图,由 runner 在收尾时统一落库——与 message_push 同一取向:
    # 工具不直接碰持久状态,避免半途中断留下半条记录。
    journal_entries: List[dict] = field(default_factory=list)


def register_drift_tools(registry: ToolRegistry, ctx: DriftRunContext) -> None:
    def journal_append(entry_type: str, note: str, key: str = "") -> str:
        """让 Drift 把本轮的事实与自我观察写进 journal。"""
        clean_type = str(entry_type or "").strip()
        clean_note = str(note or "").strip()
        if not clean_type or not clean_note:
            return "Error: entry_type 与 note 都不能为空"
        if len(ctx.journal_entries) >= 20:
            return "Error: 本轮 journal 条目已达上限(20)"
        ctx.journal_entries.append(
            {"entry_type": clean_type, "note": clean_note, "key": str(key or "").strip()}
        )
        return "已记录 journal(%s)。" % clean_type

    def message_push(message: str) -> str:
        text = str(message or "").strip()
        if not text:
            return "Error: message is empty"
        if ctx.message_pushed:
            return "Error: message_push 本轮只能调用一次"
        ctx.draft_message = text
        ctx.message_pushed = True
        return "已记录待发送消息（将在本轮结束后投递）。"

    def finish_drift(
        status: str = "completed",
        briefing: str = "",
        scratchpad_update: str = "",
        next_tendency: str = "",
    ) -> str:
        normalized = str(status or "").strip().lower()
        if normalized not in ("completed", "paused"):
            return "Error: status 必须是 completed 或 paused"
        if normalized == "paused" and not str(scratchpad_update or "").strip():
            return "Error: paused 时必须填写 scratchpad_update 说明下次从哪继续"
        ctx.finished = True
        ctx.status = normalized
        ctx.briefing = str(briefing or "").strip()
        ctx.scratchpad_update = str(scratchpad_update or "").strip()
        ctx.next_tendency = str(next_tendency or "").strip()
        return "Drift 本轮已收尾（status=%s）。不要再调用任何工具。" % normalized

    # 覆盖内置的 async message_push：drift run 跑在工作线程里，不能跨事件循环直连 bus，
    # 所以这里改成同步记录草稿，真正投递交给 runner 在主循环上完成。
    if registry.has("message_push"):
        registry.unregister("message_push")
    registry.register(
        ToolSpec(
            "journal_append",
            "把本轮发现的事实或对自己表现的观察写进 skill journal。"
            "entry_type 用 progress 记进展、self_observation 记自我观察;"
            "key 可选,用于把同一主题的多次记录归拢。",
            object_schema(
                {
                    "entry_type": {"type": "string"},
                    "note": {"type": "string"},
                    "key": {"type": "string"},
                },
                ["entry_type", "note"],
            ),
        ),
        journal_append,
    )
    registry.register(
        ToolSpec(
            "message_push",
            "生成一条主动消息草稿（本轮最多一次）；runtime 会按真实投递结果记录 sent / silent。",
            object_schema({"message": {"type": "string"}}, ["message"]),
        ),
        message_push,
    )
    registry.register(
        ToolSpec(
            "finish_drift",
            "保存本轮 Drift 的状态并结束。执行完毕前必须调用。",
            object_schema(
                {
                    "status": {"type": "string", "enum": ["completed", "paused"]},
                    "briefing": {"type": "string"},
                    "scratchpad_update": {"type": "string"},
                    "next_tendency": {"type": "string"},
                },
                ["status", "briefing"],
            ),
        ),
        finish_drift,
    )
