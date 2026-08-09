"""当前 control turn 的 ContextVar(照 Reference `agent/control/context.py`)。

`ConversationRuntime._run` 在调用 executor 前 set,执行结束 reset;
`agent_restart` 工具据此拿到自己所在的 turn id——没有它,工具无法证明
"我就是那个唯一在途 turn",准入冻结也就无从谈起。
"""

from __future__ import annotations

from contextvars import ContextVar

current_turn_id: ContextVar[str] = ContextVar("current_turn_id", default="")
