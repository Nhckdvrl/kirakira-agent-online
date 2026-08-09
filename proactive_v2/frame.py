"""主动链路的 tick 帧(照 Reference `proactive_v2/frame.py`)。

一次 tick 从扁平顺序链改成模块流水线后,模块之间需要一个共享载体传中间产物——就是 frame。
`slots` 是这个载体:每个模块把自己的产物写进约定的 slot,后面的模块按 `requires` 读。

`terminal` 是把"扁平链里的 return"翻译成流水线语义的关键:原来 `_tick()` 用 `return`
提前结束(gate 未过、alert 已推、投递失败),流水线里没有 return 可用,改为在 frame 上
标记终止原因,后续模块看到就跳过自己。这样"提前结束"仍然是显式的,而不是靠模块之间
心照不宣地互相判空。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional

# 约定的 slot 名。集中在这里,避免模块各写各的字符串导致对不上。
SLOT_GATE_PASSED = "gate:passed"
SLOT_FETCH_CHANNELS = "fetch:channels"
SLOT_NEW_CONTENT = "ingest:new_content"
SLOT_CONTEXT_TEXT = "ingest:context_text"
SLOT_JUDGE_CONTEXT = "context:judge"
SLOT_PROPOSAL_ALERT = "proposal:alert"
SLOT_PROPOSAL_CONTENT = "proposal:content"
SLOT_PROPOSAL_DRIFT = "proposal:drift"


@dataclass(frozen=True)
class ProactiveTickInput:
    session_key: str
    started_at: datetime


@dataclass
class ProactiveFrame:
    input: ProactiveTickInput
    slots: Dict[str, Any] = field(default_factory=dict)
    # 非空表示本轮已经结束,值是原因(用于 decisions 记录与排查)。
    terminal: Optional[str] = None

    @property
    def session_key(self) -> str:
        return self.input.session_key

    @property
    def now(self) -> datetime:
        return self.input.started_at

    @property
    def done(self) -> bool:
        return self.terminal is not None

    def finish(self, reason: str) -> "ProactiveFrame":
        """标记本轮结束。第一个原因胜出,后续模块不覆盖它。"""
        if self.terminal is None:
            self.terminal = reason
        return self

    def get(self, slot: str, default: Any = None) -> Any:
        return self.slots.get(slot, default)


def new_proactive_frame(
    session_key: str,
    *,
    now: Optional[datetime] = None,
    slots: Optional[Mapping[str, Any]] = None,
) -> ProactiveFrame:
    return ProactiveFrame(
        input=ProactiveTickInput(
            session_key=session_key,
            started_at=now or datetime.now(timezone.utc),
        ),
        slots=dict(slots or {}),
    )
