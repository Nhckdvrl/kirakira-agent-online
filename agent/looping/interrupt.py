from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class InterruptResult:
    message: str


class InterruptController:
    """Adapt Kirakira's session interrupt callback to Reference's port."""

    def __init__(self, callback: Callable[[str], bool]) -> None:
        self._callback = callback

    def request_interrupt(
        self,
        *,
        session_key: str,
        sender: str,
        command: str,
    ) -> InterruptResult:
        del sender, command
        interrupted = bool(self._callback(session_key))
        return InterruptResult(
            "本轮已中断。" if interrupted else "当前没有正在执行的任务。"
        )
