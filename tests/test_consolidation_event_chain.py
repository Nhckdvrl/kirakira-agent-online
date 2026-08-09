"""consolidation → ConsolidationCommitted → 引擎长期事实提取。

这条链此前是断的:引擎从 Reference 照抄来的 `_on_consolidation_committed`
(从 consolidation 窗口提取长期 profile/preference/procedure)没有任何发射者,
属于死代码。现在由 MemoryRuntime 在归档提交后广播,链路接通。

同时锁定一条安全边界:consolidation 已经写盘,下游提取失败不得回滚归档。
"""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from core.memory.events import ConsolidationCommitted
from bus.event_bus import EventBus
from core.memory.legacy import MemoryRuntime
from session.manager import Session


def _runtime(workspace: Path, bus: EventBus | None) -> MemoryRuntime:
    return MemoryRuntime(workspace, event_bus=bus)


def _session() -> Session:
    session = Session(key="cli:1")
    session.metadata["channel"] = "cli"
    session.metadata["chat_id"] = "1"
    return session


class ConsolidationEventTests(unittest.TestCase):
    def test_publishes_event_with_entries_and_scope(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                bus = EventBus()
                received: list[ConsolidationCommitted] = []

                async def handler(event: ConsolidationCommitted) -> None:
                    received.append(event)

                bus.on(ConsolidationCommitted, handler)
                runtime = _runtime(Path(tmp), bus)
                runtime._publish_consolidation(
                    _session(),
                    "cli:1@m5",
                    ["用户在名古屋读书", "偏好中文回复"],
                    [
                        {"role": "user", "content": "我在名古屋"},
                        {"role": "assistant", "content": "记住了"},
                    ],
                )
                await asyncio.sleep(0.05)

                self.assertEqual(len(received), 1)
                event = received[0]
                self.assertEqual(
                    [entry for entry, _weight in event.history_entry_payloads],
                    ["用户在名古屋读书", "偏好中文回复"],
                )
                self.assertEqual(event.source_ref, "cli:1@m5")
                self.assertEqual(event.scope_channel, "cli")
                self.assertEqual(event.scope_chat_id, "1")
                self.assertIn("名古屋", event.conversation)
                await runtime.shutdown()

        asyncio.run(scenario())

    def test_no_event_bus_is_noop(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                runtime = _runtime(Path(tmp), None)
                # 不抛异常即可:没有总线时静默跳过
                runtime._publish_consolidation(_session(), "s", ["x"], [])
                await runtime.shutdown()

        asyncio.run(scenario())

    def test_empty_history_entries_publishes_nothing(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                bus = EventBus()
                received: list[object] = []

                async def handler(event: ConsolidationCommitted) -> None:
                    received.append(event)

                bus.on(ConsolidationCommitted, handler)
                runtime = _runtime(Path(tmp), bus)
                runtime._publish_consolidation(_session(), "s", [], [])
                await asyncio.sleep(0.05)
                self.assertEqual(received, [])
                await runtime.shutdown()

        asyncio.run(scenario())

    def test_publish_failure_does_not_propagate(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                class _BrokenBus:
                    def enqueue(self, event: object) -> None:
                        raise RuntimeError("bus down")

                runtime = _runtime(Path(tmp), _BrokenBus())
                # 归档已提交,广播失败不能把异常抛回 consolidation
                runtime._publish_consolidation(_session(), "s", ["x"], [])
                await runtime.shutdown()

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
