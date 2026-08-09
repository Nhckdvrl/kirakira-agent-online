"""consolidation 驱动权移交契约(NOW.md 第 1 项 / decisions/0002 的后继)。

移交后必须同时成立:
- 有承重维护器时,runtime 不再调旧 schedule_consolidation/consolidate_turn(不重复归档);
- context guard 仍然有效:超阈值且归档无法推进时拒绝本轮,不静默丢历史;
- 没有维护器时(未配 embedding / 未绑定 session)回退旧路径,链路不中断。
"""

from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from typing import Any

from core.memory.services import MemoryServices
from agent.core.runtime import PassiveTurnPipeline, RuntimeConfig


class _Session:
    def __init__(self, count: int, consolidated: int = 0) -> None:
        self.key = "cli:1"
        self.messages = [{"role": "user", "content": "m%d" % i} for i in range(count)]
        self.last_consolidated = consolidated
        self.metadata: dict[str, Any] = {"channel": "cli", "chat_id": "1"}


class _Maintenance:
    """替身维护器:可控是否推进 last_consolidated。"""

    def __init__(self, *, advances: bool = True, bound: bool = True) -> None:
        self._get_session = (lambda key: None) if bound else None
        self.advances = advances
        self.calls: list[Any] = []

    async def consolidate(self, request):
        self.calls.append(request)
        if self.advances:
            request.session.last_consolidated = len(request.session.messages)
        return SimpleNamespace(consolidated_count=1, trace={"mode": "markdown"})


def _pipeline(maintenance: Any) -> PassiveTurnPipeline:
    pipeline = PassiveTurnPipeline.__new__(PassiveTurnPipeline)
    pipeline.config = RuntimeConfig(model="m", history_window=10)
    pipeline.memory_services = (
        MemoryServices(markdown=SimpleNamespace(maintenance=maintenance))
        if maintenance is not None
        else None
    )
    saved: list[Any] = []

    async def save_async(session):
        saved.append(session)

    from agent.looping.ports import SessionServices

    pipeline.session_services = SessionServices(
        transcript_store=SimpleNamespace(save_async=save_async)
    )
    pipeline.saved = saved  # 便于断言
    return pipeline


class MaintenanceDetectionTests(unittest.TestCase):
    def test_bound_maintenance_is_used(self) -> None:
        pipeline = _pipeline(_Maintenance(bound=True))
        self.assertIsNotNone(pipeline._markdown_maintenance())

    def test_unbound_maintenance_falls_back(self) -> None:
        # 没绑定 session 生命周期的维护器不能驱动归档
        pipeline = _pipeline(_Maintenance(bound=False))
        self.assertIsNone(pipeline._markdown_maintenance())

    def test_no_services_falls_back(self) -> None:
        self.assertIsNone(_pipeline(None)._markdown_maintenance())


class ContextGuardTests(unittest.TestCase):
    def test_below_threshold_passes_without_consolidating(self) -> None:
        async def scenario() -> None:
            maintenance = _Maintenance()
            pipeline = _pipeline(maintenance)
            # history_window=10 → threshold = 10 + max(5,5) = 15
            session = _Session(count=5)
            self.assertEqual(
                await pipeline._guard_memory_context(session, "cli:1"), ""
            )
            self.assertEqual(maintenance.calls, [])

        asyncio.run(scenario())

    def test_over_threshold_consolidates_and_passes(self) -> None:
        async def scenario() -> None:
            maintenance = _Maintenance(advances=True)
            pipeline = _pipeline(maintenance)
            session = _Session(count=40)
            self.assertEqual(
                await pipeline._guard_memory_context(session, "cli:1"), ""
            )
            # 强制归档,且推进后保存了 session
            self.assertEqual(len(maintenance.calls), 1)
            self.assertTrue(maintenance.calls[0].force)
            self.assertEqual(len(pipeline.saved), 1)

        asyncio.run(scenario())

    def test_stalled_consolidation_refuses_the_turn(self) -> None:
        async def scenario() -> None:
            # 归档没有推进 → 必须拒绝本轮,避免静默丢历史
            maintenance = _Maintenance(advances=False)
            pipeline = _pipeline(maintenance)
            reply = await pipeline._guard_memory_context(_Session(count=40), "cli:1")
            self.assertIn("安全阈值", reply)
            self.assertEqual(pipeline.saved, [])

        asyncio.run(scenario())

    def test_consolidate_exception_refuses_instead_of_crashing(self) -> None:
        async def scenario() -> None:
            class _Boom(_Maintenance):
                async def consolidate(self, request):
                    raise RuntimeError("consolidation exploded")

            pipeline = _pipeline(_Boom())
            reply = await pipeline._guard_memory_context(_Session(count=40), "cli:1")
            # 异常不外抛,但也不能放行——仍按"未能推进"处理
            self.assertIn("安全阈值", reply)

        asyncio.run(scenario())


class MaintenanceWaitTests(unittest.TestCase):
    """下一轮读历史前必须等上一轮归档收口,否则会读到错位的历史窗口。"""

    def _maintenance(self):
        from core.memory.markdown import MarkdownMemoryMaintenance

        m = MarkdownMemoryMaintenance.__new__(MarkdownMemoryMaintenance)
        m._maintenance_tasks = {}
        return m

    def test_no_task_returns_immediately(self) -> None:
        async def scenario() -> None:
            await self._maintenance().wait_for_session("absent")

        asyncio.run(scenario())

    def test_waits_until_maintenance_finishes(self) -> None:
        async def scenario() -> None:
            m = self._maintenance()
            done: list[str] = []

            async def work() -> None:
                await asyncio.sleep(0.05)
                done.append("finished")

            m._maintenance_tasks["s"] = asyncio.create_task(work())
            await m.wait_for_session("s")
            self.assertEqual(done, ["finished"])

        asyncio.run(scenario())

    def test_timeout_does_not_cancel_the_running_maintenance(self) -> None:
        async def scenario() -> None:
            m = self._maintenance()
            done: list[str] = []

            async def slow() -> None:
                await asyncio.sleep(0.3)
                done.append("finished")

            task = asyncio.create_task(slow())
            m._maintenance_tasks["s"] = task
            await m.wait_for_session("s", timeout=0.05)
            # 超时只放弃等待;取消会让归档停在半途,比等不到更糟
            self.assertFalse(task.done())
            await task
            self.assertEqual(done, ["finished"])

        asyncio.run(scenario())

    def test_failed_maintenance_does_not_propagate_to_the_turn(self) -> None:
        async def scenario() -> None:
            m = self._maintenance()

            async def boom() -> None:
                raise RuntimeError("maintenance exploded")

            m._maintenance_tasks["s"] = asyncio.create_task(boom())
            await asyncio.sleep(0)
            # 上一轮归档失败不该把本轮 turn 打挂
            await m.wait_for_session("s")

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
