"""插件热重载契约:watcher 发现变化 → 换代 → 在途租约保护旧代际。

最关键的一条:**换代发生在 turn 进行中时,旧代际只转 retired,不被销毁**;
turn 释放租约后才 quiesce。这是"热重载不抽走在途能力"的实际验证。
"""

from __future__ import annotations

import asyncio
import unittest
from typing import Any, Dict, List

from agent.plugins.generation import (
    GateResult,
    PluginContributions,
    PluginGeneration,
    PluginGenerationRegistry,
    compute_generation_id,
)
from agent.plugins.specs import PluginSemanticCheck
from agent.plugins.watcher import PluginWatcher


def _generation(plugin_id: str, revision: str, *, failing: bool = False) -> PluginGeneration:
    checks = (PluginSemanticCheck.fail("bad", "nope"),) if failing else ()
    return PluginGeneration(
        plugin_id=plugin_id,
        generation_id=compute_generation_id(
            plugin_id=plugin_id, source_revision=revision, config_revision="c"
        ),
        module_path="demo.plugin",
        source_revision=revision,
        config_revision="c",
        instance=object(),
        contributions=PluginContributions(),
        gate_result=GateResult.from_checks(
            plugin_id=plugin_id, candidate_revision=revision, checks=checks
        ),
    )


class _FakeManager:
    """最小 manager 替身:可控的 revision 与 reconcile 结果。"""

    def __init__(self) -> None:
        self.revision = "r1"
        self.reconcile_calls = 0
        self.generations = PluginGenerationRegistry()
        self.raise_on_scan: Exception | None = None
        self.reconcile_failures = 0

    def watch_revision(self) -> str:
        if self.raise_on_scan is not None:
            raise self.raise_on_scan
        return self.revision

    async def reconcile_changed(self) -> List[Dict[str, Any]]:
        self.reconcile_calls += 1
        if self.reconcile_failures:
            self.reconcile_failures -= 1
            raise RuntimeError("temporary publish failure")
        return [{"plugin_id": "demo", "state": "swapped"}]


class LeaseProtectsInFlightTurnTests(unittest.TestCase):
    def test_swap_during_active_lease_keeps_old_generation_alive(self) -> None:
        registry = PluginGenerationRegistry()
        old = _generation("demo", "v1")
        registry.publish(old)

        # 模拟一次在途 turn 持有租约
        with registry.lease_active() as leased:
            self.assertEqual(len(leased), 1)
            self.assertEqual(old.lease_count, 1)

            # turn 进行中发生热重载换代
            registry.publish(_generation("demo", "v2"))
            self.assertEqual(old.state, "retired")
            # 关键:旧代际仍不可销毁,因为 turn 还在用
            self.assertFalse(old.can_quiesce)
            self.assertEqual(registry.drain_quiescible(), ())

        # turn 结束释放租约后才可回收
        self.assertEqual(old.lease_count, 0)
        self.assertTrue(old.can_quiesce)
        self.assertEqual(len(registry.drain_quiescible()), 1)

    def test_lease_active_releases_even_on_exception(self) -> None:
        registry = PluginGenerationRegistry()
        gen = _generation("demo", "v1")
        registry.publish(gen)
        with self.assertRaises(ValueError):
            with registry.lease_active():
                self.assertEqual(gen.lease_count, 1)
                raise ValueError("turn blew up")
        self.assertEqual(gen.lease_count, 0)

    def test_lease_active_with_no_plugins_is_noop(self) -> None:
        registry = PluginGenerationRegistry()
        with registry.lease_active() as leased:
            self.assertEqual(leased, ())

    def test_committed_lease_waits_for_publication_gate(self) -> None:
        async def scenario() -> None:
            registry = PluginGenerationRegistry()
            registry.publish(_generation("demo", "v1"))
            await registry.begin_publication()
            entered = asyncio.Event()

            async def acquire() -> None:
                async with registry.lease_committed() as leased:
                    self.assertEqual(len(leased), 1)
                    entered.set()

            task = asyncio.create_task(acquire())
            await asyncio.sleep(0)
            self.assertFalse(entered.is_set())
            await registry.finish_publication()
            await asyncio.wait_for(entered.wait(), timeout=1)
            await task

        asyncio.run(scenario())


class WatcherTests(unittest.TestCase):
    def test_baseline_reconciles_once_then_only_on_change(self) -> None:
        async def scenario() -> None:
            manager = _FakeManager()
            watcher = PluginWatcher(manager, interval_seconds=0.01)
            task = asyncio.create_task(watcher.run())

            # 第一次成功扫描建立基线并 reconcile 一次(照 Reference)
            await asyncio.sleep(0.05)
            baseline_calls = manager.reconcile_calls
            self.assertEqual(baseline_calls, 1)

            # revision 不变则不再换代
            await asyncio.sleep(0.05)
            self.assertEqual(manager.reconcile_calls, baseline_calls)

            # revision 变化后触发换代
            manager.revision = "r2"
            await asyncio.sleep(0.08)
            self.assertGreater(manager.reconcile_calls, baseline_calls)

            watcher.stop()
            await watcher.wait_stopped()
            task.cancel()

        asyncio.run(scenario())

    def test_scan_failure_is_recorded_and_loop_survives(self) -> None:
        async def scenario() -> None:
            manager = _FakeManager()
            manager.raise_on_scan = OSError("plugin dir vanished mid-swap")
            watcher = PluginWatcher(manager, interval_seconds=0.01)
            task = asyncio.create_task(watcher.run())
            await asyncio.sleep(0.05)
            self.assertIn("vanished", str(watcher.last_error))

            # 目录恢复后能继续换代
            manager.raise_on_scan = None
            manager.revision = "r9"
            await asyncio.sleep(0.08)
            self.assertGreaterEqual(manager.reconcile_calls, 1)

            watcher.stop()
            await watcher.wait_stopped()
            task.cancel()

        asyncio.run(scenario())

    def test_wake_forces_immediate_reconcile(self) -> None:
        async def scenario() -> None:
            manager = _FakeManager()
            watcher = PluginWatcher(manager, interval_seconds=10)
            task = asyncio.create_task(watcher.run())
            await asyncio.sleep(0.02)
            watcher.wake()
            await asyncio.sleep(0.05)
            self.assertGreaterEqual(manager.reconcile_calls, 1)
            watcher.stop()
            await watcher.wait_stopped()
            task.cancel()

        asyncio.run(scenario())

    def test_failed_revision_is_retried_without_another_file_change(self) -> None:
        async def scenario() -> None:
            manager = _FakeManager()
            manager.reconcile_failures = 1
            watcher = PluginWatcher(manager, interval_seconds=0.01)
            task = asyncio.create_task(watcher.run())

            await asyncio.sleep(0.08)

            self.assertGreaterEqual(manager.reconcile_calls, 2)
            self.assertIsNone(watcher.last_error)
            self.assertEqual(watcher.status()["revision"], "r1")
            watcher.stop()
            await watcher.wait_stopped()
            task.cancel()

        asyncio.run(scenario())

    def test_status_reports_generations_and_pending_leases(self) -> None:
        manager = _FakeManager()
        old = _generation("demo", "v1")
        manager.generations.publish(old)
        old.acquire()
        manager.generations.publish(_generation("demo", "v2"))

        status = PluginWatcher(manager).status()
        self.assertIn("demo", status["activeGenerations"])
        self.assertEqual(len(status["retiredPending"]), 1)
        self.assertEqual(status["retiredPending"][0]["leases"], 1)


if __name__ == "__main__":
    unittest.main()
