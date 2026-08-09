"""安装 → 热重载 → 免重启激活 的端到端契约。

这条链把本轮所有插件工作串起来:安装器落盘 → reload hook 唤醒 watcher →
reconcile_changed 发现新目录并激活 → 发布代际。卸载/禁用走反向路径。
"""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from bus.event_bus import EventBus
from agent.plugins import PluginManager
from agent.tools.registry import ToolRegistry

_PLUGIN_SRC = (
    "from agent.plugins import Plugin\n"
    "class Demo(Plugin):\n"
    "    name = '{name}'\n"
    "    version = '{version}'\n"
)


def _make_manager(workspace: Path) -> PluginManager:
    return PluginManager(
        [workspace / ".kirakira" / "plugins"],
        event_bus=EventBus(),
        tool_registry=ToolRegistry(),
        workspace=workspace,
        session_manager=None,
        memory=None,
    )


def _write_source(root: Path, name: str, version: str = "1.0") -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "plugin.py").write_text(
        _PLUGIN_SRC.format(name=name, version=version), encoding="utf-8"
    )
    return root


class InstallHotActivateTests(unittest.TestCase):
    def test_cross_surface_failure_stays_gated_and_retries_same_revision(self) -> None:
        class FailOncePublisher:
            def __init__(self) -> None:
                self.calls = 0

            async def publish(self, _configs, *, source="workspace"):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("temporary MCP failure")
                return "published"

        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                workspace = Path(tmp) / "workspace"
                workspace.mkdir()
                source = _write_source(Path(tmp) / "atomicdemo", "atomicdemo")
                manager = _make_manager(workspace)
                await manager.load_all()
                await manager.install(str(source))
                publisher = FailOncePublisher()
                manager.mcp_publisher = publisher

                with self.assertRaisesRegex(RuntimeError, "temporary MCP"):
                    await manager.reconcile_changed()
                self.assertTrue(manager.generations.publication_in_progress)

                entered = asyncio.Event()

                async def new_turn() -> None:
                    async with manager.generations.lease_committed():
                        entered.set()

                turn = asyncio.create_task(new_turn())
                await asyncio.sleep(0)
                self.assertFalse(entered.is_set())

                # No file change: the dirty committed revision is republished.
                await manager.reconcile_changed()
                self.assertEqual(publisher.calls, 2)
                self.assertFalse(manager.generations.publication_in_progress)
                await asyncio.wait_for(entered.wait(), timeout=1)
                await turn

        asyncio.run(scenario())

    def test_install_then_reconcile_activates_without_restart(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                workspace = Path(tmp) / "workspace"
                workspace.mkdir()
                source = _write_source(Path(tmp) / "hotdemo", "hotdemo")
                manager = _make_manager(workspace)
                await manager.load_all()
                self.assertEqual(manager.active, [])

                # 安装并触发一次热重载
                woken: list[int] = []
                manager.reload_hook = lambda: woken.append(1)
                await manager.install(str(source))
                self.assertEqual(len(woken), 1)

                results = await manager.reconcile_changed()

                # 免重启激活,并发布了代际
                self.assertIn(
                    {"plugin_id": "hotdemo", "state": "activated"}, results
                )
                self.assertEqual(
                    [record.plugin_id for record in manager.active], ["hotdemo"]
                )
                self.assertIsNotNone(manager.generations.current("hotdemo"))

                await manager.terminate_all()

        asyncio.run(scenario())

    def test_uninstall_then_reconcile_deactivates(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                workspace = Path(tmp) / "workspace"
                workspace.mkdir()
                source = _write_source(Path(tmp) / "byedemo", "byedemo")
                manager = _make_manager(workspace)
                await manager.install(str(source))
                await manager.reconcile_changed()
                self.assertEqual(
                    [r.plugin_id for r in manager.active], ["byedemo"]
                )
                generation = manager.generations.current("byedemo")
                self.assertIsNotNone(generation)

                await manager.uninstall("byedemo")
                results = await manager.reconcile_changed()

                self.assertIn(
                    {"plugin_id": "byedemo", "state": "deactivated"}, results
                )
                self.assertEqual(manager.active, [])
                # 代际被退休,不再是当前代际
                self.assertIsNone(manager.generations.current("byedemo"))

                await manager.terminate_all()

        asyncio.run(scenario())

    def test_disable_via_manifest_deactivates_without_removing_files(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                workspace = Path(tmp) / "workspace"
                workspace.mkdir()
                source = _write_source(Path(tmp) / "toggleme", "toggleme")
                manager = _make_manager(workspace)
                await manager.install(str(source))
                await manager.reconcile_changed()
                self.assertEqual(
                    [r.plugin_id for r in manager.active], ["toggleme"]
                )

                manager.disable_plugin("toggleme")
                await manager.reconcile_changed()
                self.assertEqual(manager.active, [])
                # 文件仍在,只是被 manifest 关掉
                self.assertTrue(
                    (workspace / ".kirakira" / "plugins" / "toggleme").is_dir()
                )

                # 重新启用后又能上线
                manager.enable_plugin("toggleme")
                await manager.reconcile_changed()
                self.assertEqual(
                    [r.plugin_id for r in manager.active], ["toggleme"]
                )

                await manager.terminate_all()

        asyncio.run(scenario())

    def test_upgrade_swaps_generation_and_keeps_plugin_active(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                workspace = Path(tmp) / "workspace"
                workspace.mkdir()
                src = Path(tmp) / "upgrademe"
                _write_source(src, "upgrademe", version="1.0")
                manager = _make_manager(workspace)
                await manager.install(str(src))
                await manager.reconcile_changed()
                first = manager.generations.current("upgrademe")
                self.assertIsNotNone(first)

                # 升级源码内容 → revision 变化 → 换代
                _write_source(src, "upgrademe", version="2.0")
                await manager.install(str(src))
                await manager.reconcile_changed()

                second = manager.generations.current("upgrademe")
                self.assertIsNotNone(second)
                self.assertNotEqual(first.generation_id, second.generation_id)
                # 没有在途 turn 持租约,旧代际换代后立即被回收
                self.assertEqual(first.state, "quiesced")
                self.assertEqual(
                    [r.plugin_id for r in manager.active], ["upgrademe"]
                )

                await manager.terminate_all()

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
