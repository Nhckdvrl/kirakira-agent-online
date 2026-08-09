"""MCP stdio client and declarative workspace server tests."""

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path

from agent.mcp import (
    McpCatalogPublisher,
    McpClient,
    WorkspaceMcpWatcher,
    load_workspace_mcp_declarations,
)
from core.schema import ToolCall
from agent.plugins.snapshot import (
    RuntimeSnapshotStore,
    SnapshotToolView,
    bind_runtime_snapshot,
    reset_runtime_snapshot,
)
from agent.tools.registry import ToolRegistry


def _build_watcher(servers):
    """按运行时的真实装配方式接线：store + publisher + watcher。"""

    store = RuntimeSnapshotStore()
    publisher = McpCatalogPublisher(store)
    store.set_drain_handler(publisher.drain_snapshot)
    return store, WorkspaceMcpWatcher(servers, publisher)


async def _call_via_snapshot(store, tools, call):
    """模拟一个 turn：先锁定快照租约，再通过快照视图调用工具。"""

    lease = store.lease()
    token = bind_runtime_snapshot(lease)
    try:
        view = SnapshotToolView(tools, lease.snapshot)
        return await view.execute_async(call)
    finally:
        reset_runtime_snapshot(token)
        await lease.release()


SERVER = Path(__file__).parent / "fixtures" / "fake_mcp_server.py"


def _declaration(name: str = "fake") -> str:
    return "\n".join(
        [
            "schema_version = 1",
            'name = "%s"' % name,
            'command = ["%s", "%s"]' % (sys.executable, SERVER),
        ]
    )


class McpClientTests(unittest.TestCase):
    def test_client_handshake_calls_and_errors(self):
        async def scenario():
            client = McpClient("fake", [sys.executable, str(SERVER)])
            try:
                infos = await client.connect()
                self.assertEqual([info.name for info in infos], ["echo", "fail"])
                first, second = await asyncio.gather(
                    client.call("echo", {"text": "one"}),
                    client.call("echo", {"text": "two"}),
                )
                self.assertEqual((first, second), ("one", "two"))
                self.assertTrue((await client.call("fail", {})).startswith("Error:"))
            finally:
                await client.disconnect()

        asyncio.run(scenario())


class WorkspaceMcpDeclarationTests(unittest.TestCase):
    def _servers_dir(self, tmp: str) -> Path:
        servers = Path(tmp) / "mcp" / "servers"
        servers.mkdir(parents=True)
        return servers

    def test_revision_tracks_content_not_mtime(self):
        with tempfile.TemporaryDirectory() as tmp:
            servers = self._servers_dir(tmp)
            (servers / "fake.toml").write_text(_declaration(), encoding="utf-8")
            first = load_workspace_mcp_declarations(servers).revision
            (servers / "fake.toml").write_text(_declaration(), encoding="utf-8")
            self.assertEqual(load_workspace_mcp_declarations(servers).revision, first)

    def test_watch_path_content_changes_revision(self):
        with tempfile.TemporaryDirectory() as tmp:
            servers = self._servers_dir(tmp)
            watched = Path(tmp) / "mcp" / "code.py"
            watched.write_text("v1", encoding="utf-8")
            (servers / "fake.toml").write_text(
                _declaration() + '\nwatch_paths = ["../code.py"]\n', encoding="utf-8"
            )
            first = load_workspace_mcp_declarations(servers).revision
            watched.write_text("v2", encoding="utf-8")
            self.assertNotEqual(load_workspace_mcp_declarations(servers).revision, first)

    def test_rejects_name_mismatching_filename(self):
        with tempfile.TemporaryDirectory() as tmp:
            servers = self._servers_dir(tmp)
            (servers / "fake.toml").write_text(_declaration("other"), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_workspace_mcp_declarations(servers)

    def test_rejects_unknown_field(self):
        with tempfile.TemporaryDirectory() as tmp:
            servers = self._servers_dir(tmp)
            (servers / "fake.toml").write_text(
                _declaration() + '\nbogus = "x"\n', encoding="utf-8"
            )
            with self.assertRaises(ValueError):
                load_workspace_mcp_declarations(servers)

    def test_rejects_cwd_escaping_safe_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            servers = self._servers_dir(tmp)
            (servers / "fake.toml").write_text(
                _declaration() + '\ncwd = "../../../etc"\n', encoding="utf-8"
            )
            with self.assertRaises(ValueError):
                load_workspace_mcp_declarations(servers)

    def test_missing_directory_yields_empty_specs(self):
        with tempfile.TemporaryDirectory() as tmp:
            declarations = load_workspace_mcp_declarations(
                Path(tmp) / "mcp" / "servers", mcp_root=Path(tmp) / "mcp"
            )
            self.assertEqual(declarations.specs, {})


class WorkspaceMcpWatcherTests(unittest.TestCase):
    def _servers_dir(self, tmp: str) -> Path:
        servers = Path(tmp) / "mcp" / "servers"
        servers.mkdir(parents=True)
        return servers

    def test_publishes_declared_server_tools_into_snapshot(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                servers = self._servers_dir(tmp)
                (servers / "fake.toml").write_text(_declaration(), encoding="utf-8")
                tools = ToolRegistry()
                store, watcher = _build_watcher(servers)
                try:
                    self.assertTrue(await watcher.reconcile())
                    # MCP 工具只存在于快照里，不污染共享注册表。
                    self.assertFalse(tools.has("mcp_fake__echo"))
                    self.assertIn("mcp_fake__echo", store.current.mcp_tool_names)
                    result = await _call_via_snapshot(
                        store, tools, ToolCall("1", "mcp_fake__echo", {"text": "hello"})
                    )
                    self.assertEqual(result.content, "hello")
                    self.assertEqual(watcher.status()["servers"], ["fake"])
                finally:
                    await watcher.shutdown()

        asyncio.run(scenario())

    def test_mcp_tools_are_deferred_in_snapshot_view(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                servers = self._servers_dir(tmp)
                (servers / "fake.toml").write_text(_declaration(), encoding="utf-8")
                tools = ToolRegistry()
                store, watcher = _build_watcher(servers)
                try:
                    await watcher.reconcile()
                    view = SnapshotToolView(tools, store.current)
                    self.assertTrue(view.is_deferred("mcp_fake__echo"))
                    # deferred 工具未解锁时不进入可见列表。
                    self.assertNotIn(
                        "mcp_fake__echo",
                        [spec.name for spec in view.visible_specs(set())],
                    )
                    self.assertIn(
                        "mcp_fake__echo",
                        [
                            spec.name
                            for spec in view.visible_specs({"mcp_fake__echo"})
                        ],
                    )
                finally:
                    await watcher.shutdown()

        asyncio.run(scenario())

    def test_unchanged_revision_does_not_republish(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                servers = self._servers_dir(tmp)
                (servers / "fake.toml").write_text(_declaration(), encoding="utf-8")
                store, watcher = _build_watcher(servers)
                try:
                    self.assertTrue(await watcher.reconcile())
                    generation = watcher.status()["generationId"]
                    self.assertFalse(await watcher.reconcile())
                    self.assertEqual(watcher.status()["generationId"], generation)
                finally:
                    await watcher.shutdown()

        asyncio.run(scenario())

    def test_removing_declaration_drains_server_tools(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                servers = self._servers_dir(tmp)
                (servers / "fake.toml").write_text(_declaration(), encoding="utf-8")
                store, watcher = _build_watcher(servers)
                try:
                    await watcher.reconcile()
                    (servers / "fake.toml").unlink()
                    self.assertTrue(await watcher.reconcile())
                    self.assertEqual(store.current.mcp_tool_names, ())
                    self.assertEqual(watcher.status()["servers"], [])
                finally:
                    await watcher.shutdown()

        asyncio.run(scenario())

    def test_in_flight_turn_keeps_its_tools_across_hot_reload(self):
        """换代不能把在途 turn 的工具抽走，也不能提前断开它正在用的连接。"""

        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                servers = self._servers_dir(tmp)
                (servers / "fake.toml").write_text(_declaration(), encoding="utf-8")
                tools = ToolRegistry()
                store, watcher = _build_watcher(servers)
                try:
                    await watcher.reconcile()

                    # 一个 turn 开始，锁定当前快照。
                    lease = store.lease()
                    pinned = lease.snapshot
                    self.assertIn("mcp_fake__echo", pinned.mcp_tool_names)

                    # turn 进行中，声明变了并完成换代。
                    (servers / "fake.toml").unlink()
                    (servers / "second.toml").write_text(
                        _declaration("second"), encoding="utf-8"
                    )
                    self.assertTrue(await watcher.reconcile())
                    self.assertEqual(
                        store.current.mcp_tool_names,
                        ("mcp_second__echo", "mcp_second__fail"),
                    )

                    # 在途 turn 仍然看得到并且能真正调用旧代际的工具。
                    view = SnapshotToolView(tools, pinned)
                    result = await view.execute_async(
                        ToolCall("1", "mcp_fake__echo", {"text": "in-flight"})
                    )
                    self.assertEqual(result.content, "in-flight")

                    # 租约释放后旧代际才排空。
                    await lease.release()
                    self.assertEqual(pinned.state, "drained")
                finally:
                    await watcher.shutdown()

        asyncio.run(scenario())

    def test_invalid_candidate_keeps_previous_generation_serving(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                servers = self._servers_dir(tmp)
                (servers / "fake.toml").write_text(_declaration(), encoding="utf-8")
                tools = ToolRegistry()
                store, watcher = _build_watcher(servers)
                try:
                    await watcher.reconcile()
                    generation = watcher.status()["generationId"]
                    # 第二份声明非法：整批候选必须被拒绝，旧代际继续服务。
                    (servers / "broken.toml").write_text(
                        'schema_version = 1\nname = "broken"\ncommand = []\n',
                        encoding="utf-8",
                    )
                    with self.assertRaises(ValueError):
                        await watcher.reconcile()
                    self.assertIn("mcp_fake__echo", store.current.mcp_tool_names)
                    self.assertEqual(watcher.status()["generationId"], generation)

                    # 删掉坏声明后内容回到正在服务的 revision，无需换代。
                    (servers / "broken.toml").unlink()
                    self.assertFalse(await watcher.reconcile())
                    self.assertEqual(watcher.status()["generationId"], generation)

                    # 把坏声明改成合法的，则正常换代并带上新 server。
                    (servers / "second.toml").write_text(
                        _declaration("second"), encoding="utf-8"
                    )
                    self.assertTrue(await watcher.reconcile())
                    self.assertEqual(watcher.status()["servers"], ["fake", "second"])
                    result = await _call_via_snapshot(
                        store, tools, ToolCall("1", "mcp_second__echo", {"text": "ok"})
                    )
                    self.assertEqual(result.content, "ok")
                finally:
                    await watcher.shutdown()

        asyncio.run(scenario())

    def test_unconnectable_server_keeps_previous_generation_serving(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                servers = self._servers_dir(tmp)
                (servers / "fake.toml").write_text(_declaration(), encoding="utf-8")
                tools = ToolRegistry()
                store, watcher = _build_watcher(servers)
                try:
                    await watcher.reconcile()
                    (servers / "dead.toml").write_text(
                        "schema_version = 1\n"
                        'name = "dead"\n'
                        'command = ["%s", "-c", "raise SystemExit(1)"]\n'
                        % sys.executable,
                        encoding="utf-8",
                    )
                    with self.assertRaises(Exception):
                        await watcher.reconcile()
                    # 旧 server 的连接没有被这次失败带走。
                    result = await _call_via_snapshot(
                        store,
                        tools,
                        ToolCall("1", "mcp_fake__echo", {"text": "still-alive"}),
                    )
                    self.assertEqual(result.content, "still-alive")
                finally:
                    await watcher.shutdown()

        asyncio.run(scenario())

    def test_two_sources_share_one_generation(self):
        """workspace 与插件两个来源共用一代 catalog，互相不会覆盖。"""

        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                servers = self._servers_dir(tmp)
                (servers / "fake.toml").write_text(_declaration(), encoding="utf-8")
                store, watcher = _build_watcher(servers)
                publisher = watcher._publisher
                try:
                    await watcher.reconcile()
                    await publisher.publish(
                        {
                            "plugged": {
                                "command": [sys.executable, str(SERVER)],
                                "env": {},
                            }
                        },
                        source="plugins",
                    )
                    self.assertEqual(
                        store.current.mcp_tool_names,
                        ("mcp_fake__echo", "mcp_fake__fail", "mcp_plugged__echo", "mcp_plugged__fail"),
                    )
                finally:
                    await watcher.shutdown()

        asyncio.run(scenario())

    def test_shutdown_releases_generations(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                servers = self._servers_dir(tmp)
                (servers / "fake.toml").write_text(_declaration(), encoding="utf-8")
                store, watcher = _build_watcher(servers)
                await watcher.reconcile()
                await watcher.shutdown()
                self.assertIsNone(watcher._publisher.host.get(
                    store.current.mcp_generation_id
                ))

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
