"""akasha RAR 引擎与记忆引擎插件路由。

三条最关键的语义:
- **路由真的被读**:`[memory].plugin` 决定用哪套引擎,未知名字 fail loud;
- **工具面完全由 engine 决定**:akasha 只声明 recall + 自定义 reinforce_memory,
  **没有 memorize/forget**(它从 turn 自动摄入)——不能再退回旧 schema 注册,
  否则模型看得到 memorize、调用后被引擎拒绝(实弹踩过);
- **本地完整性**:Akasha 运行时可在没有外部源码 checkout 时独立导入。
"""

from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace

from core.memory.services import resolve_memory_plugin


class EngineRoutingTests(unittest.TestCase):
    def test_routes_by_name_with_default_fallback(self) -> None:
        self.assertEqual(resolve_memory_plugin("").plugin_id, "default")
        self.assertEqual(resolve_memory_plugin("default").plugin_id, "default")
        self.assertEqual(resolve_memory_plugin("akasha").plugin_id, "akasha")

    def test_unknown_engine_fails_loud(self) -> None:
        # 配错引擎名必须立刻可见,不能静默回落到 default
        with self.assertRaises(ValueError) as caught:
            resolve_memory_plugin("nope")
        self.assertIn("nope", str(caught.exception))

    def test_both_plugins_share_the_same_shape(self) -> None:
        for name in ("default", "akasha"):
            plugin = resolve_memory_plugin(name)
            self.assertTrue(hasattr(plugin, "build"))
            self.assertTrue(hasattr(plugin, "ensure_workspace_storage"))


class AkashaEngineShapeTests(unittest.TestCase):
    def test_descriptor_and_tool_profile(self) -> None:
        from plugins.akasha.engine import AkashaMemoryEngine

        descriptor = AkashaMemoryEngine.DESCRIPTOR
        self.assertEqual(descriptor.name, "akasha")
        capabilities = {c.value for c in descriptor.capabilities}
        self.assertIn("retrieve.context_block", capabilities)
        # akasha 的真相源是会话消息表,不是自己的 item 表
        self.assertEqual(descriptor.notes.get("truth"), "sessions.db/messages")


class ToolProfileDrivesRegistrationTests(unittest.TestCase):
    """工具面由 engine 声明;声明什么注册什么,没声明的不注册。"""

    def _registry_for(self, profile) -> set[str]:
        from core.memory.engine import MemoryCapability
        from agent.tools.builtins import _register_memory_tools
        from agent.tools.registry import ToolRegistry

        engine = SimpleNamespace(
            DESCRIPTOR=SimpleNamespace(
                capabilities=frozenset({MemoryCapability.RETRIEVE_CONTEXT_BLOCK})
            ),
            tool_profile=lambda: profile,
        )
        handlers = SimpleNamespace(
            _live_memory_engine=lambda: engine,
            memorize=None,
            recall_memory=None,
            forget_memory=None,
            memory_signal=None,
        )
        registry = ToolRegistry()
        _register_memory_tools(registry, handlers)
        return set(registry.names())

    def test_akasha_shaped_profile_registers_no_memorize(self) -> None:
        spec = lambda name="": SimpleNamespace(  # noqa: E731
            name=name, description="d", parameters={"type": "object", "properties": {}}
        )
        names = self._registry_for(
            SimpleNamespace(
                recall=spec(), memorize=None, forget=None,
                tools=(spec("reinforce_memory"),),
            )
        )
        self.assertEqual(names, {"recall_memory", "reinforce_memory"})
        # 关键:引擎没声明 memorize 就不注册,不能退回旧 schema
        self.assertNotIn("memorize", names)

    def test_default_shaped_profile_registers_the_trio(self) -> None:
        spec = SimpleNamespace(
            name="", description="d", parameters={"type": "object", "properties": {}}
        )
        names = self._registry_for(
            SimpleNamespace(recall=spec, memorize=spec, forget=spec, tools=())
        )
        self.assertEqual(names, {"recall_memory", "memorize", "forget_memory"})

    def test_custom_tool_without_name_fails_loud(self) -> None:
        spec = SimpleNamespace(
            name="", description="d", parameters={"type": "object", "properties": {}}
        )
        with self.assertRaises(ValueError):
            self._registry_for(
                SimpleNamespace(recall=None, memorize=None, forget=None, tools=(spec,))
            )

    def test_disabled_engine_keeps_legacy_trio(self) -> None:
        """kirakira 的显式偏离:引擎未承重时仍注册词法版三件套。"""
        from agent.tools.builtins import _register_memory_tools
        from agent.tools.registry import ToolRegistry

        handlers = SimpleNamespace(
            _live_memory_engine=lambda: None,
            memorize=None, recall_memory=None, forget_memory=None, memory_signal=None,
        )
        registry = ToolRegistry()
        _register_memory_tools(registry, handlers)
        self.assertEqual(set(registry.names()), {"recall_memory", "memorize", "forget_memory"})


class SessionMessagesProjectionTests(unittest.TestCase):
    """akasha 读的 messages 投影表与 messages_fts。"""

    def test_projection_tracks_sessions(self) -> None:
        import tempfile

        from session.manager import SessionManager

        with tempfile.TemporaryDirectory() as tmp:
            manager = SessionManager(Path(tmp))
            session = manager.get_or_create("web:u1")
            session.add_message("user", "部署脚本在 scripts/rollout.sh")
            session.add_message("assistant", "记住了")
            manager.save(session)

            rows = manager._index.execute(
                "SELECT id, session_key, seq, role, content FROM messages ORDER BY seq"
            ).fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0][0], session.messages[0]["id"])
            self.assertEqual(rows[0][2], 0)
            self.assertEqual(rows[1][3], "assistant")
            # 外部内容 FTS 随触发器增量维护
            hits = manager._index.execute(
                "SELECT COUNT(*) FROM messages_fts WHERE content MATCH ?", ("rollout",)
            ).fetchone()[0]
            self.assertGreaterEqual(hits, 1)

            manager.delete_session("web:u1")
            left = manager._index.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            self.assertEqual(left, 0)  # 删会话同时清投影,不留孤儿行
            manager.close()

    def test_index_lives_at_reference_path(self) -> None:
        import tempfile

        from session.manager import SessionManager

        with tempfile.TemporaryDirectory() as tmp:
            manager = SessionManager(Path(tmp))
            manager.close()
            # akasha 硬编码读 workspace/sessions.db
            self.assertTrue((Path(tmp) / "sessions.db").exists())


class LocalIntegrityTests(unittest.TestCase):
    def test_akasha_runtime_has_no_external_checkout_dependency(self) -> None:
        from plugins.akasha.engine import AkashaMemoryEngine
        from plugins.akasha.memory_plugin import MemoryPlugin

        self.assertEqual(AkashaMemoryEngine.__name__, "AkashaMemoryEngine")
        self.assertEqual(MemoryPlugin.__name__, "MemoryPlugin")


if __name__ == "__main__":
    unittest.main()
