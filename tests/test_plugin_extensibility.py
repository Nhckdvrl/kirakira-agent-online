"""插件声明式扩展点契约(照 Reference agent/plugins)。

覆盖三条:
1. 插件声明 ProactiveSourceSpec → 编译成真实 ProactiveSource(fetch/ack 走 MCP 工具);
2. PluginJobHost 按 interval / event 触发作业,带去抖,并能干净关停;
3. 插件类定义即注册(plugin_registry)。
"""

from __future__ import annotations

import asyncio
import json
import unittest

from bus.event_bus import EventBus
from agent.plugins.jobs import (
    EventTrigger,
    IntervalTrigger,
    PluginJobHost,
    PluginJobSpec,
)
from agent.plugins.registry import plugin_registry
from agent.plugins.specs import (
    PluginSemanticCheck,
    ProactiveSourceSpec,
    RegisteredProactiveSource,
    proactive_source_key,
)
from plugins.wake_proactive.mcp_sources import (
    McpProactiveSource,
    compile_proactive_sources,
)
from plugins.wake_proactive.sources import SourceRegistry
from core.schema import ToolResult


class _FakeToolRegistry:
    """最小 ToolRegistry 替身:按 mcp_<server>__<tool> 命名约定返回 JSON。"""

    def __init__(self, payloads: dict[str, object]) -> None:
        self._payloads = payloads
        self.calls: list[tuple[str, dict]] = []

    def names(self) -> list[str]:
        return list(self._payloads)

    async def execute_async(self, call):
        self.calls.append((call.name, dict(call.arguments)))
        payload = self._payloads[call.name]
        text = payload if isinstance(payload, str) else json.dumps(payload)
        return ToolResult(tool_call_id=call.id, content=text)


def _registered(**kwargs) -> RegisteredProactiveSource:
    spec = ProactiveSourceSpec(**kwargs)
    return RegisteredProactiveSource(plugin_id="demo", spec=spec)


class ProactiveSourceCompilationTests(unittest.TestCase):
    def test_spec_compiles_into_working_source(self) -> None:
        async def scenario() -> None:
            tools = _FakeToolRegistry(
                {
                    "mcp_feed__pull": [
                        {"id": "e1", "kind": "alert", "title": "down"},
                        {"id": "e2", "kind": "content", "title": "news"},
                        {"id": "e3", "kind": "ignored_kind"},
                    ],
                    "mcp_feed__done": {"ok": True},
                }
            )
            sources = compile_proactive_sources(
                [
                    _registered(
                        id="feed",
                        channels=("alert", "content"),
                        server="feed",
                        fetch_tool="pull",
                        ack_tool="done",
                    )
                ],
                tools,
            )
            self.assertEqual(len(sources), 1)
            source = sources[0]
            self.assertIsInstance(source, McpProactiveSource)
            self.assertEqual(source.id, "demo:feed")

            events = await source.fetch()
            # 未声明的 kind 被丢弃,只保留 alert/content
            self.assertEqual([e["kind"] for e in events], ["alert", "content"])
            self.assertEqual(events[0]["event_id"], "e1")
            self.assertEqual(events[0]["ack_server"], "demo:feed")

            await source.ack(["e1"])
            self.assertEqual(tools.calls[-1][0], "mcp_feed__done")
            self.assertEqual(tools.calls[-1][1], {"event_ids": ["e1"]})

        asyncio.run(scenario())

    def test_alert_without_event_id_fails_loud(self) -> None:
        async def scenario() -> None:
            tools = _FakeToolRegistry({"mcp_feed__pull": [{"kind": "alert"}]})
            source = compile_proactive_sources(
                [_registered(id="feed", channels=("alert",), server="feed", fetch_tool="pull")],
                tools,
            )[0]
            with self.assertRaises(RuntimeError):
                await source.fetch()

        asyncio.run(scenario())

    def test_context_source_accepts_dict_snapshot(self) -> None:
        async def scenario() -> None:
            tools = _FakeToolRegistry({"mcp_env__snapshot": {"battery": 42}})
            source = compile_proactive_sources(
                [
                    _registered(
                        id="env",
                        channels=("context",),
                        server="env",
                        fetch_tool="snapshot",
                    )
                ],
                tools,
            )[0]
            events = await source.fetch()
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["kind"], "context")
            self.assertEqual(events[0]["battery"], 42)
            # 只读 context 源没有 ack_tool,ACK 是空操作
            await source.ack(["whatever"])

        asyncio.run(scenario())

    def test_missing_mcp_tool_fails_loud(self) -> None:
        async def scenario() -> None:
            source = compile_proactive_sources(
                [_registered(id="x", channels=("alert",), server="gone", fetch_tool="pull")],
                _FakeToolRegistry({}),
            )[0]
            with self.assertRaises(RuntimeError):
                await source.fetch()

        asyncio.run(scenario())

    def test_compiled_source_plugs_into_registry(self) -> None:
        async def scenario() -> None:
            tools = _FakeToolRegistry(
                {"mcp_feed__pull": [{"id": "a1", "kind": "alert", "title": "x"}]}
            )
            registry = SourceRegistry()
            for source in compile_proactive_sources(
                [_registered(id="feed", channels=("alert",), server="feed", fetch_tool="pull")],
                tools,
            ):
                registry.add(source)
            grouped = await registry.fetch_all()
            self.assertEqual(len(grouped["alert"]), 1)
            self.assertEqual(grouped["alert"][0]["event_id"], "a1")

        asyncio.run(scenario())


class _Ping:
    pass


class PluginJobHostTests(unittest.TestCase):
    def test_interval_job_fires(self) -> None:
        async def scenario() -> None:
            fired: list[str] = []

            async def handler(ctx) -> None:
                fired.append(ctx.reason)

            host = PluginJobHost()
            host.register(
                "demo",
                PluginJobSpec(id="tick", triggers=[IntervalTrigger(seconds=1)], handler=handler),
            )
            host.start()
            await asyncio.sleep(1.15)
            await host.aclose()
            self.assertIn("interval", fired)

        asyncio.run(scenario())

    def test_event_job_fires_and_debounces(self) -> None:
        async def scenario() -> None:
            fired: list[object] = []

            async def handler(ctx) -> None:
                fired.append(ctx.event)

            bus = EventBus()
            host = PluginJobHost(event_bus=bus)
            host.register(
                "demo",
                PluginJobSpec(
                    id="on_ping",
                    triggers=[EventTrigger(event_type=_Ping)],
                    handler=handler,
                    debounce_seconds=60,
                ),
            )
            host.start()
            bus.enqueue(_Ping())
            bus.enqueue(_Ping())
            await asyncio.sleep(0.1)
            await host.aclose()
            # 去抖:60s 内第二次不再触发
            self.assertEqual(len(fired), 1)

        asyncio.run(scenario())

    def test_handler_failure_does_not_kill_host(self) -> None:
        async def scenario() -> None:
            calls: list[str] = []

            async def boom(ctx) -> None:
                calls.append("called")
                raise RuntimeError("job blew up")

            bus = EventBus()
            host = PluginJobHost(event_bus=bus)
            host.register(
                "demo",
                PluginJobSpec(id="bad", triggers=[EventTrigger(event_type=_Ping)], handler=boom),
            )
            host.start()
            bus.enqueue(_Ping())
            await asyncio.sleep(0.1)
            self.assertEqual(calls, ["called"])
            await host.aclose()

        asyncio.run(scenario())

    def test_duplicate_job_id_rejected(self) -> None:
        async def handler(ctx) -> None:
            return None

        host = PluginJobHost()
        spec = PluginJobSpec(id="dup", triggers=[], handler=handler)
        host.register("demo", spec)
        with self.assertRaises(ValueError):
            host.register("demo", spec)


class PluginSpecTests(unittest.TestCase):
    def test_source_key(self) -> None:
        self.assertEqual(
            proactive_source_key(_registered(id="s", channels=("alert",), server="v", fetch_tool="f")),
            "demo:s",
        )

    def test_semantic_check_helpers(self) -> None:
        self.assertTrue(PluginSemanticCheck.ok("c").passed)
        self.assertFalse(PluginSemanticCheck.fail("c", "why").passed)

    def test_plugin_subclass_auto_registers(self) -> None:
        from agent.plugins import Plugin

        class _Demo(Plugin):
            name = "demo-autoreg"

        self.assertIs(plugin_registry.get_class(_Demo.__module__), _Demo)


if __name__ == "__main__":
    unittest.main()


class PluginSourceRuntimeWiringTests(unittest.TestCase):
    """插件声明的主动源必须真正进入 runtime 的 SourceRegistry。

    编译器早先就写好了,但一度没有接线——声明了却没通电等于没有这个能力。
    """

    def test_cli_source_registry_includes_plugin_declared_sources(self) -> None:
        import tempfile
        from pathlib import Path

        from bootstrap.app import _build_source_registry

        tools = _FakeToolRegistry({"mcp_feed__pull": []})
        registered = [
            _registered(
                id="feed", channels=("alert",), server="feed", fetch_tool="pull"
            )
        ]
        with tempfile.TemporaryDirectory() as tmp:
            registry = _build_source_registry(Path(tmp), registered, tools)
        ids = [source.id for source in registry.sources]
        self.assertIn("demo:feed", ids)

    def test_registry_survives_duplicate_source_ids(self) -> None:
        import tempfile
        from pathlib import Path

        from bootstrap.app import _build_source_registry

        tools = _FakeToolRegistry({"mcp_feed__pull": []})
        dup = _registered(
            id="feed", channels=("alert",), server="feed", fetch_tool="pull"
        )
        with tempfile.TemporaryDirectory() as tmp:
            registry = _build_source_registry(Path(tmp), [dup, dup], tools)
        # 重复 id 只跳过冲突的那个,不影响整条链路
        self.assertEqual(
            len([s for s in registry.sources if s.id == "demo:feed"]), 1
        )

    def test_no_plugin_sources_still_prepares_file_inbox(self) -> None:
        import tempfile
        from pathlib import Path

        from bootstrap.app import _build_source_registry

        with tempfile.TemporaryDirectory() as tmp:
            registry = _build_source_registry(Path(tmp), None, None)
            # 没有 *.jsonl 时源为空是既有语义;关键是 inbox 目录被准备好且不报错
            self.assertEqual(registry.sources, [])
            self.assertTrue((Path(tmp) / "proactive" / "inbox").is_dir())

    def test_plugin_sources_coexist_with_file_inbox_sources(self) -> None:
        import tempfile
        from pathlib import Path

        from bootstrap.app import _build_source_registry

        tools = _FakeToolRegistry({"mcp_feed__pull": []})
        with tempfile.TemporaryDirectory() as tmp:
            inbox = Path(tmp) / "proactive" / "inbox"
            inbox.mkdir(parents=True, exist_ok=True)
            (inbox / "local.jsonl").write_text("", encoding="utf-8")
            registry = _build_source_registry(
                Path(tmp),
                [_registered(id="feed", channels=("alert",), server="feed", fetch_tool="pull")],
                tools,
            )
        ids = {source.id for source in registry.sources}
        self.assertIn("local", ids)
        self.assertIn("demo:feed", ids)
