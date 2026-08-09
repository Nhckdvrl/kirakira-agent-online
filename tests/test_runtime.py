"""Tests for the Akashic-style passive runtime."""

import asyncio
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path

from bus.queue import MessageBus
from agent.prompting.context_builder import ContextBuilder
from bus.event_bus import EventBus
from bus.events import InboundMessage, OutboundMessage
from core.memory.legacy import MemoryRuntime
from agent.model_runtime.types import ContextLengthError
from agent.core.runtime import AgentLoop, DefaultReasoner, PassiveTurnPipeline, RuntimeConfig
from core.schema import ModelResponse, ToolCall, ToolSpec
from session.manager import SessionManager
from agent.plugins.snapshot import RuntimeSnapshotStore, compile_snapshot
from agent.subagent import SubagentManager
from agent.tool_hooks import HookOutcome
from agent.tools import build_default_registry
from agent.tools.registry import Tool
from bus.events_lifecycle import (
    ContextBudgetUpdated,
    ContextPrepared,
    StreamDeltaReady,
    ToolCallCompleted,
    ToolCallStarted,
    TurnFinished,
    TurnStarted,
)


class FakeModel:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def complete(self, messages, tools, system, model, max_tokens):
        self.calls.append({"messages": list(messages), "tools": list(tools)})
        return self.responses.pop(0)


def _snapshot_tool(name, reply):
    async def handler(**_kwargs):
        return reply

    return Tool(
        spec=ToolSpec(name, "snapshot tool %s" % name, {"type": "object", "properties": {}}),
        handler=handler,
        deferred=True,
    )


def _tool_result_for(session_manager, session_key, tool_name):
    """从已保存的 session tool chain 里取出某个工具的执行结果。"""

    session = session_manager.get_or_create(session_key)
    for message in session.messages:
        for group in message.get("tool_chain") or []:
            for call in group.get("calls") or []:
                if call.get("name") == tool_name:
                    return call.get("result")
    raise AssertionError("tool %s was never called" % tool_name)


def build_test_runtime(workdir, model, *, snapshot_store=None):
    bus = MessageBus()
    event_bus = EventBus()
    session_manager = SessionManager(workdir)
    memory = MemoryRuntime(workdir, session_manager=session_manager)
    tools = build_default_registry(workdir, memory=memory, session_manager=session_manager)
    context = ContextBuilder(workdir, memory)
    config = RuntimeConfig(model="fake", max_iterations=5, max_tokens=1000, history_window=20)
    reasoner = DefaultReasoner(
        model_client=model,
        tools=tools,
        config=config,
        context=context,
        event_bus=event_bus,
    )
    pipeline = PassiveTurnPipeline(
        bus=bus,
        event_bus=event_bus,
        session_manager=session_manager,
        memory=memory,
        tools=tools,
        reasoner=reasoner,
        config=config,
        snapshot_store=snapshot_store,
    )
    return bus, AgentLoop(bus=bus, pipeline=pipeline), session_manager, memory


class RuntimeTests(unittest.TestCase):
    def test_agent_loop_parallelizes_sessions_but_serializes_same_session(self):
        class RecordingPipeline:
            def __init__(self, bus):
                self.bus = bus
                self.events = []

            async def run(self, message, key):
                self.events.append(("start", key, message.content))
                if message.content == "first":
                    await asyncio.sleep(0.05)
                elif message.content == "other":
                    await asyncio.sleep(0.01)
                self.events.append(("end", key, message.content))
                await self.bus.publish_outbound(
                    OutboundMessage(message.channel, message.chat_id, message.content)
                )

        async def scenario():
            bus = MessageBus()
            pipeline = RecordingPipeline(bus)
            loop = AgentLoop(bus=bus, pipeline=pipeline)
            received = []

            async def collect(message):
                received.append(message.content)
                if len(received) == 3:
                    loop.stop()
                    bus.stop()

            bus.subscribe_outbound("cli", collect)
            tasks = [
                asyncio.create_task(loop.run()),
                asyncio.create_task(bus.dispatch_outbound()),
            ]
            await bus.publish_inbound(InboundMessage("cli", "u", "same", "first"))
            await bus.publish_inbound(InboundMessage("cli", "u", "same", "second"))
            await bus.publish_inbound(InboundMessage("cli", "u", "other", "other"))
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=3)

            self.assertLess(
                pipeline.events.index(("end", "cli:other", "other")),
                pipeline.events.index(("end", "cli:same", "first")),
            )
            self.assertLess(
                pipeline.events.index(("end", "cli:same", "first")),
                pipeline.events.index(("start", "cli:same", "second")),
            )

        asyncio.run(scenario())

    def test_interrupt_persists_resumable_turn_marker(self):
        class SlowModel:
            def __init__(self):
                self.started = threading.Event()

            def complete(self, messages, tools, system, model, max_tokens):
                self.started.set()
                time.sleep(0.2)
                return ModelResponse(text="too late")

        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                model = SlowModel()
                bus, loop, sessions, _memory = build_test_runtime(Path(tmp), model)
                loop_task = asyncio.create_task(loop.run())
                await bus.publish_inbound(
                    InboundMessage("cli", "tester", "interrupt", "long task")
                )
                await asyncio.to_thread(model.started.wait, 1)

                self.assertTrue(loop.request_interrupt("cli:interrupt"))
                await asyncio.wait_for(bus._inbound.join(), timeout=2)
                loop.stop()
                await loop_task

                session = sessions.get_or_create("cli:interrupt")
                self.assertEqual(session.messages[-1]["content"], "[interrupted]")
                self.assertTrue(session.messages[-1]["interrupted"])

        asyncio.run(scenario())

    def test_streaming_model_emits_delta_lifecycle_events(self):
        class StreamingModel:
            def complete_stream(
                self, messages, tools, system, model, max_tokens, on_delta
            ):
                on_delta("你", "先想")
                on_delta("好", "")
                return ModelResponse(text="你好", reasoning_content="先想")

        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                _bus, loop, _sessions, _memory = build_test_runtime(
                    Path(tmp), StreamingModel()
                )
                deltas = []
                loop.pipeline.event_bus.on(
                    StreamDeltaReady,
                    lambda event: deltas.append(
                        (event.content_delta, event.reasoning_delta)
                    ),
                )

                outbound = await loop.pipeline.run(
                    InboundMessage("cli", "tester", "chat", "hello"),
                    "cli:chat",
                    dispatch_outbound=False,
                )

                self.assertEqual(outbound.content, "你好")
                self.assertEqual(deltas, [("你", "先想"), ("好", "")])

        asyncio.run(scenario())

    def test_spawn_runs_isolated_child_and_disables_recursive_spawn(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                model = FakeModel(
                    [
                        ModelResponse(
                            tool_calls=[
                                ToolCall("parent_call", "spawn", {"task": "inspect project"})
                            ]
                        ),
                        ModelResponse(text="child result"),
                        ModelResponse(text="parent result"),
                    ]
                )
                _bus, loop, sessions, memory = build_test_runtime(Path(tmp), model)
                manager = SubagentManager(
                    reasoner=loop.pipeline.reasoner,
                    tools=loop.pipeline.tools,
                    sessions=sessions,
                    memory=memory,
                    bus=_bus,
                )
                manager.register_tool()

                outbound = await loop.pipeline.run(
                    InboundMessage("cli", "tester", "chat", "delegate"),
                    "cli:chat",
                    dispatch_outbound=False,
                )

                self.assertEqual(outbound.content, "parent result")
                self.assertNotIn("spawn", [spec.name for spec in model.calls[1]["tools"]])
                child_files = [
                    path
                    for path in (Path(tmp) / "sessions").glob("*.json")
                    if "sub_" in path.name
                ]
                self.assertEqual(len(child_files), 1)

        asyncio.run(scenario())

    def test_background_spawn_reinjects_completion_into_parent_session(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                model = FakeModel([ModelResponse(text="background result")])
                bus, loop, sessions, memory = build_test_runtime(Path(tmp), model)
                manager = SubagentManager(
                    reasoner=loop.pipeline.reasoner,
                    tools=loop.pipeline.tools,
                    sessions=sessions,
                    memory=memory,
                    bus=bus,
                )
                manager.register_tool()
                token = loop.pipeline.tools.set_context(
                    session_key="web:parent",
                    channel="web",
                    chat_id="parent",
                )
                try:
                    accepted = json.loads(
                        await manager.spawn("inspect project", mode="background")
                    )
                finally:
                    loop.pipeline.tools.reset_context(token)

                self.assertEqual(accepted["status"], "running")
                self.assertTrue(await manager.wait(timeout=2.0))
                completion = await asyncio.wait_for(bus.consume_inbound(), timeout=1.0)
                self.assertEqual(completion.session_key, "web:parent")
                self.assertTrue(completion.metadata["omit_user_turn"])
                self.assertTrue(completion.metadata["subagent_completion"])
                self.assertIn("background result", completion.content)
                await bus.complete_inbound(completion)
                await manager.shutdown()

        asyncio.run(scenario())

    def test_background_spawn_management_limit_and_cancel(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                model = FakeModel([])
                bus, loop, sessions, memory = build_test_runtime(Path(tmp), model)
                manager = SubagentManager(
                    reasoner=loop.pipeline.reasoner,
                    tools=loop.pipeline.tools,
                    sessions=sessions,
                    memory=memory,
                    bus=bus,
                )
                manager.register_tool()
                release = asyncio.Event()

                async def blocked_child(*args, **kwargs):
                    await release.wait()
                    return {
                        "task_id": args[0],
                        "status": "completed",
                        "result": "done",
                    }

                manager._run_child = blocked_child
                token = loop.pipeline.tools.set_context(
                    session_key="web:parent",
                    channel="web",
                    chat_id="parent",
                )
                try:
                    accepted = [
                        json.loads(
                            await manager.spawn(
                                "job %d" % index,
                                mode="background",
                                label="job-%d" % index,
                            )
                        )
                        for index in range(3)
                    ]
                    rejected = await manager.spawn("job 4", mode="background")
                finally:
                    loop.pipeline.tools.reset_context(token)

                listing = json.loads(await manager.manage("list"))
                self.assertEqual(listing["running_count"], 3)
                self.assertIn("limit 3", rejected)
                self.assertTrue(await manager.cancel(accepted[0]["task_id"]))
                cancelled = await asyncio.wait_for(bus.consume_inbound(), timeout=1.0)
                self.assertIn('status="cancelled"', cancelled.content)
                await bus.complete_inbound(cancelled)

                release.set()
                self.assertTrue(await manager.wait(timeout=2.0))
                self.assertEqual(json.loads(await manager.manage("list"))["running_count"], 0)
                await manager.shutdown()

        asyncio.run(scenario())

    def test_tool_search_unlocks_deferred_tool_for_next_iteration(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                model = FakeModel(
                    [
                        ModelResponse(
                            tool_calls=[
                                ToolCall(
                                    "search", "tool_search", {"query": "select:remote_demo"}
                                )
                            ]
                        ),
                        ModelResponse(
                            tool_calls=[ToolCall("remote", "remote_demo", {})]
                        ),
                        ModelResponse(text="done"),
                    ]
                )
                _bus, loop, _sessions, _memory = build_test_runtime(Path(tmp), model)
                loop.pipeline.tools.register(
                    ToolSpec(
                        "remote_demo",
                        "Deferred remote demo",
                        {"type": "object", "properties": {}, "required": []},
                    ),
                    lambda: "remote ok",
                    deferred=True,
                )

                await loop.pipeline.run(
                    InboundMessage("cli", "tester", "chat", "use remote"),
                    "cli:chat",
                    dispatch_outbound=False,
                )

                self.assertNotIn(
                    "remote_demo", [spec.name for spec in model.calls[0]["tools"]]
                )
                self.assertIn(
                    "remote_demo", [spec.name for spec in model.calls[1]["tools"]]
                )

        asyncio.run(scenario())

    def test_turn_pins_snapshot_tools_across_mid_turn_hot_reload(self):
        """turn 中途换代时，本轮必须继续看到并调用它开始时锁定的那代工具。"""

        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                store = RuntimeSnapshotStore()
                gen1 = compile_snapshot(
                    mcp_tools={"mcp_a__ping": _snapshot_tool("mcp_a__ping", "gen1-pong")},
                    mcp_generation_id="gen1",
                    revision="1",
                )
                await store.commit(store.publish(gen1))

                swapped = asyncio.Event()

                async def swap_generation(**_kwargs):
                    # 模拟 watcher 在本轮工具调用之后立刻完成换代。
                    await store.commit(
                        store.publish(
                            compile_snapshot(
                                mcp_tools={
                                    "mcp_b__ping": _snapshot_tool("mcp_b__ping", "gen2")
                                },
                                mcp_generation_id="gen2",
                                revision="2",
                            )
                        )
                    )
                    swapped.set()
                    return "swapped"

                model = FakeModel(
                    [
                        ModelResponse(
                            tool_calls=[ToolCall("s", "trigger_reload", {})]
                        ),
                        ModelResponse(
                            tool_calls=[
                                ToolCall("search", "tool_search", {"query": "select:mcp_a__ping"})
                            ]
                        ),
                        ModelResponse(tool_calls=[ToolCall("p", "mcp_a__ping", {})]),
                        ModelResponse(text="done"),
                    ]
                )
                _bus, loop, _sessions, _memory = build_test_runtime(
                    Path(tmp), model, snapshot_store=store
                )
                loop.pipeline.tools.register(
                    ToolSpec(
                        "trigger_reload",
                        "swap the runtime snapshot mid-turn",
                        {"type": "object", "properties": {}, "required": []},
                    ),
                    swap_generation,
                )

                outbound = await loop.pipeline.run(
                    InboundMessage("cli", "tester", "chat", "ping"),
                    "cli:chat",
                    dispatch_outbound=False,
                )

                self.assertTrue(swapped.is_set())
                self.assertEqual(outbound.content, "done")
                # 换代后本轮 tool_search 仍然只看得到自己那一代的 MCP 工具。
                search_result = json.loads(
                    _tool_result_for(_sessions, "cli:chat", "tool_search")
                )
                self.assertEqual(search_result["unlocked"], ["mcp_a__ping"])
                # 并且真的调用到了旧代际的实现，而不是报工具不存在。
                self.assertEqual(
                    _tool_result_for(_sessions, "cli:chat", "mcp_a__ping"), "gen1-pong"
                )
                # 全局 current 已经切到新代际。
                self.assertEqual(store.current.mcp_tool_names, ("mcp_b__ping",))
                # 本轮租约释放后，旧代际才允许排空。
                self.assertEqual(gen1.state, "drained")

        asyncio.run(scenario())

    def test_context_length_error_retries_with_trimmed_history(self):
        class OverflowOnceModel:
            def __init__(self):
                self.calls = []

            def complete(self, messages, tools, system, model, max_tokens):
                self.calls.append([dict(message) for message in messages])
                if len(self.calls) == 1:
                    raise ContextLengthError("too long")
                return ModelResponse(text="recovered")

        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                model = OverflowOnceModel()
                _bus, loop, sessions, _memory = build_test_runtime(Path(tmp), model)
                prepared = []
                budgets = []
                loop.pipeline.event_bus.on(ContextPrepared, prepared.append)
                loop.pipeline.event_bus.on(ContextBudgetUpdated, budgets.append)
                session = sessions.get_or_create("cli:chat")
                for index in range(8):
                    session.add_message("user", "u%d %s" % (index, "x" * 200))
                    session.add_message("assistant", "a%d %s" % (index, "y" * 200))
                original_history = [dict(message) for message in session.messages]

                outbound = await loop.pipeline.run(
                    InboundMessage("cli", "tester", "chat", "current"),
                    "cli:chat",
                    dispatch_outbound=False,
                )

                self.assertEqual(outbound.content, "recovered")
                self.assertEqual(len(model.calls), 2)
                self.assertLessEqual(len(model.calls[1]), len(model.calls[0]))
                self.assertEqual([item.plan_name for item in prepared], ["full", "trim_skills_catalog"])
                saved = sessions.get_or_create("cli:chat")
                trace = saved.messages[-1]["context_trace"]
                self.assertEqual(trace["selected_plan"], "trim_skills_catalog")
                self.assertEqual(len(trace["attempts"]), 2)
                self.assertEqual(len(budgets), 1)
                self.assertEqual(
                    saved.metadata["context_budget"]["selected_plan"],
                    "trim_skills_catalog",
                )
                # Prompt degradation is a per-attempt runtime view. It must never
                # rewrite or delete the durable conversation history.
                self.assertEqual(saved.messages[: len(original_history)], original_history)

        asyncio.run(scenario())

    def test_react_usage_counts_requests_without_provider_telemetry(self):
        stats = DefaultReasoner._react_stats(
            [120, 150],
            [
                {"prompt_tokens": 100, "completion_tokens": 20},
                {},
            ],
        )

        self.assertEqual(stats["iteration_count"], 2)
        self.assertEqual(stats["request_count"], 2)
        self.assertEqual(stats["model_usage"]["covered_request_count"], 1)
        self.assertEqual(stats["model_usage"]["coverage"], "partial")

    def test_before_turn_history_rewrite_reaches_model(self):
        class RewriteHistory:
            def run(self, ctx):
                ctx.history_messages = (
                    {"role": "user", "content": "plugin history"},
                    {"role": "assistant", "content": "plugin answer"},
                )
                return ctx

        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                model = FakeModel([ModelResponse(text="done")])
                _bus, loop, sessions, _memory = build_test_runtime(Path(tmp), model)
                session = sessions.get_or_create("cli:chat")
                session.add_message("user", "old history")
                session.add_message("assistant", "old answer")
                loop.pipeline.add_before_turn_plugin_modules([RewriteHistory()])

                await loop.pipeline.run(
                    InboundMessage("cli", "tester", "chat", "current"),
                    "cli:chat",
                    dispatch_outbound=False,
                )

                contents = [msg.get("content") for msg in model.calls[0]["messages"]]
                self.assertIn("plugin history", contents)
                self.assertNotIn("old history", contents)

        asyncio.run(scenario())

    def test_disabled_tool_is_hidden_and_direct_call_is_denied(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                model = FakeModel(
                    [
                        ModelResponse(
                            tool_calls=[
                                ToolCall("call_1", "read_file", {"path": "secret.txt"})
                            ]
                        ),
                        ModelResponse(text="blocked"),
                    ]
                )
                _bus, loop, sessions, _memory = build_test_runtime(Path(tmp), model)
                events = []
                loop.pipeline.event_bus.on(
                    ToolCallCompleted,
                    lambda event: events.append((event.tool_name, event.status)),
                )

                await loop.pipeline.run(
                    InboundMessage(
                        "cli",
                        "tester",
                        "chat",
                        "read",
                        metadata={"disabled_tools": ["read_file"]},
                    ),
                    "cli:chat",
                    dispatch_outbound=False,
                )

                self.assertNotIn(
                    "read_file", [spec.name for spec in model.calls[0]["tools"]]
                )
                call = sessions.get_or_create("cli:chat").messages[-1]["tool_chain"][0]["calls"][0]
                self.assertEqual(call["status"], "denied")
                self.assertIn(("read_file", "denied"), events)

        asyncio.run(scenario())

    def test_failed_builtin_tool_is_recorded_as_error(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                model = FakeModel(
                    [
                        ModelResponse(
                            tool_calls=[
                                ToolCall("call_1", "read_file", {"path": "missing.txt"})
                            ]
                        ),
                        ModelResponse(text="failed as expected"),
                    ]
                )
                _bus, loop, sessions, _memory = build_test_runtime(Path(tmp), model)

                await loop.pipeline.run(
                    InboundMessage("cli", "tester", "chat", "read"),
                    "cli:chat",
                    dispatch_outbound=False,
                )

                call = sessions.get_or_create("cli:chat").messages[-1]["tool_chain"][0]["calls"][0]
                self.assertEqual(call["status"], "error")

        asyncio.run(scenario())

    def test_turn_persistence_flags(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                model = FakeModel([ModelResponse(text="ack")])
                _bus, loop, sessions, memory = build_test_runtime(Path(tmp), model)

                await loop.pipeline.run(
                    InboundMessage(
                        "cli",
                        "tester",
                        "chat",
                        "请记住：不应写入",
                        metadata={"omit_user_turn": True, "skip_post_memory": True},
                    ),
                    "cli:chat",
                    dispatch_outbound=False,
                )

                session = sessions.get_or_create("cli:chat")
                self.assertEqual([msg["role"] for msg in session.messages], ["assistant"])
                self.assertEqual(memory.recall("不应写入"), [])

        asyncio.run(scenario())

    def test_bus_to_loop_to_outbound_and_session_tool_chain(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                workdir = Path(tmp)
                (workdir / "README.md").write_text("hello")
                model = FakeModel(
                    [
                        ModelResponse(tool_calls=[ToolCall("call_1", "read_file", {"path": "README.md"})]),
                        ModelResponse(text="读到了 hello。"),
                    ]
                )
                bus, loop, sessions, _memory = build_test_runtime(workdir, model)
                got = []

                async def collect(msg):
                    got.append(msg)
                    loop.stop()
                    bus.stop()

                bus.subscribe_outbound("cli", collect)
                tasks = [
                    asyncio.create_task(loop.run()),
                    asyncio.create_task(bus.dispatch_outbound()),
                ]
                await bus.publish_inbound(InboundMessage("cli", "tester", "chat", "read README"))
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)

                self.assertEqual(got[0].content, "读到了 hello。")
                session = sessions.get_or_create("cli:chat")
                self.assertEqual(session.messages[-1]["tools_used"], ["read_file"])
                self.assertEqual(session.messages[-1]["tool_chain"][0]["calls"][0]["result"], "hello")

        asyncio.run(scenario())


    def test_memorize_tool_does_not_duplicate_consolidation_memory(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                workdir = Path(tmp)
                model = FakeModel(
                    [
                        ModelResponse(tool_calls=[ToolCall("call_1", "memorize", {"content": "我喜欢蓝色"})]),
                        ModelResponse(text="记住了。"),
                    ]
                )
                bus, loop, _sessions, memory = build_test_runtime(workdir, model)

                async def collect(_msg):
                    loop.stop()
                    bus.stop()

                bus.subscribe_outbound("cli", collect)
                tasks = [
                    asyncio.create_task(loop.run()),
                    asyncio.create_task(bus.dispatch_outbound()),
                ]
                await bus.publish_inbound(InboundMessage("cli", "tester", "chat", "请记住：我喜欢蓝色"))
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)

                recalled = memory.recall("蓝色", limit=10)
                self.assertEqual([r.content for r in recalled].count("我喜欢蓝色"), 1)

        asyncio.run(scenario())

    def test_before_turn_module_can_abort(self):
        class AbortModule:
            async def run(self, ctx):
                ctx.abort = True
                ctx.abort_reply = "blocked by plugin"
                return ctx

        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                workdir = Path(tmp)
                model = FakeModel([ModelResponse(text="should not be used")])
                bus, loop, _sessions, _memory = build_test_runtime(workdir, model)
                loop.pipeline.add_before_turn_plugin_modules([AbortModule()])
                got = []

                async def collect(msg):
                    got.append(msg.content)
                    loop.stop()
                    bus.stop()

                bus.subscribe_outbound("cli", collect)
                tasks = [
                    asyncio.create_task(loop.run()),
                    asyncio.create_task(bus.dispatch_outbound()),
                ]
                await bus.publish_inbound(InboundMessage("cli", "tester", "chat", "hello"))
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)

                self.assertEqual(got, ["blocked by plugin"])
                self.assertEqual(model.calls, [])

        asyncio.run(scenario())

    def test_slash_command_can_abort_before_memory_retrieval(self):
        class CommandModule:
            def run(self, ctx):
                ctx.abort = True
                ctx.abort_reply = "command handled"
                return ctx

        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                model = FakeModel([ModelResponse(text="unused")])
                _bus, loop, _sessions, memory = build_test_runtime(Path(tmp), model)
                loop.pipeline.add_before_turn_plugin_modules([CommandModule()])

                def fail_if_called(_query):
                    raise AssertionError("memory retrieval should be skipped")

                memory.build_retrieval_block = fail_if_called
                outbound = await loop.pipeline.run(
                    InboundMessage("cli", "tester", "chat", "/status"),
                    "cli:chat",
                    dispatch_outbound=False,
                )

                self.assertEqual(outbound.content, "command handled")
                self.assertEqual(model.calls, [])

        asyncio.run(scenario())

    def test_tool_hook_can_deny_tool(self):
        class DenyReadFile:
            name = "deny_read_file"
            event = "pre_tool_use"

            def matches(self, ctx):
                return ctx.request.tool_name == "read_file"

            async def run(self, ctx):
                return HookOutcome(decision="deny", reason="read_file denied")

        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                workdir = Path(tmp)
                (workdir / "README.md").write_text("hello")
                model = FakeModel(
                    [
                        ModelResponse(tool_calls=[ToolCall("call_1", "read_file", {"path": "README.md"})]),
                        ModelResponse(text="工具被拦截了。"),
                    ]
                )
                bus, loop, sessions, _memory = build_test_runtime(workdir, model)
                loop.pipeline.reasoner.add_tool_hooks([DenyReadFile()])

                async def collect(_msg):
                    loop.stop()
                    bus.stop()

                bus.subscribe_outbound("cli", collect)
                tasks = [
                    asyncio.create_task(loop.run()),
                    asyncio.create_task(bus.dispatch_outbound()),
                ]
                await bus.publish_inbound(InboundMessage("cli", "tester", "chat", "read README"))
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)

                session = sessions.get_or_create("cli:chat")
                call = session.messages[-1]["tool_chain"][0]["calls"][0]
                self.assertEqual(call["status"], "denied")
                self.assertEqual(call["result"], "read_file denied")

        asyncio.run(scenario())

    def test_runtime_emits_turn_and_tool_lifecycle_events(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                workdir = Path(tmp)
                (workdir / "README.md").write_text("hello")
                model = FakeModel(
                    [
                        ModelResponse(tool_calls=[ToolCall("call_1", "read_file", {"path": "README.md"})]),
                        ModelResponse(text="done"),
                    ]
                )
                bus, loop, _sessions, _memory = build_test_runtime(workdir, model)
                events = []
                loop.pipeline.event_bus.on(TurnStarted, lambda event: events.append(("turn", event.session_key)))
                loop.pipeline.event_bus.on(ToolCallStarted, lambda event: events.append(("tool_start", event.tool_name)))
                loop.pipeline.event_bus.on(ToolCallCompleted, lambda event: events.append(("tool_done", event.tool_name, event.status)))

                async def collect(_msg):
                    loop.stop()
                    bus.stop()

                bus.subscribe_outbound("cli", collect)
                tasks = [
                    asyncio.create_task(loop.run()),
                    asyncio.create_task(bus.dispatch_outbound()),
                ]
                await bus.publish_inbound(InboundMessage("cli", "tester", "chat", "read README"))
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)

                self.assertIn(("turn", "cli:chat"), events)
                self.assertIn(("tool_start", "read_file"), events)
                self.assertIn(("tool_done", "read_file", "success"), events)

        asyncio.run(scenario())

    def test_lifecycle_contract_correlates_iterations_and_finishes_authoritatively(self):
        class StreamingToolModel:
            def __init__(self):
                self.responses = [
                    (
                        [("checking", "thinking")],
                        ModelResponse(
                            text="checking",
                            tool_calls=[
                                ToolCall(
                                    "call_1", "read_file", {"path": "README.md"}
                                )
                            ],
                        ),
                    ),
                    (
                        [("final ", ""), ("answer", "")],
                        ModelResponse(text="final answer"),
                    ),
                ]

            def complete_stream(
                self, messages, tools, system, model, max_tokens, on_delta
            ):
                deltas, response = self.responses.pop(0)
                for content, reasoning in deltas:
                    on_delta(content, reasoning)
                return response

        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                workdir = Path(tmp)
                (workdir / "README.md").write_text("hello")
                _bus, loop, _sessions, _memory = build_test_runtime(
                    workdir, StreamingToolModel()
                )
                lifecycle = []
                event_bus = loop.pipeline.event_bus
                event_bus.on(
                    StreamDeltaReady,
                    lambda event: lifecycle.append(
                        ("delta", event.iteration, event.content_delta)
                    ),
                )
                event_bus.on(
                    ToolCallStarted,
                    lambda event: lifecycle.append(
                        ("tool_start", event.iteration, event.call_id)
                    ),
                )
                event_bus.on(
                    ToolCallCompleted,
                    lambda event: lifecycle.append(
                        ("tool_done", event.iteration, event.call_id)
                    ),
                )
                event_bus.on(
                    TurnFinished,
                    lambda event: lifecycle.append(("finished", event)),
                )

                outbound = await loop.pipeline.run(
                    InboundMessage("cli", "tester", "chat", "read README"),
                    "cli:chat",
                    dispatch_outbound=False,
                )

                self.assertEqual(outbound.content, "final answer")
                self.assertIn(("tool_start", 0, "call_1"), lifecycle)
                self.assertIn(("tool_done", 0, "call_1"), lifecycle)
                self.assertIn(("delta", 1, "final "), lifecycle)
                finished = next(item[1] for item in lifecycle if item[0] == "finished")
                self.assertEqual(finished.status, "success")
                self.assertIs(finished.outbound, outbound)
                self.assertEqual(finished.outbound.content, "final answer")
                self.assertFalse(finished.will_dispatch)
                self.assertGreaterEqual(finished.duration_seconds, 0)

        asyncio.run(scenario())

    def test_lifecycle_contract_emits_error_and_interrupt_terminal_states(self):
        class FailingModel:
            def complete(self, messages, tools, system, model, max_tokens):
                raise RuntimeError("model unavailable")

        class SlowModel:
            def __init__(self):
                self.started = threading.Event()

            def complete(self, messages, tools, system, model, max_tokens):
                self.started.set()
                time.sleep(0.1)
                return ModelResponse(text="late")

        async def scenario():
            with tempfile.TemporaryDirectory() as tmp:
                _bus, failing_loop, _sessions, _memory = build_test_runtime(
                    Path(tmp), FailingModel()
                )
                failed = []
                failing_loop.pipeline.event_bus.on(TurnFinished, failed.append)
                with self.assertRaisesRegex(RuntimeError, "model unavailable"):
                    await failing_loop.pipeline.run(
                        InboundMessage("cli", "tester", "error", "hello"),
                        "cli:error",
                        dispatch_outbound=False,
                    )
                self.assertEqual(len(failed), 1)
                self.assertEqual(failed[0].status, "error")
                self.assertEqual(failed[0].error, "model unavailable")
                self.assertIsNone(failed[0].outbound)

            with tempfile.TemporaryDirectory() as tmp:
                slow_model = SlowModel()
                _bus, slow_loop, _sessions, _memory = build_test_runtime(
                    Path(tmp), slow_model
                )
                interrupted = []
                slow_loop.pipeline.event_bus.on(TurnFinished, interrupted.append)
                task = asyncio.create_task(
                    slow_loop.pipeline.run(
                        InboundMessage("cli", "tester", "interrupt", "hello"),
                        "cli:interrupt",
                        dispatch_outbound=False,
                    )
                )
                await asyncio.to_thread(slow_model.started.wait, 1)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertEqual(len(interrupted), 1)
                self.assertEqual(interrupted[0].status, "interrupted")
                self.assertIsNone(interrupted[0].outbound)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
