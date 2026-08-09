"""Runtime snapshot store, lease, and drain-ordering tests."""

import asyncio
import unittest

from agent.plugins.snapshot import (
    RuntimeSnapshotStore,
    SnapshotToolView,
    bind_runtime_snapshot,
    compile_snapshot,
    derive_snapshot,
    get_current_runtime_snapshot,
    reset_runtime_snapshot,
)
from core.schema import ToolCall, ToolSpec
from agent.tools.registry import Tool, ToolRegistry


def _echo_tool(name: str, reply: str) -> Tool:
    async def handler(**kwargs: object) -> str:
        return reply

    return Tool(
        spec=ToolSpec(name, "echo", {"type": "object", "properties": {}}),
        handler=handler,
        deferred=True,
    )


class SnapshotStoreTests(unittest.TestCase):
    def test_publish_switches_current_and_retires_previous_after_drain(self):
        async def scenario():
            drained = []
            store = RuntimeSnapshotStore()
            store.set_drain_handler(lambda s: _record(drained, s))

            first = compile_snapshot(revision="1")
            await store.commit(store.publish(first))
            self.assertIs(store.current, first)

            second = compile_snapshot(revision="2")
            await store.commit(store.publish(second))

            self.assertIs(store.current, second)
            self.assertEqual(first.state, "drained")
            self.assertEqual([s.snapshot_id for s in drained], [first.snapshot_id])

        asyncio.run(scenario())

    def test_retired_snapshot_drains_only_after_last_lease_releases(self):
        async def scenario():
            drained = []
            store = RuntimeSnapshotStore()
            store.set_drain_handler(lambda s: _record(drained, s))

            first = compile_snapshot(revision="1")
            await store.commit(store.publish(first))
            lease_a = store.lease()
            lease_b = store.lease()

            await store.commit(store.publish(compile_snapshot(revision="2")))
            # 旧快照已退休，但仍有两个在途租约，资源不能回收。
            self.assertEqual(first.state, "retired")
            self.assertEqual(drained, [])

            await lease_a.release()
            self.assertEqual(first.state, "retired")
            self.assertEqual(drained, [])

            await lease_b.release()
            self.assertEqual(first.state, "drained")
            self.assertEqual([s.snapshot_id for s in drained], [first.snapshot_id])

        asyncio.run(scenario())

    def test_rollback_restores_previous_and_drains_candidate(self):
        async def scenario():
            drained = []
            store = RuntimeSnapshotStore()
            store.set_drain_handler(lambda s: _record(drained, s))

            first = compile_snapshot(revision="1")
            await store.commit(store.publish(first))

            candidate = compile_snapshot(revision="2")
            transaction = store.publish(candidate)
            self.assertIs(store.current, candidate)

            await store.rollback(transaction)

            self.assertIs(store.current, first)
            self.assertEqual(candidate.state, "drained")
            self.assertEqual([s.snapshot_id for s in drained], [candidate.snapshot_id])
            self.assertEqual(first.state, "published")

        asyncio.run(scenario())

    def test_double_publish_without_commit_is_rejected(self):
        async def scenario():
            store = RuntimeSnapshotStore()
            store.publish(compile_snapshot(revision="1"))
            with self.assertRaises(RuntimeError):
                store.publish(compile_snapshot(revision="2"))

        asyncio.run(scenario())

    def test_lease_release_underflow_fails_loud(self):
        async def scenario():
            store = RuntimeSnapshotStore()
            snapshot = compile_snapshot(revision="1")
            await store.commit(store.publish(snapshot))
            lease = store.lease()
            await lease.release()
            with self.assertRaises(RuntimeError):
                await store.release_lease(snapshot)

        asyncio.run(scenario())

    def test_lease_is_idempotent_on_double_release(self):
        async def scenario():
            store = RuntimeSnapshotStore()
            await store.commit(store.publish(compile_snapshot(revision="1")))
            lease = store.lease()
            await lease.release()
            await lease.release()
            self.assertFalse(lease.active)

        asyncio.run(scenario())

    def test_wait_drained_unblocks_when_lease_releases(self):
        async def scenario():
            store = RuntimeSnapshotStore()
            first = compile_snapshot(revision="1")
            await store.commit(store.publish(first))
            lease = store.lease()
            await store.commit(store.publish(compile_snapshot(revision="2")))

            waiter = asyncio.create_task(store.wait_drained(first))
            await asyncio.sleep(0)
            self.assertFalse(waiter.done())

            await lease.release()
            await asyncio.wait_for(waiter, timeout=1)
            self.assertEqual(first.state, "drained")

        asyncio.run(scenario())


class SnapshotBindingTests(unittest.TestCase):
    def test_binding_is_visible_only_to_owner_task(self):
        async def scenario():
            store = RuntimeSnapshotStore()
            await store.commit(store.publish(compile_snapshot(revision="1")))
            lease = store.lease()
            token = bind_runtime_snapshot(lease)
            try:
                self.assertIsNotNone(get_current_runtime_snapshot())

                # 子任务没有自己的租约，不应该看到父任务锁定的快照。
                async def child():
                    return get_current_runtime_snapshot()

                self.assertIsNone(await asyncio.create_task(child()))
            finally:
                reset_runtime_snapshot(token)
                await lease.release()

        asyncio.run(scenario())

    def test_released_lease_stops_exposing_snapshot(self):
        async def scenario():
            store = RuntimeSnapshotStore()
            await store.commit(store.publish(compile_snapshot(revision="1")))
            lease = store.lease()
            token = bind_runtime_snapshot(lease)
            try:
                self.assertIsNotNone(get_current_runtime_snapshot())
                await lease.release()
                self.assertIsNone(get_current_runtime_snapshot())
            finally:
                reset_runtime_snapshot(token)

        asyncio.run(scenario())

    def test_forked_lease_keeps_snapshot_alive_for_subtask(self):
        async def scenario():
            drained = []
            store = RuntimeSnapshotStore()
            store.set_drain_handler(lambda s: _record(drained, s))
            first = compile_snapshot(revision="1")
            await store.commit(store.publish(first))

            parent = store.lease()
            child = parent.fork()
            await store.commit(store.publish(compile_snapshot(revision="2")))

            await parent.release()
            self.assertEqual(drained, [])
            await child.release()
            self.assertEqual([s.snapshot_id for s in drained], [first.snapshot_id])

        asyncio.run(scenario())


class SnapshotToolViewTests(unittest.TestCase):
    def test_view_composes_base_registry_and_snapshot_tools(self):
        async def scenario():
            base = ToolRegistry()
            base.register(
                ToolSpec("base_tool", "b", {"type": "object", "properties": {}}),
                lambda: "base",
            )
            snapshot = compile_snapshot(
                mcp_tools={"mcp_x__echo": _echo_tool("mcp_x__echo", "from-snapshot")},
                mcp_generation_id="gen1",
            )
            view = SnapshotToolView(base, snapshot)

            self.assertTrue(view.has("base_tool"))
            self.assertTrue(view.has("mcp_x__echo"))
            self.assertTrue(view.is_deferred("mcp_x__echo"))
            self.assertFalse(view.is_deferred("base_tool"))
            self.assertEqual(view.names(), ["base_tool", "mcp_x__echo"])

            result = await view.execute_async(ToolCall("1", "mcp_x__echo", {}))
            self.assertEqual(result.content, "from-snapshot")
            # 快照工具不进基础注册表。
            self.assertFalse(base.has("mcp_x__echo"))

        asyncio.run(scenario())

    def test_view_without_snapshot_falls_back_to_base_only(self):
        base = ToolRegistry()
        base.register(
            ToolSpec("base_tool", "b", {"type": "object", "properties": {}}),
            lambda: "base",
        )
        view = SnapshotToolView(base, None)
        self.assertEqual(view.names(), ["base_tool"])
        self.assertFalse(view.has("mcp_x__echo"))

    def test_derive_preserves_other_capabilities(self):
        base = compile_snapshot(
            phase_modules={"before_turn_modules": [object()]},
            mcp_tools={"mcp_a__x": _echo_tool("mcp_a__x", "a")},
            mcp_generation_id="gen1",
        )
        derived = derive_snapshot(
            base,
            mcp_tools={"mcp_b__x": _echo_tool("mcp_b__x", "b")},
            mcp_generation_id="gen2",
            revision="r2",
        )
        self.assertEqual(len(derived.before_turn_modules), 1)
        self.assertEqual(derived.mcp_tool_names, ("mcp_b__x",))
        self.assertEqual(derived.mcp_generation_id, "gen2")
        self.assertEqual(derived.state, "compiled")


async def _record(sink, snapshot):
    sink.append(snapshot)


if __name__ == "__main__":
    unittest.main()
