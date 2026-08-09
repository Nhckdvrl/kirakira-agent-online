"""Drift skill journal 与自我观察(照 Reference plugins/drift_flow/state.py)。

Drift 的连续性此前只有 continuum(一段 scratchpad + 一个倾向)。journal 补上
"跑过什么"的完整记录:append-only,按 entry_type 分类,self_observation 跨 skill 汇总,
让下一轮 Agent 看得到自己前几轮干了什么、干得怎么样。
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from plugins.drift_flow.state import DriftStateStore
from plugins.drift_flow.tools import DriftRunContext, register_drift_tools
from core.schema import ToolCall
from agent.tools.registry import ToolRegistry

NOW = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)


def _store(tmp: str) -> DriftStateStore:
    return DriftStateStore(Path(tmp) / "drift.db")


class JournalStoreTests(unittest.TestCase):
    def test_append_and_load_in_chronological_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = _store(tmp)
            store.append_journal("audit", "progress", {"note": "first"}, NOW)
            store.append_journal(
                "audit", "progress", {"note": "second"}, NOW + timedelta(minutes=1)
            )
            notes = [e["payload"]["note"] for e in store.load_journal("audit")]
            # 时间正序,便于直接拼进 briefing
            self.assertEqual(notes, ["first", "second"])
            store.close()

    def test_filter_by_entry_type_and_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = _store(tmp)
            store.append_journal("audit", "progress", {"note": "p"}, NOW, key="topic-a")
            store.append_journal("audit", "self_observation", {"note": "o"}, NOW)
            store.append_journal("audit", "progress", {"note": "q"}, NOW, key="topic-b")

            self.assertEqual(
                [e["payload"]["note"] for e in store.load_journal("audit", entry_type="progress")],
                ["p", "q"],
            )
            self.assertEqual(
                [e["payload"]["note"] for e in store.load_journal("audit", key="topic-a")],
                ["p"],
            )
            store.close()

    def test_journal_is_scoped_per_skill(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = _store(tmp)
            store.append_journal("a", "progress", {"note": "mine"}, NOW)
            store.append_journal("b", "progress", {"note": "theirs"}, NOW)
            self.assertEqual(
                [e["payload"]["note"] for e in store.load_journal("a")], ["mine"]
            )
            store.close()

    def test_self_observations_span_skills(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = _store(tmp)
            store.append_journal("a", "self_observation", {"note": "obs-a"}, NOW)
            store.append_journal("b", "self_observation", {"note": "obs-b"}, NOW)
            store.append_journal("a", "progress", {"note": "not-an-observation"}, NOW)
            observed = [
                (o["skill"], o["payload"]["note"])
                for o in store.recent_self_observations()
            ]
            self.assertEqual(observed, [("a", "obs-a"), ("b", "obs-b")])
            store.close()

    def test_limit_keeps_the_most_recent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = _store(tmp)
            for i in range(5):
                store.append_journal("a", "progress", {"note": str(i)}, NOW)
            notes = [e["payload"]["note"] for e in store.load_journal("a", limit=2)]
            self.assertEqual(notes, ["3", "4"])
            store.close()

    def test_blank_skill_or_type_is_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = _store(tmp)
            store.append_journal("", "progress", {"note": "x"}, NOW)
            store.append_journal("a", "", {"note": "x"}, NOW)
            self.assertEqual(store.load_journal("a"), [])
            store.close()

    def test_corrupt_payload_does_not_break_the_whole_read(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = _store(tmp)
            store.append_journal("a", "progress", {"note": "good"}, NOW)
            store._db.execute("UPDATE skill_journal SET payload_json = 'not json'")
            store._db.commit()
            entries = store.load_journal("a")
            # 脏数据降级为空 payload,不该让整段 journal 读不出来
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0]["payload"], {})
            store.close()

    def test_persists_across_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "drift.db"
            first = DriftStateStore(path)
            first.append_journal("a", "progress", {"note": "kept"}, NOW)
            first.close()
            reopened = DriftStateStore(path)
            self.assertEqual(
                [e["payload"]["note"] for e in reopened.load_journal("a")], ["kept"]
            )
            reopened.close()


class JournalToolTests(unittest.TestCase):
    """工具只收集意图,不直接碰持久状态——与 message_push 同一取向。"""

    def _invoke(self, ctx: DriftRunContext, **arguments) -> str:
        registry = ToolRegistry()
        register_drift_tools(registry, ctx)
        result = registry.execute(
            ToolCall(id="t1", name="journal_append", arguments=arguments)
        )
        return result.content

    def test_tool_is_registered(self) -> None:
        registry = ToolRegistry()
        register_drift_tools(registry, DriftRunContext(skill="audit"))
        self.assertIn("journal_append", registry.names())

    def test_valid_entry_is_collected_not_persisted(self) -> None:
        ctx = DriftRunContext(skill="audit")
        out = self._invoke(ctx, entry_type="progress", note="查了 3 条记忆", key="mem")
        self.assertIn("已记录", out)
        self.assertEqual(
            ctx.journal_entries,
            [{"entry_type": "progress", "note": "查了 3 条记忆", "key": "mem"}],
        )

    def test_blank_arguments_are_rejected(self) -> None:
        ctx = DriftRunContext(skill="audit")
        self.assertIn("Error", self._invoke(ctx, entry_type="", note="x"))
        self.assertIn("Error", self._invoke(ctx, entry_type="progress", note="  "))
        self.assertEqual(ctx.journal_entries, [])

    def test_entry_cap_prevents_unbounded_growth(self) -> None:
        ctx = DriftRunContext(skill="audit")
        for i in range(20):
            self._invoke(ctx, entry_type="progress", note="n%d" % i)
        self.assertIn("上限", self._invoke(ctx, entry_type="progress", note="overflow"))
        self.assertEqual(len(ctx.journal_entries), 20)


if __name__ == "__main__":
    unittest.main()
