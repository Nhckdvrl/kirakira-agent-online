"""M0/M1 production-owner migration and compatibility facade tests."""

from __future__ import annotations

import json
import asyncio
import socket
import sqlite3
import tempfile
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from bus.queue import MessageBus
from infra.channels.contract import ChannelContext
from infra.channels.web_chat_channel import WebChannel
from bus.event_bus import EventBus
from core.memory.legacy import MemoryRuntime
from bootstrap.memory_admin import clear, doctor, migrate, rollback, verify
from core.schema import ToolCall
from session.manager import SessionManager
from agent.tools import build_default_registry


class MemoryM1Tests(unittest.TestCase):
    @staticmethod
    def _free_port() -> int:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    def _workspace(self, root: Path) -> Path:
        memory = root / "memory"
        memory.mkdir(parents=True)
        (memory / "MEMORY.md").write_text(
            "# Long-Term Memory\n\n手写内容\n\n"
            "<!-- kirakira:managed-memory:start -->\n"
            "- [mem_0001 type=requested_memory reinforced=2] old managed\n"
            "<!-- kirakira:managed-memory:end -->\n",
            encoding="utf-8",
        )
        for name in ("SELF.md", "PENDING.md", "RECENT_CONTEXT.md"):
            (memory / name).write_text(f"# {name}\n", encoding="utf-8")
        (memory / "items.json").write_text(
            json.dumps(
                [
                    {
                        "id": "mem_0001",
                        "content": "用户喜欢蓝色",
                        "created_at": "2026-01-01T00:00:00+00:00",
                        "updated_at": "2026-01-02T00:00:00+00:00",
                        "source_ref": "web:a:0",
                        "status": "active",
                        "memory_type": "requested_memory",
                        "reinforcement": 2,
                        "embedding": None,
                    },
                    {
                        "id": "mem_0002",
                        "content": "发布前必须运行测试",
                        "created_at": "2026-01-03T00:00:00+00:00",
                        "updated_at": "2026-01-04T00:00:00+00:00",
                        "source_ref": "web:a:2",
                        "status": "forgotten",
                        "memory_type": "procedure",
                        "reinforcement": 1,
                        "embedding": None,
                    },
                ],
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return root

    def test_doctor_is_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workspace = self._workspace(Path(tmp))
            before = sorted(str(path.relative_to(workspace)) for path in workspace.rglob("*"))
            report = doctor(workspace, project_root=Path(__file__).resolve().parents[1])
            after = sorted(str(path.relative_to(workspace)) for path in workspace.rglob("*"))
            self.assertEqual(before, after)
            self.assertEqual(report["legacy"]["items"], 2)
            self.assertEqual(report["coremem"]["integrity"], "missing")

    def test_staged_migration_runtime_and_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workspace = self._workspace(Path(tmp))
            result = migrate(workspace)
            backup_id = result["backup_id"]
            self.assertTrue(result["verify"]["ok"])
            self.assertFalse((workspace / "memory" / "items.json").exists())
            self.assertIn("手写内容", (workspace / "memory" / "MEMORY.md").read_text())
            self.assertNotIn("managed-memory", (workspace / "memory" / "MEMORY.md").read_text())

            conn = sqlite3.connect(workspace / "memory" / "coremem.db")
            try:
                rows = conn.execute(
                    "SELECT id, memory_type, status, reinforcement, source_ref, extra_json "
                    "FROM memory_items ORDER BY id"
                ).fetchall()
                self.assertEqual(conn.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            finally:
                conn.close()
            self.assertEqual(rows[0][:5], ("mem_0001", "profile", "active", 2, "web:a:0"))
            self.assertEqual(json.loads(rows[0][5])["legacy_memory_type"], "requested_memory")
            self.assertEqual(rows[1][2], "superseded")
            self.assertIn("rule_schema", json.loads(rows[1][5]))

            runtime = MemoryRuntime(workspace)
            self.assertEqual(runtime.engine, "coremem")
            self.assertEqual(runtime.recall("蓝色")[0].id, "mem_0001")
            registry = build_default_registry(workspace, memory=runtime)
            tool_result = registry.execute(
                ToolCall("call-1", "memorize", {"content": "用户偏好安静", "memory_type": "preference"})
            )
            self.assertFalse(tool_result.is_error)
            recalled = registry.execute(
                ToolCall("call-2", "recall_memory", {"query": "安静", "limit": 5})
            )
            self.assertIn("用户偏好安静", recalled.content)
            new = runtime.memorize("用户喜欢猫", memory_type="preference")
            self.assertFalse((workspace / "memory" / "items.json").exists())
            self.assertIn(new.id, [item["id"] for item in runtime.list_records()])
            self.assertEqual(runtime.forget([new.id]), [new.id])
            self.assertEqual(
                next(item for item in runtime.list_records(include_forgotten=True) if item["id"] == new.id)["status"],
                "superseded",
            )
            runtime.store2.close()
            post_write_verify = verify(workspace, backup_id=backup_id)
            self.assertTrue(post_write_verify["ok"])
            self.assertFalse(post_write_verify["migration_snapshot_exact"])

            restored = rollback(workspace, backup_id=backup_id)
            self.assertTrue(restored["ok"])
            legacy = MemoryRuntime(workspace)
            self.assertEqual(legacy.engine, "legacy")
            self.assertEqual(len(legacy.list_records(include_forgotten=True)), 2)
            legacy.store2.close()

    def test_memory2_dashboard_filters_edits_and_confirmed_hard_delete(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                workspace = self._workspace(Path(tmp))
                migrate(workspace)
                sessions = SessionManager(workspace)
                memory = MemoryRuntime(workspace, session_manager=sessions)
                created = memory.memorize("Dashboard 临时记忆", memory_type="preference")
                channel = WebChannel(host="127.0.0.1", port=self._free_port())
                ctx = ChannelContext(
                    MessageBus(), sessions, EventBus(), workspace,
                    __import__("logging").getLogger("test.memory2.dashboard"),
                    memory=memory,
                )
                await channel.start(ctx)
                base = f"http://127.0.0.1:{channel.port}"
                try:
                    listed = json.loads(
                        await asyncio.to_thread(
                            lambda: urllib.request.urlopen(
                                base + "/api/memories?q=Dashboard&memory_type=preference&status=active",
                                timeout=5,
                            ).read()
                        )
                    )
                    self.assertEqual(listed["total"], 1)
                    patch = urllib.request.Request(
                        base + "/api/memory",
                        data=json.dumps(
                            {"id": created.id, "content": "Dashboard 已编辑", "memory_type": "event"}
                        ).encode(),
                        headers={"content-type": "application/json"},
                        method="PATCH",
                    )
                    await asyncio.to_thread(lambda: urllib.request.urlopen(patch, timeout=5).read())
                    self.assertEqual(memory.store2.get_item_for_dashboard(created.id)["memory_type"], "event")

                    unconfirmed = urllib.request.Request(
                        base + f"/api/memory?id={created.id}&hard=true", method="DELETE"
                    )
                    with self.assertRaises(urllib.error.HTTPError) as caught:
                        await asyncio.to_thread(
                            lambda: urllib.request.urlopen(unconfirmed, timeout=5).read()
                        )
                    self.assertEqual(caught.exception.code, 400)

                    confirmed = urllib.request.Request(
                        base + "/api/memories/delete",
                        data=json.dumps({"ids": [created.id], "confirm": "HARD_DELETE"}).encode(),
                        headers={"content-type": "application/json"},
                        method="POST",
                    )
                    result = json.loads(
                        await asyncio.to_thread(
                            lambda: urllib.request.urlopen(confirmed, timeout=5).read()
                        )
                    )
                    self.assertEqual(result["deleted"], 1)
                    self.assertIsNone(memory.store2.get_item_for_dashboard(created.id))
                finally:
                    await channel.stop()
                    sessions.close()
                    memory.store2.close()

        asyncio.run(scenario())

    def test_clear_forgets_runtime_memory_but_preserves_self_and_sessions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workspace = self._workspace(Path(tmp))
            migrate(workspace)
            (workspace / "memory" / "HISTORY.md").write_text("# History\nold\n")
            (workspace / "sessions").mkdir()
            (workspace / "sessions" / "keep.json").write_text("{}")
            original_self = (workspace / "memory" / "SELF.md").read_text()

            with self.assertRaises(ValueError):
                clear(workspace, confirm="")
            result = clear(workspace, confirm="CLEAR_ALL_MEMORY")

            self.assertEqual(result["deleted_structured_items"], 2)
            self.assertEqual((workspace / "memory" / "SELF.md").read_text(), original_self)
            self.assertTrue((workspace / "sessions" / "keep.json").exists())
            self.assertEqual(
                (workspace / "memory" / "RECENT_CONTEXT.md").read_text(),
                "# Recent Context\n",
            )
            self.assertEqual(
                (workspace / "memory" / "HISTORY.md").read_text(), "# History\n"
            )
            self.assertTrue(verify(workspace)["ok"])

            purge = clear(
                workspace,
                confirm="CLEAR_ALL_MEMORY",
                include_sessions=True,
                clear_self=True,
            )
            self.assertEqual(purge["deleted_session_files"], 1)
            self.assertEqual(list((workspace / "sessions").iterdir()), [])
            self.assertEqual(
                (workspace / "memory" / "SELF.md").read_text(), "# Self Model\n"
            )
            self.assertTrue(
                (workspace / "memory" / "backups" / purge["backup_id"] / "sessions" / "keep.json").exists()
            )


if __name__ == "__main__":
    unittest.main()


class RepairKindsTests(unittest.TestCase):
    """把非规范 memory_type 归一,让旧数据重新可注入。

    背景:注入选择器只接受 event/profile/preference/procedure,其余类型即使被检索
    命中也永远进不了上下文——实弹里表现为"召回 score 0.58 但 injected=False,
    模型答出与记忆不符的内容"。
    """

    def _db(self, tmp: Path, rows: list[tuple[str, str, str]]) -> None:
        (tmp / "memory").mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(tmp / "memory" / "coremem.db")
        conn.execute(
            "CREATE TABLE memory_items (id TEXT PRIMARY KEY, memory_type TEXT, "
            "status TEXT, summary TEXT, embedding BLOB, source_ref TEXT)"
        )
        conn.executemany(
            "INSERT INTO memory_items(id, memory_type, status, summary) VALUES (?,?,?,?)",
            [(i, t, s, "内容 %s" % i) for i, t, s in rows],
        )
        conn.commit()
        conn.close()

    def test_dry_run_reports_without_writing(self) -> None:
        from bootstrap.memory_admin import repair_kinds

        with tempfile.TemporaryDirectory() as raw:
            tmp = Path(raw)
            self._db(tmp, [("a", "identity", "active")])
            report = repair_kinds(tmp, dry_run=True)
            self.assertTrue(report["dry_run"])
            self.assertEqual(report["repaired"], 0)
            self.assertEqual(report["items"][0]["target_type"], "profile")
            conn = sqlite3.connect(tmp / "memory" / "coremem.db")
            self.assertEqual(
                conn.execute("SELECT memory_type FROM memory_items").fetchone()[0],
                "identity",  # dry-run 不落库
            )
            conn.close()

    def test_repair_maps_legacy_types_and_backs_up(self) -> None:
        from bootstrap.memory_admin import repair_kinds

        with tempfile.TemporaryDirectory() as raw:
            tmp = Path(raw)
            self._db(
                tmp,
                [
                    ("a", "identity", "active"),
                    ("b", "fact", "superseded"),
                    ("c", "preference", "active"),  # 规范类型不该被动
                ],
            )
            report = repair_kinds(tmp)
            self.assertEqual(report["repaired"], 2)
            self.assertTrue(report["backup_id"])  # 改数据前必须留备份
            conn = sqlite3.connect(tmp / "memory" / "coremem.db")
            types = dict(conn.execute("SELECT id, memory_type FROM memory_items"))
            conn.close()
            self.assertEqual(types, {"a": "profile", "b": "profile", "c": "preference"})

    def test_repair_is_idempotent(self) -> None:
        from bootstrap.memory_admin import repair_kinds

        with tempfile.TemporaryDirectory() as raw:
            tmp = Path(raw)
            self._db(tmp, [("a", "identity", "active")])
            repair_kinds(tmp)
            again = repair_kinds(tmp)
            self.assertEqual(again["repaired"], 0)
            self.assertIn("没有需要修复", again["note"])

    def test_missing_db_is_reported_not_raised(self) -> None:
        from bootstrap.memory_admin import repair_kinds

        with tempfile.TemporaryDirectory() as raw:
            report = repair_kinds(Path(raw))
            self.assertFalse(report["ok"])
            self.assertIn("不存在", report["error"])
