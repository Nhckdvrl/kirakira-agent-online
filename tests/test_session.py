"""Session persistence and history boundary tests."""

import tempfile
import unittest
from pathlib import Path

from session.manager import Session, SessionManager


class SessionTests(unittest.TestCase):
    def test_sanitized_key_collisions_use_distinct_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            manager = SessionManager(Path(tmp))
            first = manager.get_or_create("qq:a/b")
            second = manager.get_or_create("qq:a:b")
            first.add_message("user", "first")
            second.add_message("user", "second")
            manager.save(first)
            manager.save(second)

            reloaded = SessionManager(Path(tmp))
            self.assertEqual(reloaded.get_or_create("qq:a/b").messages[0]["content"], "first")
            self.assertEqual(reloaded.get_or_create("qq:a:b").messages[0]["content"], "second")
            self.assertEqual(len(list((Path(tmp) / "sessions").glob("*.json"))), 2)

    def test_history_never_starts_from_orphan_assistant(self):
        session = Session("test")
        session.add_message("user", "old user")
        session.add_message("assistant", "orphan at window boundary")
        session.add_message("user", "new user")
        session.add_message("assistant", "new answer")

        history = session.get_history(max_messages=3)

        self.assertEqual(history[0], {"role": "user", "content": "new user"})

    def test_history_preserves_reasoning_for_tool_calls_and_legacy_assistant(self):
        session = Session("reasoning")
        session.add_message("user", "inspect")
        session.add_message(
            "assistant",
            "done",
            thinking="legacy final thought",
            tool_chain=[
                {
                    "text": "",
                    "reasoning_content": "tool thought",
                    "calls": [
                        {
                            "call_id": "c1",
                            "name": "read_file",
                            "arguments": {"path": "README.md"},
                            "result": "ok",
                        }
                    ],
                }
            ],
        )

        history = session.get_history()

        self.assertEqual(history[1]["reasoning_content"], "tool thought")
        self.assertEqual(history[-1]["reasoning_content"], "legacy final thought")

    def test_sqlite_authority_returns_stable_fetchable_source_refs(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            manager = SessionManager(workspace)
            session = manager.get_or_create("web:search")
            session.add_message("user", "alpha searchable marker omega")
            session.add_message("assistant", "ack")
            manager.save(session)
            manager.close()

            reloaded = SessionManager(workspace)
            results = reloaded.search_messages("searchable marker")
            self.assertEqual(results[0]["source_ref"], session.messages[0]["id"])
            fetched = reloaded.fetch_messages(results[0]["source_ref"], context=0)
            self.assertEqual(fetched[0]["content"], "alpha searchable marker omega")
            reloaded.close()

    def test_normal_save_is_append_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            manager = SessionManager(Path(tmp))
            session = manager.get_or_create("web:append")
            first = session.add_message("user", "immutable")
            manager.save(session)

            first["content"] = "rewritten"
            with self.assertRaisesRegex(RuntimeError, "不得更新"):
                manager.save(session)

    def test_cross_manager_admission_is_exclusive(self):
        with tempfile.TemporaryDirectory() as tmp:
            first = SessionManager(Path(tmp))
            second = SessionManager(Path(tmp))
            owner = first.acquire_admission("web:one")
            with self.assertRaisesRegex(RuntimeError, "另一进程占用"):
                second.acquire_admission("web:one")
            first.release_admission("web:one", owner)
            recovered = second.acquire_admission("web:one")
            second.release_admission("web:one", recovered)

    def test_explicit_message_delete_preserves_surviving_ids_and_seq(self):
        with tempfile.TemporaryDirectory() as tmp:
            manager = SessionManager(Path(tmp))
            session = manager.get_or_create("web:delete")
            first = session.add_message("user", "first")
            second = session.add_message("assistant", "second")
            manager.save(session)

            self.assertEqual(manager.delete_messages([str(first["id"])]), 1)
            surviving = manager.get_or_create("web:delete").messages
            self.assertEqual(len(surviving), 1)
            self.assertEqual(surviving[0]["id"], second["id"])
            self.assertEqual(surviving[0]["seq"], 1)

    def test_legacy_json_is_backed_up_and_imported_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            legacy = Session("legacy:one")
            legacy.add_message("user", "from json")
            session_dir = workspace / "sessions"
            session_dir.mkdir()
            session_dir.joinpath("legacy.json").write_text(
                __import__("json").dumps(legacy.to_json()), encoding="utf-8"
            )

            manager = SessionManager(workspace)
            loaded = manager.get_or_create("legacy:one")
            self.assertEqual(loaded.messages[0]["content"], "from json")
            backups = list((workspace / ".kirakira" / "backups").iterdir())
            self.assertEqual(len(backups), 1)

            # SQLite 已成为 owner；后续破坏镜像不会改变重启后的会话。
            session_dir.joinpath("legacy.json").write_text("{broken", encoding="utf-8")
            manager.close()
            reloaded = SessionManager(workspace)
            self.assertEqual(
                reloaded.get_or_create("legacy:one").messages[0]["content"],
                "from json",
            )


if __name__ == "__main__":
    unittest.main()
