"""Long-term memory consistency tests."""

import tempfile
import unittest
from pathlib import Path

from core.memory.legacy import MemoryRuntime
from session.manager import SessionManager


class MemoryTests(unittest.TestCase):
    def test_optional_embeddings_add_semantic_recall_without_breaking_lexical_store(self):
        class FakeEmbeddings:
            def embed(self, text):
                if "coding" in text or "编程" in text:
                    return [1.0, 0.0]
                return [0.0, 1.0]

        with tempfile.TemporaryDirectory() as tmp:
            memory = MemoryRuntime(Path(tmp))
            memory.embedding_client = FakeEmbeddings()
            memory.memorize("用户周末喜欢coding")
            memory.memorize("用户喜欢古典音乐")

            recalled = memory.recall("编程活动")

            self.assertEqual(recalled[0].content, "用户周末喜欢coding")

    def test_forget_removes_item_from_injected_markdown(self):
        with tempfile.TemporaryDirectory() as tmp:
            memory = MemoryRuntime(Path(tmp))
            record = memory.memorize("用户喜欢蓝色")
            self.assertIn("用户喜欢蓝色", memory.store.read_long_term())

            memory.forget([record.id])

            self.assertNotIn("用户喜欢蓝色", memory.store.read_long_term())
            self.assertEqual(memory.recall("蓝色"), [])

    def test_exact_duplicate_reinforces_instead_of_duplicating(self):
        with tempfile.TemporaryDirectory() as tmp:
            memory = MemoryRuntime(Path(tmp))
            first = memory.memorize("  Use MCP for Steam  ", source_ref="a")
            second = memory.memorize("use mcp for steam", source_ref="b")

            self.assertEqual(first.id, second.id)
            self.assertEqual(second.reinforcement, 2)
            self.assertEqual(second.source_ref, "b")
            self.assertEqual(memory.store.read_long_term().count(first.id), 1)

    def test_chinese_partial_query_recalls_longer_sentence(self):
        with tempfile.TemporaryDirectory() as tmp:
            memory = MemoryRuntime(Path(tmp))
            memory.memorize("用户长期偏好安静的工作环境")

            recalled = memory.recall("工作环境")

            self.assertEqual(len(recalled), 1)

    def test_managed_memory_rewrite_preserves_manual_markdown(self):
        with tempfile.TemporaryDirectory() as tmp:
            memory = MemoryRuntime(Path(tmp))
            memory.store.memory_path.write_text(
                "# Long-Term Memory\n\n用户手写的重要内容\n", encoding="utf-8"
            )
            record = memory.memorize("托管内容")
            memory.forget([record.id])

            markdown = memory.store.read_long_term()
            self.assertIn("用户手写的重要内容", markdown)
            self.assertNotIn("托管内容", markdown)

    def test_deleting_session_forgets_only_memories_sourced_from_that_session(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            sessions = SessionManager(workspace)
            memory = MemoryRuntime(workspace, session_manager=sessions)
            session = sessions.get_or_create("web:deleted")
            session.add_message("user", "source")
            sessions.save(session)
            removed = memory.memorize("session fact", source_ref="web:deleted:0")
            kept = memory.memorize("other fact", source_ref="web:other:0")

            sessions.delete_session("web:deleted")

            statuses = {item["id"]: item["status"] for item in memory.list_records(include_forgotten=True)}
            self.assertEqual(statuses[removed.id], "forgotten")
            self.assertEqual(statuses[kept.id], "active")
            sessions.close()


if __name__ == "__main__":
    unittest.main()
