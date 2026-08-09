"""Fail-loud boundary tests for the passive chain.

这些用例锁住一条边界：**能降级的地方降级，不能降级的地方必须报错**。
静默吞掉这些失败会产生“看起来正常、其实已经损坏”的运行时状态。
"""

import json
import tempfile
import unittest
from pathlib import Path

from core.memory.legacy import MemoryRuntime
from session.manager import SessionManager


class BoomEmbeddings:
    def embed(self, text):
        raise RuntimeError("embedding backend down")


class MemoryEmbeddingBoundaryTests(unittest.TestCase):
    def _memory(self, tmp):
        sessions = SessionManager(Path(tmp))
        return MemoryRuntime(Path(tmp), session_manager=sessions)

    def test_recall_degrades_to_lexical_when_embedding_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            memory = self._memory(tmp)
            memory.memorize("kirakira uses declarative mcp", memory_type="fact")
            memory.embedding_client = BoomEmbeddings()

            # 检索侧允许降级：向量服务挂了仍然要能用词法召回。
            with self.assertLogs("core.memory.legacy", level="ERROR"):
                hits = memory.recall("declarative mcp")

            self.assertTrue(any("declarative mcp" in hit.content for hit in hits))

    def test_memorize_fails_loud_when_embedding_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            memory = self._memory(tmp)
            memory.embedding_client = BoomEmbeddings()
            # 写入侧不许降级：否则这条记忆此后永远无法被语义召回，索引还会半有半无。
            with self.assertRaises(RuntimeError):
                memory.memorize("never silently stored", memory_type="fact")

    def test_update_record_fails_loud_when_embedding_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            memory = self._memory(tmp)
            record = memory.memorize("original", memory_type="fact")
            memory.embedding_client = BoomEmbeddings()

            with self.assertRaises(RuntimeError):
                memory.update_record(record.id, content="rewritten")


class SessionCorruptionBoundaryTests(unittest.TestCase):
    def test_corrupt_legacy_session_file_blocks_first_migration(self):
        with tempfile.TemporaryDirectory() as tmp:
            session_dir = Path(tmp) / "sessions"
            session_dir.mkdir()
            corrupt = session_dir / "corrupt.json"
            corrupt.write_text("{not json", encoding="utf-8")

            # 一次性迁移不能静默漏掉损坏的 legacy source。
            with self.assertRaises(RuntimeError) as ctx:
                SessionManager(Path(tmp))
            self.assertIn("corrupt.json", str(ctx.exception))

    def test_keyless_legacy_file_does_not_create_phantom_session(self):
        with tempfile.TemporaryDirectory() as tmp:
            keyless = Path(tmp) / "sessions" / "keyless.json"
            keyless.parent.mkdir(parents=True, exist_ok=True)
            keyless.write_text(json.dumps({"messages": []}), encoding="utf-8")

            manager = SessionManager(Path(tmp))
            self.assertEqual(manager.list_sessions(), [])


if __name__ == "__main__":
    unittest.main()
