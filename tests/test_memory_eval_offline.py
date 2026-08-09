from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from eval.longmemeval.dataset import (
    SUPPORTED_QUESTION_TYPES,
    builtin_smoke_instances,
    load_longmemeval,
)
from eval.longmemeval.offline_ab import run_offline_ab


class LongMemEvalLoaderTests(unittest.TestCase):
    def test_loader_preserves_reference_evidence_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "data.json"
            path.write_text(
                json.dumps(
                    [
                        {
                            "question_id": "q1",
                            "question_type": "single-session-user",
                            "question": "Where?",
                            "answer": "Nagoya",
                            "haystack_session_ids": ["s1"],
                            "haystack_dates": ["2026-01-01"],
                            "haystack_sessions": [
                                [
                                    {
                                        "role": "user",
                                        "content": "Nagoya",
                                        "has_answer": True,
                                    }
                                ]
                            ],
                            "answer_session_ids": ["s1"],
                        }
                    ]
                ),
                encoding="utf-8",
            )
            loaded = load_longmemeval(path)
            self.assertEqual(loaded[0].answer_session_ids, ["s1"])
            self.assertTrue(loaded[0].haystack_sessions[0][0].has_answer)

    def test_builtin_smoke_covers_all_reference_types(self) -> None:
        self.assertEqual(
            {item.question_type for item in builtin_smoke_instances()},
            set(SUPPORTED_QUESTION_TYPES),
        )


class OfflineMemoryABTests(unittest.TestCase):
    def test_both_real_engines_ingest_retrieve_and_persist_without_http(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                workspace = Path(tmp) / "eval"
                report = await run_offline_ab(workspace=workspace)
                self.assertEqual(report["paid_api_calls"], 0)
                scores = report["scores"]
                self.assertEqual(scores["default"]["cases"], 3)
                self.assertEqual(scores["akasha"]["cases"], 3)
                self.assertGreaterEqual(scores["default"]["retrieval_hits"], 2)
                self.assertGreaterEqual(scores["akasha"]["retrieval_hits"], 2)
                self.assertEqual(scores["default"]["evidence_hits"], 3)
                self.assertEqual(scores["akasha"]["evidence_hits"], 3)

                results = report["results"]
                for item in results:
                    self.assertEqual(item["offline_calls"]["http"], 0)
                    self.assertEqual(item["state"]["sessions_integrity"], "ok")
                    self.assertEqual(item["state"]["memory_integrity"], "ok")
                    self.assertGreater(item["state"]["messages"], 0)
                    if item["engine"] == "default":
                        self.assertGreater(item["state"]["memory_items"], 0)
                    else:
                        self.assertGreater(item["state"]["akasha_nodes"], 0)
                        self.assertGreater(item["state"]["fts_idf_tokens"], 0)
                        self.assertEqual(
                            item["state"]["message_embeddings"],
                            item["state"]["messages"],
                        )
                self.assertTrue((workspace / "report.json").exists())

        asyncio.run(scenario())

    def test_refuses_to_mix_with_nonempty_workspace(self) -> None:
        async def scenario() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                workspace = Path(tmp) / "eval"
                workspace.mkdir()
                (workspace / "keep.txt").write_text("user data", encoding="utf-8")
                with self.assertRaises(FileExistsError):
                    await run_offline_ab(workspace=workspace)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
