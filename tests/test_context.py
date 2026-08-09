"""Kirakira Agent learning harness module."""

import tempfile
import unittest
from pathlib import Path

from agent.model_runtime.context import compact_messages, estimate_tokens, microcompact


class ContextTests(unittest.TestCase):
    def test_estimate_tokens_returns_positive_count(self):
        self.assertGreater(estimate_tokens([{"role": "user", "content": "hello"}]), 0)

    def test_microcompact_clears_old_tool_outputs(self):
        messages = [
            {"role": "tool", "content": "x" * 200, "tool_call_id": "1"},
            {"role": "tool", "content": "y" * 200, "tool_call_id": "2"},
        ]
        microcompact(messages, keep_tool_results=1)

        self.assertEqual(messages[0]["content"], "[cleared by microcompact]")
        self.assertEqual(messages[1]["content"], "y" * 200)

    def test_compact_messages_writes_transcript_and_keeps_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            compacted = compact_messages(
                [{"role": "user", "content": "hello"}],
                Path(tmp),
                summary="important summary",
            )

            self.assertIn("important summary", compacted[0]["content"])
            self.assertEqual(len(list(Path(tmp).glob("transcript_*.jsonl"))), 1)


if __name__ == "__main__":
    unittest.main()
