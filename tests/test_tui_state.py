"""Presentation reducer tests; these do not require Textual."""

from datetime import datetime
import unittest

from bus.events import OutboundMessage
from bus.events_lifecycle import (
    StreamDeltaReady,
    ToolCallCompleted,
    ToolCallStarted,
    TurnFinished,
    TurnStarted,
)
from frontend.tui.state import TurnViewState


class TurnViewStateTests(unittest.TestCase):
    def test_multi_iteration_tools_and_authoritative_final(self):
        now = datetime.now().astimezone()
        state = TurnViewState()

        state.apply(TurnStarted("cli:local", "cli", "local", "查新闻", now))
        state.apply(StreamDeltaReady("cli:local", "cli", "local", 0, "我来查。", ""))
        state.apply(
            ToolCallStarted(
                "cli:local",
                "cli",
                "local",
                "call-1",
                "web_search",
                {"query": "今日新闻"},
                0,
            )
        )
        state.apply(
            ToolCallCompleted(
                "cli:local",
                "cli",
                "local",
                "call-1",
                "web_search",
                {"query": "今日新闻"},
                "[]",
                "success",
                0,
            )
        )
        state.apply(StreamDeltaReady("cli:local", "cli", "local", 1, "最终答案", "分析"))
        state.apply(
            TurnFinished(
                "cli:local",
                "cli",
                "local",
                "success",
                now,
                now,
                1.25,
                True,
                OutboundMessage("cli", "local", "权威最终答案"),
            )
        )

        self.assertEqual(state.status, "success")
        self.assertEqual(state.final_content, "权威最终答案")
        self.assertEqual(state.draft_answer, "权威最终答案")
        self.assertEqual(state.tools["call-1"].status, "success")
        self.assertIn("web_search", state.process_text())
        self.assertIn("我来查", state.process_text())

    def test_final_replaces_matching_stream_instead_of_appending(self):
        now = datetime.now().astimezone()
        state = TurnViewState()
        state.apply(TurnStarted("cli:local", "cli", "local", "hello", now))
        state.apply(StreamDeltaReady("cli:local", "cli", "local", 0, "hello", ""))
        state.apply(
            TurnFinished(
                "cli:local",
                "cli",
                "local",
                "success",
                now,
                now,
                0.5,
                True,
                OutboundMessage("cli", "local", "hello"),
            )
        )
        self.assertEqual(state.draft_answer, "hello")
        self.assertNotEqual(state.draft_answer, "hellohello")

    def test_other_session_events_are_ignored(self):
        state = TurnViewState()
        changed = state.apply(
            StreamDeltaReady("telegram:1", "telegram", "1", 0, "foreign", "")
        )
        self.assertFalse(changed)
        self.assertEqual(state.steps, {})


if __name__ == "__main__":
    unittest.main()
