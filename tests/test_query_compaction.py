"""Query-local compaction and persisted replay tests."""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from agent.model_runtime.query_compaction import QueryCompactor
from session.manager import Session, SessionManager


def _batch(index: int, size: int = 700) -> list[dict[str, object]]:
    call_id = f"call-{index}"
    return [
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {
                    "id": call_id,
                    "type": "function",
                    "function": {"name": "read_file", "arguments": {"i": index}},
                }
            ],
        },
        {
            "role": "tool",
            "tool_call_id": call_id,
            "content": f"result-{index}-" + ("x" * size),
        },
    ]


def _tool_group(index: int) -> dict[str, object]:
    return {
        "text": "",
        "calls": [
            {
                "call_id": f"call-{index}",
                "name": "read_file",
                "arguments": {"i": index},
                "result": f"result-{index}",
            }
        ],
    }


class QueryCompactionTests(unittest.TestCase):
    def test_live_shell_origin_is_pinned_until_terminal_tool_result(self) -> None:
        async def scenario() -> None:
            base = [{"role": "user", "content": "run task"}]
            shell_batch = [
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "shell-call",
                            "type": "function",
                            "function": {"name": "bash", "arguments": {"command": "x"}},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "shell-call",
                    "content": json.dumps(
                        {"execution_id": 1234, "process_status": "running"}
                    ),
                },
            ]
            batches = [shell_batch, _batch(1), _batch(2)]
            messages = [*base, *[item for batch in batches for item in batch]]
            compactor = QueryCompactor(
                base_messages=base,
                context_window=2000,
                soft_limit_tokens=1,
                scope_id="web:test",
                estimate=lambda value: len(json.dumps(value)) // 4,
            )
            for batch in batches:
                compactor.record_completed_batch(batch)

            compacted = await compactor.prepare(
                messages,
                summarize=lambda _prompt: asyncio.sleep(0, result="summary"),
            )

            self.assertFalse(compacted)
            self.assertEqual(messages[1]["tool_calls"][0]["id"], "shell-call")

        asyncio.run(scenario())

    def test_compacts_only_closed_prefix_and_keeps_newest_batch(self) -> None:
        async def scenario() -> None:
            base = [{"role": "user", "content": "inspect repository"}]
            batches = [_batch(index) for index in range(3)]
            messages = [*base, *[item for batch in batches for item in batch]]
            compactor = QueryCompactor(
                base_messages=base,
                context_window=2000,
                soft_limit_tokens=100,
                scope_id="web:test",
                estimate=lambda value: len(json.dumps(value)) // 4,
            )
            for batch in batches:
                compactor.record_completed_batch(batch)

            compacted = await compactor.prepare(
                messages,
                summarize=lambda _prompt: asyncio.sleep(
                    0, result="## Goal\ninspect\n## Progress\nread old files"
                ),
            )

            self.assertTrue(compacted)
            payload = compactor.persistence_payload()
            self.assertIsNotNone(payload)
            self.assertGreaterEqual(int(payload["compacted_tool_groups"]), 1)
            self.assertEqual(messages[-1]["content"], batches[-1][-1]["content"])
            compact_call = messages[1]["tool_calls"][0]
            self.assertEqual(compact_call["function"]["name"], "context_compact")

        asyncio.run(scenario())

    def test_persisted_projection_replays_summary_plus_uncompacted_suffix(self) -> None:
        session = Session("web:test")
        session.add_message("user", "inspect")
        session.add_message(
            "assistant",
            "done",
            tool_chain=[_tool_group(0), _tool_group(1), _tool_group(2)],
            react_compaction={
                "schema_version": 1,
                "summary": "## Goal\ninspect\n## Progress\nfirst two reads complete",
                "compacted_tool_groups": 2,
                "generation": 1,
                "trigger": "soft_limit",
                "context_window": 2000,
                "soft_limit_tokens": 1480,
                "estimated_tokens_before": 1600,
                "estimated_tokens_after": 700,
            },
        )

        history = session.get_history()

        call_names = [
            call["function"]["name"]
            for message in history
            for call in message.get("tool_calls", [])
        ]
        self.assertEqual(call_names, ["context_compact", "read_file"])
        self.assertIn("first two reads", history[2]["content"])
        self.assertEqual(history[-1], {"role": "assistant", "content": "done"})

    def test_compaction_metadata_is_appended_without_rewriting_old_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manager = SessionManager(Path(tmp))
            session = manager.get_or_create("web:test")
            session.add_message("user", "old immutable row")
            manager.save(session)
            before = manager._index.execute(
                "SELECT id, seq, role, content, ts, extra_json FROM messages ORDER BY seq"
            ).fetchall()
            session.add_message(
                "assistant",
                "done",
                tool_chain=[_tool_group(0), _tool_group(1)],
                react_compaction={
                    "schema_version": 1,
                    "summary": "## Goal\ninspect",
                    "compacted_tool_groups": 1,
                    "generation": 1,
                    "trigger": "soft_limit",
                    "context_window": 2000,
                    "soft_limit_tokens": 1480,
                    "estimated_tokens_before": 1600,
                    "estimated_tokens_after": 700,
                },
            )
            manager.save(session)
            after = manager._index.execute(
                "SELECT id, seq, role, content, ts, extra_json FROM messages ORDER BY seq"
            ).fetchall()

            self.assertEqual(after[:1], before)
            self.assertEqual(len(after), 2)
            self.assertIn("react_compaction", json.loads(after[1][-1]))


if __name__ == "__main__":
    unittest.main()
