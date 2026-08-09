"""Durable plugin reload transaction tests (ported from Reference)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from agent.plugins.reload_journal import ReloadJournal


class ReloadJournalTests(unittest.TestCase):
    def test_records_phases_and_survives_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp) / "workspace"
            journal = ReloadJournal(workspace)
            tx_id = journal.begin(
                plugin_id="weather",
                base_snapshot_id="snapshot-v1",
                generation_id="weather:source-v2:2",
                source_revision="source-v2",
                config_revision="config-v2",
            )
            journal.advance(tx_id, "prepared", candidate_snapshot_id="snapshot-v2")
            journal.advance(tx_id, "validating")
            journal.advance(tx_id, "commit_started")
            journal.advance(tx_id, "committed")
            journal.advance(tx_id, "draining")

            record = journal.get(tx_id)
            self.assertEqual(record.phase, "draining")
            self.assertEqual(
                [event.phase for event in journal.events(tx_id)],
                [
                    "preparing",
                    "prepared",
                    "validating",
                    "commit_started",
                    "committed",
                    "draining",
                ],
            )
            self.assertEqual(ReloadJournal(workspace).get(tx_id), record)

    def test_rejects_invalid_transition(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            journal = ReloadJournal(Path(tmp))
            tx_id = journal.begin(
                plugin_id="weather",
                base_snapshot_id=None,
                generation_id="g2",
                source_revision="s2",
                config_revision="c2",
            )
            with self.assertRaisesRegex(RuntimeError, "状态跳转无效"):
                journal.advance(tx_id, "committed")

    def test_builds_deterministic_recovery_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            journal = ReloadJournal(Path(tmp))
            prepared = journal.begin(
                plugin_id="weather",
                base_snapshot_id="s1",
                generation_id="g2",
                source_revision="source-v2",
                config_revision="config-v2",
            )
            journal.advance(prepared, "prepared")
            committed = journal.begin(
                plugin_id="calendar",
                base_snapshot_id="s1",
                generation_id="g3",
                source_revision="source-v3",
                config_revision="config-v3",
            )
            journal.advance(committed, "prepared")
            journal.advance(committed, "validating")
            journal.advance(committed, "commit_started")

            actions = journal.pending_recovery()
            self.assertEqual(
                [(action.tx_id, action.action) for action in actions],
                [(committed, "restore_committed"), (prepared, "discard_candidate")],
            )
            for action in actions:
                journal.finish_recovery(action)
            self.assertEqual(journal.pending_recovery(), ())


if __name__ == "__main__":
    unittest.main()
