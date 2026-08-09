"""Context degradation must remain a projection over append-only history."""

from __future__ import annotations

from pathlib import Path

import pytest

from session.manager import SessionManager


def test_normal_save_rejects_historical_message_removal(tmp_path: Path) -> None:
    manager = SessionManager(tmp_path)
    session = manager.get_or_create("web:user")
    session.add_message("user", "immutable user message")
    session.add_message("assistant", "immutable assistant message")
    manager.save(session)
    durable = manager._index.execute(
        "SELECT id, seq, role, content, ts, extra_json FROM messages ORDER BY seq"
    ).fetchall()

    session.messages.pop(0)
    with pytest.raises(RuntimeError, match="不得删除"):
        manager.save(session)

    after = manager._index.execute(
        "SELECT id, seq, role, content, ts, extra_json FROM messages ORDER BY seq"
    ).fetchall()
    assert after == durable
    manager.close()
