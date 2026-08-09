from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from cloud.models import Base, User
from cloud.proactive_store import UserScopedPostgresProactiveStore


def test_cloud_proactive_state_preserves_reservoir_delivery_and_tick_contracts() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as db, db.begin():
        alice = User(email="proactive-a@example.com", password_hash="x")
        bob = User(email="proactive-b@example.com", password_hash="x")
        db.add_all([alice, bob])
        db.flush()
        alice_id, bob_id = alice.id, bob.id
    alice_store = UserScopedPostgresProactiveStore(engine, alice_id)
    bob_store = UserScopedPostgresProactiveStore(engine, bob_id)
    now = datetime(2026, 8, 9, 12, tzinfo=UTC)
    events = [
        {
            "item_id": "feed:1",
            "_source": "feed",
            "event_id": "1",
            "title": "one",
        },
        {
            "item_id": "feed:2",
            "_source": "feed",
            "event_id": "2",
            "title": "two",
        },
    ]

    assert alice_store.ingest("content", events, now) == ["feed:1", "feed:2"]
    assert alice_store.ingest("content", events, now) == []
    assert alice_store.unread_count("content") == 2
    assert bob_store.unread_count("content") == 0
    alice_store.consume_and_queue_ack(
        ["feed:1"], {"feed": ["1"]}, now
    )
    assert [item["item_id"] for item in alice_store.unread("content")] == [
        "feed:2"
    ]
    assert alice_store.pending_acknowledgements() == {"feed": ["1"]}
    alice_store.mark_acknowledged("feed", ["1"])
    assert alice_store.pending_acknowledgements() == {}

    alice_store.queue_feedback(
        "feed", ["2"], "interesting", now, reason="selected"
    )
    assert alice_store.pending_feedback() == [
        {
            "source_id": "feed",
            "source_event_id": "2",
            "feedback": "interesting",
            "reason": "selected",
        }
    ]
    alice_store.mark_feedback_sent("feed", "2", "interesting")
    assert alice_store.pending_feedback() == []

    alice_store.mark_delivery("conversation-a", "hash", now)
    assert alice_store.is_delivery_duplicate(
        "conversation-a", "hash", 1, now + timedelta(minutes=30)
    )
    assert not bob_store.is_delivery_duplicate(
        "conversation-a", "hash", 1, now + timedelta(minutes=30)
    )
    alice_store.unmark_delivery("conversation-a", "hash")
    assert not alice_store.is_delivery_duplicate(
        "conversation-a", "hash", 1, now
    )
    alice_store.mark_push("conversation-a", now)
    assert alice_store.in_cooldown(
        "conversation-a", now + timedelta(minutes=30), 1
    )

    alice_store.record_decision(now, "content_sent", "feed:2")
    assert alice_store.recent_decisions()[0]["action"] == "content_sent"
    alice_store.record_tick_start("tick-1", "conversation-a", now)
    alice_store.record_tick_step(
        "tick-1", 0, "gate", "completed", 4, terminal=None
    )
    alice_store.record_tick_finish(
        "tick-1", now + timedelta(seconds=1), "content_sent", "completed", 1
    )
    tick = alice_store.recent_ticks()[0]
    assert tick["terminal"] == "content_sent"
    assert tick["steps"] == [
        {
            "step_index": 0,
            "slot": "gate",
            "status": "completed",
            "terminal": None,
            "duration_ms": 4,
            "error": "",
        }
    ]
    assert bob_store.recent_ticks() == []
    engine.dispose()
