from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from cloud.drift_store import UserScopedPostgresDriftStore
from cloud.models import Base, User


def test_cloud_drift_state_preserves_schedule_fairness_journal_and_user_scope() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as db, db.begin():
        alice = User(email="drift-a@example.com", password_hash="x")
        bob = User(email="drift-b@example.com", password_hash="x")
        db.add_all([alice, bob])
        db.flush()
        alice_id, bob_id = alice.id, bob.id
    alice_store = UserScopedPostgresDriftStore(engine, alice_id)
    bob_store = UserScopedPostgresDriftStore(engine, bob_id)
    now = datetime(2026, 8, 9, 12, tzinfo=UTC)

    alice_store.record_run(
        skill="review-memory",
        now=now,
        status="completed",
        briefing="checked one fact",
        message_result="silent",
    )
    assert alice_store.last_drift_at() == now
    assert bob_store.last_drift_at() is None
    assert not alice_store.can_run(now + timedelta(hours=2), 3)
    assert alice_store.can_run(now + timedelta(hours=3), 3)
    assert alice_store.last_run_at_by_skill() == {"review-memory": now}

    due = now + timedelta(hours=4)
    alice_store.save_schedule("conversation-a", "anchor", due, now)
    assert alice_store.load_schedule("conversation-a") == {
        "timer_anchor": "anchor",
        "next_attempt_at": due,
    }
    assert bob_store.load_schedule("conversation-a") is None
    alice_store.clear_schedule("conversation-a")
    assert alice_store.load_schedule("conversation-a") is None

    alice_store.append_journal(
        "review-memory",
        "self_observation",
        {"note": "avoid repeats"},
        now,
        key="memory-1",
    )
    assert alice_store.load_journal("review-memory")[0]["payload"] == {
        "note": "avoid repeats"
    }
    assert alice_store.recent_self_observations()[0]["skill"] == "review-memory"
    assert bob_store.recent_self_observations() == []

    alice_store.save_continuum(
        skill="review-memory",
        now=now,
        scratchpad="next item 2",
        next_tendency="verify date",
    )
    assert alice_store.get_continuum("review-memory") == {
        "scratchpad": "next item 2",
        "next_tendency": "verify date",
    }
    assert bob_store.get_continuum("review-memory") == {
        "scratchpad": "",
        "next_tendency": "",
    }
    engine.dispose()
