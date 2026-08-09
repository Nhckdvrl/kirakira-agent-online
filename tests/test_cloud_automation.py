from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from cloud.models import Base
from cloud.store import CloudStore, StoreStateError


@pytest_asyncio.fixture
async def automation_store():
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    store = CloudStore(async_sessionmaker(engine, expire_on_commit=False))
    yield store
    await engine.dispose()


@pytest.mark.asyncio
async def test_automation_target_inbox_claim_and_delivery_are_durable(automation_store):
    store = automation_store
    user = await store.register_user(
        "automation@example.com", "correct-horse-battery"
    )
    first = await store.create_conversation(user.id, "first")
    second = await store.create_conversation(user.id, "second")
    await store.configure_automation(
        user.id,
        first.id,
        proactive_enabled=True,
        drift_enabled=True,
        proactive_context="only urgent items",
    )
    item_id, accepted = await store.ingest_proactive_event(
        user.id,
        first.id,
        kind="content",
        source_id="feed",
        event_id="42",
        payload={"title": "durable item"},
    )
    assert accepted
    assert (await store.ingest_proactive_event(
        user.id,
        first.id,
        kind="content",
        source_id="feed",
        event_id="42",
        payload={"title": "duplicate"},
    )) == (item_id, False)

    claimed = await store.claim_next_automation("worker")
    assert claimed is not None and claimed.conversation_id == first.id
    inbox = await store.fetch_automation_inbox(user.id, first.id)
    assert inbox[0]["item_id"] == item_id
    message = await store.append_automation_message(
        user_id=user.id,
        conversation_id=first.id,
        worker_id="worker",
        tick_token=claimed.tick_token,
        source="proactive",
        content="a push",
        metadata={"proactive": True},
    )
    replay = await store.append_automation_message(
        user_id=user.id,
        conversation_id=first.id,
        worker_id="worker",
        tick_token=claimed.tick_token,
        source="proactive",
        content="must not duplicate",
    )
    assert replay.id == message.id
    await store.finish_automation(
        first.id,
        "worker",
        next_tick_at=datetime.now(UTC) + timedelta(hours=1),
    )
    assert [item.content for item in await store.list_messages(user.id, first.id)] == [
        "a push"
    ]

    # Selecting another target disables the old target, preventing user-scoped
    # Proactive/Drift reservoirs from crossing conversation boundaries.
    await store.configure_automation(
        user.id,
        second.id,
        proactive_enabled=False,
        drift_enabled=True,
    )
    old = await store.get_automation(user.id, first.id)
    assert old is not None and not old.enabled


@pytest.mark.asyncio
async def test_automation_cannot_deliver_while_passive_run_is_queued(automation_store):
    store = automation_store
    user = await store.register_user("race@example.com", "correct-horse-battery")
    conversation = await store.create_conversation(user.id)
    await store.configure_automation(
        user.id, conversation.id, proactive_enabled=True, drift_enabled=False
    )
    claimed = await store.claim_next_automation("worker")
    assert claimed is not None
    await store.append_user_message_and_run(user.id, conversation.id, "hello")
    with pytest.raises(StoreStateError):
        await store.append_automation_message(
            user_id=user.id,
            conversation_id=conversation.id,
            worker_id="worker",
            tick_token=claimed.tick_token,
            source="proactive",
            content="race",
        )
