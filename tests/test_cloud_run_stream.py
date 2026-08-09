from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from bus.event_bus import EventBus
from bus.events_lifecycle import StreamDeltaReady, ToolCallCompleted, ToolCallStarted
from cloud.models import Base
from cloud.run_stream import DurableRunStreamBridge
from cloud.store import CloudStore


@pytest.mark.asyncio
async def test_stream_bridge_batches_text_and_redacts_tool_payloads() -> None:
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    store = CloudStore(async_sessionmaker(engine, expire_on_commit=False))
    user = await store.register_user(
        "stream@example.com", "correct-horse-battery"
    )
    conversation = await store.create_conversation(user.id)
    _, run = await store.append_user_message_and_run(
        user.id, conversation.id, "hello"
    )
    bus = EventBus()
    bridge = DurableRunStreamBridge(
        store, flush_interval_seconds=10, flush_chars=100
    )
    bridge.bind(bus)
    await bus.fanout(
        StreamDeltaReady(
            str(conversation.id), "api", str(run.id), 0, "hello ", "secret thought"
        )
    )
    await bus.fanout(
        StreamDeltaReady(
            str(conversation.id), "api", str(run.id), 0, "world", "more thought"
        )
    )
    await bus.fanout(
        ToolCallStarted(
            str(conversation.id),
            "api",
            str(run.id),
            "call-1",
            "write_file",
            {"content": "private"},
            1,
        )
    )
    await bus.fanout(
        ToolCallCompleted(
            str(conversation.id),
            "api",
            str(run.id),
            "call-1",
            "write_file",
            {"content": "private"},
            "private result",
            "success",
            1,
        )
    )
    events = await store.list_run_events(user.id, run.id)
    assert [event.event_type for event in events] == [
        "run.queued",
        "run.output.delta",
        "tool.started",
        "tool.completed",
    ]
    assert events[1].data == {"content": "hello world", "iteration": 0}
    serialized = repr([event.data for event in events])
    assert "secret thought" not in serialized
    assert "private result" not in serialized
    assert "private" not in serialized
    await bridge.close()
    await engine.dispose()
