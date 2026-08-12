from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from agent.tools.registry import ToolRegistry
from cloud.models import Base, Message, Run
from cloud.scheduler import CloudScheduleWorker, CloudSchedulerTools, create_cloud_schedule
from cloud.store import CloudStore, StoreNotFoundError


@pytest_asyncio.fixture
async def scheduler_store():
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield CloudStore(factory), factory
    await engine.dispose()


async def _scope(store: CloudStore, suffix: str):
    user = await store.register_user(
        f"{suffix}@example.com", "correct-horse-battery"
    )
    conversation = await store.create_conversation(user.id, suffix)
    return user, conversation


@pytest.mark.asyncio
async def test_cloud_schedule_tools_preserve_cron_timezone_and_user_scope(scheduler_store):
    store, _ = scheduler_store
    alice, conversation = await _scope(store, "schedule-alice")
    bob, _ = await _scope(store, "schedule-bob")
    tools = ToolRegistry()
    CloudSchedulerTools(store, tools)
    tools.set_context(principal_id=str(alice.id), chat_id=str(conversation.id))

    result = await tools._tools["schedule"].handler(
        message="daily",
        tier="instant",
        trigger="every",
        when="0 9 * * *",
        timezone="Asia/Tokyo",
        name="morning",
    )
    created = json.loads(result)
    assert created["cron_expr"] == "0 9 * * *"
    assert created["timezone"] == "Asia/Tokyo"
    assert created["remaining_runs"] == -1
    assert len(await store.list_scheduled_jobs(alice.id)) == 1
    assert await store.list_scheduled_jobs(bob.id) == []
    with pytest.raises(StoreNotFoundError):
        await store.cancel_scheduled_job(bob.id, created["id"])


@pytest.mark.asyncio
async def test_instant_schedule_is_idempotently_delivered_to_conversation(scheduler_store):
    store, factory = scheduler_store
    user, conversation = await _scope(store, "schedule-instant")
    job = await create_cloud_schedule(
        store,
        user.id,
        conversation.id,
        message="stand up",
        delay_seconds=60,
    )
    async with factory.begin() as session:
        persisted = await session.get(type(job), job.id)
        persisted.run_at = datetime.now(UTC) - timedelta(seconds=1)
    worker = CloudScheduleWorker(store, worker_id="scheduler-a")
    assert await worker.run_once() is True
    messages = await store.list_messages(user.id, conversation.id)
    assert [(item.role, item.content) for item in messages] == [
        ("assistant", "stand up")
    ]
    stored = (await store.list_scheduled_jobs(user.id, include_finished=True))[0]
    assert stored.status == "completed"


@pytest.mark.asyncio
async def test_soft_schedule_queues_isolated_agent_run_with_memory_disabled(scheduler_store):
    store, factory = scheduler_store
    user, conversation = await _scope(store, "schedule-soft")
    job = await create_cloud_schedule(
        store,
        user.id,
        conversation.id,
        tier="soft",
        prompt="summarize the latest status",
        delay_seconds=60,
    )
    async with factory.begin() as session:
        persisted = await session.get(type(job), job.id)
        persisted.run_at = datetime.now(UTC) - timedelta(seconds=1)
    worker = CloudScheduleWorker(store, worker_id="scheduler-a")
    assert await worker.run_once() is True
    async with factory() as session:
        run = await session.scalar(__import__("sqlalchemy").select(Run))
        message = await session.get(Message, run.input_message_id)
    assert run.status == "queued"
    assert message.agent_metadata["session_key_override"].startswith("scheduler:job_")
    assert message.agent_metadata["skip_post_memory"] is True
    assert message.agent_metadata["skip_memory_retrieval"] is True
    assert "recall_memory" in message.agent_metadata["disabled_tools"]


@pytest.mark.asyncio
async def test_misfire_and_lease_claim_semantics(scheduler_store):
    store, factory = scheduler_store
    user, conversation = await _scope(store, "schedule-misfire")
    stale = await create_cloud_schedule(
        store, user.id, conversation.id, message="stale", delay_seconds=60
    )
    repeating = await create_cloud_schedule(
        store,
        user.id,
        conversation.id,
        message="repeat",
        delay_seconds=60,
        interval_seconds=3600,
        repeat_count=10,
    )
    async with factory.begin() as session:
        for item in (stale, repeating):
            persisted = await session.get(type(item), item.id)
            persisted.run_at = datetime.now(UTC) - timedelta(hours=6)
    assert await store.claim_next_scheduled_job("worker-a") is None
    jobs = await store.list_scheduled_jobs(user.id, include_finished=True)
    by_id = {item.id: item for item in jobs}
    assert by_id[stale.id].status == "missed"
    assert by_id[repeating.id].status == "pending"
    restored_run_at = by_id[repeating.id].run_at
    if restored_run_at.tzinfo is None:
        restored_run_at = restored_run_at.replace(tzinfo=UTC)
    assert restored_run_at > datetime.now(UTC)
