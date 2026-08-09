"""Opt-in tests for PostgreSQL locking and pgvector behavior."""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from cloud.database import async_database_url, sync_database_url
from cloud.memory_store import UserScopedPostgresMemoryStore
from cloud.store import CloudStore


pytestmark = pytest.mark.skipif(
    not os.getenv("KIRAKIRA_TEST_POSTGRES_URL"),
    reason="KIRAKIRA_TEST_POSTGRES_URL is not configured",
)


@pytest.mark.asyncio
async def test_real_postgres_locking_rate_limit_and_pgvector() -> None:
    raw_url = os.environ["KIRAKIRA_TEST_POSTGRES_URL"]
    async_engine = create_async_engine(async_database_url(raw_url), pool_pre_ping=True)
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    store = CloudStore(factory)
    suffix = uuid4().hex
    user = await store.register_user(
        f"pg-{suffix}@example.com", "correct-horse-battery"
    )
    conversation = await store.create_conversation(user.id, "postgres integration")
    try:
        await store.configure_automation(
            user.id,
            conversation.id,
            proactive_enabled=True,
            drift_enabled=True,
        )
        claims = await asyncio.gather(
            *(store.claim_next_automation(f"worker-{index}") for index in range(12))
        )
        claimed = [row for row in claims if row is not None]
        assert len(claimed) == 1

        decisions = await asyncio.gather(
            *(
                store.consume_rate_limit(
                    f"pg:{suffix}", "integration", limit=10, window_seconds=60
                )
                for _ in range(30)
            )
        )
        assert sum(item.allowed for item in decisions) == 10

        # Concurrent appends serialize on the conversation row and never reuse seq.
        await asyncio.gather(
            *(
                store.append_user_message_and_run(
                    user.id, conversation.id, f"message-{index}"
                )
                for index in range(20)
            )
        )
        messages = await store.list_messages(user.id, conversation.id)
        assert [item.seq for item in messages] == list(range(1, 21))

        sync_engine = create_engine(sync_database_url(raw_url), pool_pre_ping=True)
        memory = UserScopedPostgresMemoryStore(sync_engine)
        with memory.bind_user(user.id):
            result = memory.upsert_item(
                "fact", "pgvector integration", [1.0] + [0.0] * 1023
            )
            item_id = result.split(":", 1)[1]
        with sync_engine.connect() as connection:
            mirrored = connection.scalar(
                text(
                    "SELECT embedding_vector IS NOT NULL FROM memory_items "
                    "WHERE user_id=:user_id AND id=:item_id"
                ),
                {"user_id": user.id, "item_id": item_id},
            )
            distance = connection.scalar(
                text(
                    "SELECT embedding_vector <=> CAST(:query AS vector(1024)) "
                    "FROM memory_items WHERE user_id=:user_id AND id=:item_id"
                ),
                {
                    "query": "[1," + ",".join(["0"] * 1023) + "]",
                    "user_id": user.id,
                    "item_id": item_id,
                },
            )
        assert mirrored is True
        assert float(distance) == pytest.approx(0.0)
        sync_engine.dispose()
    finally:
        async with factory.begin() as session:
            await session.execute(text("DELETE FROM users WHERE id=:user_id"), {"user_id": user.id})
        await async_engine.dispose()
