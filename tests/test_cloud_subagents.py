from __future__ import annotations

import json
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from agent.core.runtime import ReasonerResult
from agent.tools.registry import ToolRegistry
from cloud.models import Base
from cloud.store import CloudStore
from cloud.subagents import CloudSubagentRuntime
from core.memory.engine import MemoryQueryResult


@pytest_asyncio.fixture
async def subagent_store():
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield CloudStore(factory)
    await engine.dispose()


class FakeMemory:
    async def query(self, request):
        assert request.scope.user_id
        return MemoryQueryResult(text_block="remembered context")


class FakeReasoner:
    async def run_turn(self, **kwargs):
        assert kwargs["retrieved_memory_block"] == "remembered context"
        assert "spawn" in kwargs["disabled_tools"]
        return ReasonerResult("child result", tools_used=["web_search"])


@pytest.mark.asyncio
async def test_inline_and_background_subagents_are_tenant_durable(subagent_store) -> None:
    user = await subagent_store.register_user("subagent@example.com", "correct-horse-battery")
    conversation = await subagent_store.create_conversation(user.id, "parent")
    tools = ToolRegistry()
    runtime = CloudSubagentRuntime(
        store=subagent_store,
        reasoner=FakeReasoner(),
        tools=tools,
        memory_engine=FakeMemory(),
        worker_id="subagent-worker",
    )
    runtime.register_tools()
    token = tools.set_context(
        principal_id=str(user.id),
        chat_id=str(conversation.id),
        session_key=str(conversation.id),
    )
    try:
        inline = json.loads(await runtime.spawn("research inline", mode="inline"))
        background = json.loads(await runtime.spawn("research later", mode="background"))
        assert inline["status"] == "completed" and inline["result"] == "child result"
        assert background["status"] == "queued"
        listing = json.loads(await runtime.manage("list"))
        assert listing["running_count"] == 1
    finally:
        tools.reset_context(token)
    assert await runtime.run_once() is True
    jobs = await subagent_store.list_subagent_jobs(user.id)
    assert {item.status for item in jobs} == {"completed"}
    messages = await subagent_store.list_messages(user.id, conversation.id)
    assert messages[-1].content.endswith("child result")


@pytest.mark.asyncio
async def test_queued_subagent_can_be_cancelled_without_execution(subagent_store) -> None:
    user = await subagent_store.register_user("subagent-cancel@example.com", "correct-horse-battery")
    conversation = await subagent_store.create_conversation(user.id, "parent")
    job = await subagent_store.create_subagent_job(
        user.id,
        conversation.id,
        task="cancel me",
        label="cancel",
        profile="general",
        max_iterations=2,
    )
    cancelled = await subagent_store.cancel_subagent_job(user.id, job.id)
    assert cancelled.status == "cancelled"
    assert await subagent_store.claim_next_subagent_job("worker") is None
