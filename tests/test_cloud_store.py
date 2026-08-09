"""Cloud durable-state and worker contracts."""

from __future__ import annotations

import asyncio
from datetime import UTC, timedelta
from uuid import UUID

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from agent.turns.models import TurnResult
from agent.tool_hooks import (
    ToolExecutionAbortedError,
    ToolExecutionRequest,
    ToolExecutor,
)
from core.schema import ToolResult
from cloud.executor import CLOUD_TRANSCRIPT_COMMIT_KEY
from cloud.models import Base, Run, utc_now
from cloud.store import CloudStore, StoreNotFoundError
from cloud.worker import CloudWorker
from cloud.tool_checkpoints import _signature, build_cloud_tool_checkpoint_hooks


@pytest_asyncio.fixture
async def cloud_store():
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


async def _user(store: CloudStore, suffix: str):
    return await store.register_user(f"{suffix}@example.com", "correct-horse-battery")


@pytest.mark.asyncio
async def test_auth_session_is_opaque_revocable_and_user_bound(cloud_store):
    store, _ = cloud_store
    user = await _user(store, "alice")
    token = "raw-browser-token"
    await store.create_auth_session(user.id, token, ttl_seconds=3600)

    assert (await store.user_for_token(token)).id == user.id
    assert await store.user_for_token("wrong-token") is None
    await store.revoke_auth_session(token)
    assert await store.user_for_token(token) is None


@pytest.mark.asyncio
async def test_conversations_and_runs_never_cross_users(cloud_store):
    store, _ = cloud_store
    alice = await _user(store, "alice")
    bob = await _user(store, "bob")
    conversation = await store.create_conversation(alice.id, "private")
    _, run = await store.append_user_message_and_run(
        alice.id, conversation.id, "alice secret"
    )

    with pytest.raises(StoreNotFoundError):
        await store.get_conversation(bob.id, conversation.id)
    with pytest.raises(StoreNotFoundError):
        await store.list_messages(bob.id, conversation.id)
    with pytest.raises(StoreNotFoundError):
        await store.get_run(bob.id, run.id)


@pytest.mark.asyncio
async def test_same_conversation_runs_are_claimed_serially(cloud_store):
    store, _ = cloud_store
    user = await _user(store, "serial")
    conversation = await store.create_conversation(user.id)
    _, first = await store.append_user_message_and_run(user.id, conversation.id, "one")
    _, second = await store.append_user_message_and_run(user.id, conversation.id, "two")

    claimed = await store.claim_next_run("worker-a")
    assert claimed.id == first.id and claimed.status == "running"
    assert await store.claim_next_run("worker-b") is None

    await store.complete_run(first.id, "worker-a", "first answer")
    claimed_second = await store.claim_next_run("worker-b")
    assert claimed_second.id == second.id
    _, _, _, input_message, history = await store.load_run_input(second.id)
    assert input_message.content == "two"
    assert [(item.role, item.content) for item in history] == [
        ("user", "one"),
        ("assistant", "first answer"),
    ]
    events = await store.list_run_events(user.id, first.id)
    assert [event.event_type for event in events] == [
        "run.queued",
        "run.started",
        "run.completed",
    ]
    assert [event.seq for event in events] == [1, 2, 3]


@pytest.mark.asyncio
async def test_different_conversations_can_be_claimed_in_parallel(cloud_store):
    store, _ = cloud_store
    user = await _user(store, "parallel")
    first_conversation = await store.create_conversation(user.id, "a")
    second_conversation = await store.create_conversation(user.id, "b")
    await store.append_user_message_and_run(user.id, first_conversation.id, "one")
    await store.append_user_message_and_run(user.id, second_conversation.id, "two")

    first = await store.claim_next_run("worker-a")
    second = await store.claim_next_run("worker-b")
    assert first is not None and second is not None
    assert first.conversation_id != second.conversation_id


@pytest.mark.asyncio
async def test_cloud_worker_commits_final_message_and_run(cloud_store):
    store, _ = cloud_store
    user = await _user(store, "worker")
    conversation = await store.create_conversation(user.id)
    _, run = await store.append_user_message_and_run(user.id, conversation.id, "hello")

    class Executor:
        seen = None

        async def execute(self, request):
            self.seen = request
            return TurnResult(request.conversation_id, "hello from cloud")

    executor = Executor()
    worker = CloudWorker(store, executor, worker_id="worker-1")
    assert await worker.run_once() is True
    completed = await store.get_run(user.id, run.id)
    assert completed.status == "completed" and completed.output_message_id is not None
    assert executor.seen.principal.subject_id == str(user.id)
    messages = await store.list_messages(user.id, conversation.id)
    assert [message.content for message in messages] == ["hello", "hello from cloud"]


@pytest.mark.asyncio
async def test_cloud_worker_persists_agent_transcript_delta(cloud_store):
    store, _ = cloud_store
    user = await _user(store, "transcript-delta")
    conversation = await store.create_conversation(user.id)
    _, run = await store.append_user_message_and_run(user.id, conversation.id, "hello")
    assistant_id = UUID("2f7ca88f-08cd-4b73-b89d-e62e0f504589")

    class Executor:
        async def execute(self, request):
            return TurnResult(
                request.conversation_id,
                "durable answer",
                metadata={
                    CLOUD_TRANSCRIPT_COMMIT_KEY: {
                        "assistant_message_id": str(assistant_id),
                        "assistant_metadata": {
                            "reasoning_content": "durable reasoning",
                            "tool_chain": [{"calls": []}],
                        },
                        "conversation_metadata": {"turn_count": 1},
                        "last_consolidated": 0,
                    }
                },
            )

    worker = CloudWorker(store, Executor(), worker_id="worker-1")
    assert await worker.run_once() is True

    messages = await store.list_messages(user.id, conversation.id)
    assert messages[-1].id == assistant_id
    assert messages[-1].agent_metadata["reasoning_content"] == "durable reasoning"
    persisted_conversation = await store.get_conversation(user.id, conversation.id)
    assert persisted_conversation.agent_metadata == {"turn_count": 1}


@pytest.mark.asyncio
async def test_cloud_worker_marks_executor_failure(cloud_store):
    store, _ = cloud_store
    user = await _user(store, "failure")
    conversation = await store.create_conversation(user.id)
    _, run = await store.append_user_message_and_run(user.id, conversation.id, "boom")

    class Executor:
        async def execute(self, request):
            raise RuntimeError("model unavailable")

    worker = CloudWorker(store, Executor(), worker_id="worker-1")
    assert await worker.run_once() is True
    failed = await store.get_run(user.id, run.id)
    assert failed.status == "failed" and "model unavailable" in failed.error


@pytest.mark.asyncio
async def test_heartbeat_renews_lease_and_observes_durable_cancel(cloud_store):
    store, _ = cloud_store
    user = await _user(store, "heartbeat")
    conversation = await store.create_conversation(user.id)
    _, run = await store.append_user_message_and_run(user.id, conversation.id, "wait")
    claimed = await store.claim_next_run("worker-1", lease_seconds=10)
    original_expiry = claimed.lease_expires_at

    assert not await store.heartbeat_run(run.id, "worker-1", lease_seconds=30)
    renewed = await store.get_run(user.id, run.id)
    renewed_expiry = renewed.lease_expires_at.replace(tzinfo=UTC)
    original_expiry = original_expiry.replace(tzinfo=UTC)
    assert renewed_expiry > original_expiry

    await store.request_cancel(user.id, run.id)
    assert await store.heartbeat_run(run.id, "worker-1", lease_seconds=30)


@pytest.mark.asyncio
async def test_worker_cancels_active_executor_from_durable_request(cloud_store):
    store, _ = cloud_store
    user = await _user(store, "cancel-running")
    conversation = await store.create_conversation(user.id)
    _, run = await store.append_user_message_and_run(user.id, conversation.id, "wait")
    started = asyncio.Event()
    interrupted = asyncio.Event()

    class Executor:
        async def execute(self, request):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                interrupted.set()
                raise

    worker = CloudWorker(
        store,
        Executor(),
        worker_id="worker-1",
        heartbeat_interval_seconds=0.01,
    )
    work = asyncio.create_task(worker.run_once())
    await asyncio.wait_for(started.wait(), timeout=1)
    await store.request_cancel(user.id, run.id)

    assert await asyncio.wait_for(work, timeout=1) is True
    assert interrupted.is_set()
    cancelled = await store.get_run(user.id, run.id)
    assert cancelled.status == "cancelled"
    assert cancelled.lease_owner is None
    messages = await store.list_messages(user.id, conversation.id)
    assert [(message.role, message.content) for message in messages] == [
        ("user", "wait")
    ]


@pytest.mark.asyncio
async def test_cancel_wins_race_with_run_completion(cloud_store):
    store, _ = cloud_store
    user = await _user(store, "cancel-at-completion")
    conversation = await store.create_conversation(user.id)
    _, run = await store.append_user_message_and_run(user.id, conversation.id, "wait")
    await store.claim_next_run("worker-1")
    await store.request_cancel(user.id, run.id)

    finished, output = await store.complete_run(run.id, "worker-1", "too late")

    assert finished.status == "cancelled" and output is None
    messages = await store.list_messages(user.id, conversation.id)
    assert [(message.role, message.content) for message in messages] == [
        ("user", "wait")
    ]


@pytest.mark.asyncio
async def test_expired_lease_is_requeued_and_attempted_again(cloud_store):
    store, factory = cloud_store
    user = await _user(store, "expired")
    conversation = await store.create_conversation(user.id)
    _, run = await store.append_user_message_and_run(user.id, conversation.id, "retry")
    first = await store.claim_next_run("worker-dead")
    assert first.attempt == 1
    async with factory.begin() as session:
        persisted = await session.get(Run, run.id)
        persisted.lease_expires_at = utc_now() - timedelta(seconds=1)

    assert await store.requeue_expired_runs() == 1
    recovered = await store.get_run(user.id, run.id)
    assert recovered.status == "queued" and recovered.lease_owner is None
    second = await store.claim_next_run("worker-live")
    assert second.id == run.id and second.attempt == 2


@pytest.mark.asyncio
async def test_run_forever_stops_while_idle(cloud_store):
    store, _ = cloud_store

    class Executor:
        async def execute(self, request):
            raise AssertionError("no Run should be executed")

    worker = CloudWorker(
        store,
        Executor(),
        worker_id="worker-1",
        poll_interval_seconds=0.01,
        reaper_interval_seconds=0.01,
    )
    service = asyncio.create_task(worker.run_forever())
    await asyncio.sleep(0.03)
    worker.stop()
    await asyncio.wait_for(service, timeout=1)


@pytest.mark.asyncio
async def test_run_events_and_rate_limits_are_user_scoped_and_durable(cloud_store):
    store, _ = cloud_store
    alice = await _user(store, "events-alice")
    bob = await _user(store, "events-bob")
    conversation = await store.create_conversation(alice.id)
    _, run = await store.append_user_message_and_run(
        alice.id, conversation.id, "stream me"
    )

    events = await store.list_run_events(alice.id, run.id)
    assert [(event.seq, event.event_type) for event in events] == [
        (1, "run.queued")
    ]
    with pytest.raises(StoreNotFoundError):
        await store.list_run_events(bob.id, run.id)

    first = await store.consume_rate_limit(
        "user:alice", "messages", limit=2, window_seconds=60
    )
    second = await store.consume_rate_limit(
        "user:alice", "messages", limit=2, window_seconds=60
    )
    rejected = await store.consume_rate_limit(
        "user:alice", "messages", limit=2, window_seconds=60
    )
    other_scope = await store.consume_rate_limit(
        "user:alice", "login", limit=2, window_seconds=60
    )
    assert first.allowed and first.remaining == 1
    assert second.allowed and second.remaining == 0
    assert not rejected.allowed and rejected.retry_after_seconds > 0
    assert other_scope.allowed


@pytest.mark.asyncio
async def test_tool_checkpoint_replays_result_and_fences_ambiguous_side_effect(
    cloud_store,
):
    store, _ = cloud_store
    user = await _user(store, "checkpoint")
    conversation = await store.create_conversation(user.id)
    _, run = await store.append_user_message_and_run(
        user.id, conversation.id, "change something"
    )
    executor = ToolExecutor(build_cloud_tool_checkpoint_hooks(store))
    invoked = 0

    async def invoke(name: str, arguments: dict) -> ToolResult:
        nonlocal invoked
        invoked += 1
        return ToolResult("call", f"changed:{name}:{arguments['value']}")

    request = ToolExecutionRequest(
        str(conversation.id),
        "api",
        str(run.id),
        "external_write",
        {"value": 7},
        iteration=0,
        call_index=0,
    )
    first = await executor.execute(request, invoke)
    replay = await executor.execute(request, invoke)
    assert first.status == replay.status == "success"
    assert first.output == replay.output == "changed:external_write:7"
    assert invoked == 1

    ambiguous_request = ToolExecutionRequest(
        str(conversation.id),
        "api",
        str(run.id),
        "external_write",
        {"value": 8},
        iteration=1,
        call_index=0,
    )
    await store.begin_tool_checkpoint(
        run.id,
        iteration=1,
        call_index=0,
        signature=_signature("external_write", {"value": 8}),
        tool_name="external_write",
        arguments={"value": 8},
    )
    with pytest.raises(ToolExecutionAbortedError, match="ambiguous"):
        await executor.execute(ambiguous_request, invoke)
    assert invoked == 1
