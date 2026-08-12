from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from cloud.channel_gateway import (
    ChannelDeliveryWorker,
    QQBotGatewayAdapter,
    _parse_onebot_message,
)
from cloud.models import Base, ChannelDelivery
from cloud.store import CloudStore, StoreConflictError, StoreNotFoundError


@pytest_asyncio.fixture
async def channel_store():
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


@pytest.mark.asyncio
async def test_pairing_maps_external_identity_without_cross_tenant_access(channel_store):
    store, _ = channel_store
    alice = await store.register_user("channel-a@example.com", "correct-horse-battery")
    bob = await store.register_user("channel-b@example.com", "correct-horse-battery")
    conversation = await store.create_conversation(alice.id, "telegram")
    await store.create_channel_pairing(
        alice.id, conversation.id, "telegram", "one-time-code-123"
    )
    link = await store.consume_channel_pairing(
        "one-time-code-123",
        provider="telegram",
        external_user_id="u42",
        external_chat_id="c42",
        display_name="Alice TG",
    )
    assert link.user_id == alice.id and link.conversation_id == conversation.id
    assert [item.id for item in await store.list_channel_links(alice.id)] == [link.id]
    assert await store.list_channel_links(bob.id) == []
    with pytest.raises(StoreNotFoundError):
        await store.delete_channel_link(bob.id, link.id)
    await store.create_channel_pairing(
        alice.id, conversation.id, "telegram", "another-code"
    )
    with pytest.raises(StoreConflictError):
        await store.consume_channel_pairing(
            "another-code",
            provider="telegram",
            external_user_id="u42",
            external_chat_id="c42",
        )


@pytest.mark.asyncio
async def test_inbound_channel_event_is_durable_and_idempotent(channel_store):
    store, _ = channel_store
    user = await store.register_user("channel-in@example.com", "correct-horse-battery")
    conversation = await store.create_conversation(user.id, "telegram")
    await store.create_channel_pairing(user.id, conversation.id, "telegram", "pair-code-123")
    await store.consume_channel_pairing(
        "pair-code-123",
        provider="telegram",
        external_user_id="u1",
        external_chat_id="c1",
    )
    first_message, first_run, accepted = await store.ingest_channel_message(
        provider="telegram",
        external_event_id="update-100",
        external_chat_id="c1",
        content="hello from telegram",
    )
    replay_message, replay_run, replay_accepted = await store.ingest_channel_message(
        provider="telegram",
        external_event_id="update-100",
        external_chat_id="c1",
        content="duplicate",
    )
    assert accepted is True and replay_accepted is False
    assert replay_message.id == first_message.id and replay_run.id == first_run.id
    assert first_run.status == "queued"


@pytest.mark.asyncio
async def test_assistant_completion_creates_durable_channel_delivery(channel_store, monkeypatch):
    store, factory = channel_store
    user = await store.register_user("channel-out@example.com", "correct-horse-battery")
    conversation = await store.create_conversation(user.id, "telegram")
    await store.create_channel_pairing(user.id, conversation.id, "telegram", "pair-code-out")
    await store.consume_channel_pairing(
        "pair-code-out",
        provider="telegram",
        external_user_id="u2",
        external_chat_id="c2",
    )
    _, run = await store.append_user_message_and_run(user.id, conversation.id, "question")
    claimed = await store.claim_next_run("agent-worker")
    await store.complete_run(claimed.id, "agent-worker", "answer")

    sent = []
    worker = ChannelDeliveryWorker(store, worker_id="channel-worker")
    async def fake_send(provider, chat_id, content):
        sent.append((provider, chat_id, content))
    monkeypatch.setattr(worker, "_send", fake_send)
    assert await worker.run_once() is True
    assert sent == [("telegram", "c2", "answer")]
    async with factory() as session:
        row = await session.scalar(select(ChannelDelivery))
    assert row.status == "sent" and row.sent_at is not None
    await worker.client.aclose()


def test_onebot_segment_parser_preserves_text_and_images():
    text, images = _parse_onebot_message(
        {
            "message": [
                {"type": "text", "data": {"text": "hello "}},
                {"type": "at", "data": {"qq": "1"}},
                {"type": "image", "data": {"url": "https://cdn.example.test/a.jpg"}},
            ]
        }
    )
    assert text == "hello"
    assert images == ["https://cdn.example.test/a.jpg"]


@pytest.mark.asyncio
async def test_onebot_outbound_uses_native_private_and_group_actions(
    channel_store, monkeypatch
):
    store, _ = channel_store
    requests = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"status": "ok", "retcode": 0})

    monkeypatch.setenv("KIRAKIRA_QQ_API_BASE_URL", "https://onebot.example.test")
    monkeypatch.setenv("KIRAKIRA_QQ_ACCESS_TOKEN", "secret")
    worker = ChannelDeliveryWorker(store, worker_id="qq-worker")
    await worker.client.aclose()
    worker.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    await worker._send("qq", "123", "private")
    await worker._send("qq", "gqq:456", "group")
    assert requests[0].url.path == "/send_private_msg"
    assert requests[1].url.path == "/send_group_msg"
    assert requests[0].headers["authorization"] == "Bearer secret"
    await worker.client.aclose()


@pytest.mark.asyncio
async def test_qqbot_gateway_ingests_linked_c2c_idempotently(channel_store):
    store, _ = channel_store
    user = await store.register_user("qqbot@example.com", "correct-horse-battery")
    conversation = await store.create_conversation(user.id, "qqbot")
    await store.create_channel_pairing(user.id, conversation.id, "qqbot", "qqbot-pair-code")
    await store.consume_channel_pairing(
        "qqbot-pair-code",
        provider="qqbot",
        external_user_id="openid-1",
        external_chat_id="c2c:openid-1",
    )
    adapter = QQBotGatewayAdapter(store, "app", "secret")
    await adapter._handle_c2c(
        {
            "id": "qq-message-1",
            "content": "hello",
            "author": {"user_openid": "openid-1"},
        }
    )
    await adapter._handle_c2c(
        {
            "id": "qq-message-1",
            "content": "duplicate",
            "author": {"user_openid": "openid-1"},
        }
    )
    messages = await store.list_messages(user.id, conversation.id)
    assert [item.content for item in messages] == ["hello"]
    await adapter.client.aclose()
