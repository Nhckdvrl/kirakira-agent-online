"""HTTP contracts for the first Kirakira Cloud API slice."""

from __future__ import annotations

import httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from cloud.api import create_app
from cloud.database import CloudSettings
from cloud.models import Base


@pytest_asyncio.fixture
async def api_client():
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    app = create_app(
        session_factory=factory,
        settings=CloudSettings("sqlite+aiosqlite://", session_cookie_secure=False),
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    await engine.dispose()


async def _register(client: httpx.AsyncClient, email: str):
    response = await client.post(
        "/v1/auth/register",
        json={"email": email, "password": "correct-horse-battery"},
    )
    assert response.status_code == 201
    return response


@pytest.mark.asyncio
async def test_register_cookie_conversation_and_accepted_run(api_client):
    client = api_client
    response = await _register(client, "alice@example.com")
    cookie = response.headers["set-cookie"].lower()
    assert "httponly" in cookie and "samesite=lax" in cookie

    assert (await client.get("/v1/me")).status_code == 200
    created = await client.post("/v1/conversations", json={"title": "Research"})
    assert created.status_code == 201
    conversation_id = created.json()["id"]
    assert (await client.get(f"/v1/conversations/{conversation_id}")).status_code == 200
    accepted = await client.post(
        f"/v1/conversations/{conversation_id}/messages",
        json={"content": "hello"},
    )
    assert accepted.status_code == 202
    run = await client.get(f"/v1/runs/{accepted.json()['run_id']}")
    assert run.json()["status"] == "queued"
    cancelled = await client.post(f"/v1/runs/{accepted.json()['run_id']}/cancel")
    assert cancelled.json()["status"] == "cancelled"
    events = await client.get(f"/v1/runs/{accepted.json()['run_id']}/events")
    assert [event["event_type"] for event in events.json()] == [
        "run.queued",
        "run.cancelled",
    ]
    stream = await client.get(
        f"/v1/runs/{accepted.json()['run_id']}/events/stream"
    )
    assert stream.status_code == 200
    assert "event: run.queued" in stream.text
    assert "event: run.cancelled" in stream.text
    messages = await client.get(f"/v1/conversations/{conversation_id}/messages")
    assert [item["content"] for item in messages.json()] == ["hello"]


@pytest.mark.asyncio
async def test_configure_automation_and_ingest_event(api_client):
    await _register(api_client, "automation-api@example.com")
    created = await api_client.post("/v1/conversations", json={"title": "Active"})
    conversation_id = created.json()["id"]
    configured = await api_client.put(
        f"/v1/conversations/{conversation_id}/automation",
        json={
            "proactive_enabled": True,
            "drift_enabled": True,
            "proactive_context": "Only send actionable updates.",
        },
    )
    assert configured.status_code == 200
    assert configured.json()["enabled"] is True
    event = await api_client.post(
        f"/v1/conversations/{conversation_id}/proactive-events",
        json={
            "kind": "content",
            "source_id": "feed",
            "event_id": "entry-1",
            "payload": {"title": "new release", "url": "https://example.test"},
        },
    )
    assert event.status_code == 202 and event.json()["accepted"] is True
    duplicate = await api_client.post(
        f"/v1/conversations/{conversation_id}/proactive-events",
        json={
            "kind": "content",
            "source_id": "feed",
            "event_id": "entry-1",
            "payload": {"title": "duplicate"},
        },
    )
    assert duplicate.json() == {**event.json(), "accepted": False}


@pytest.mark.asyncio
async def test_message_admission_idempotency_does_not_duplicate_turn(api_client):
    client = api_client
    await _register(client, "idempotent@example.com")
    first_conversation = (await client.post("/v1/conversations", json={})).json()
    second_conversation = (await client.post("/v1/conversations", json={})).json()
    headers = {"Idempotency-Key": "browser-request-123"}
    first = await client.post(
        f"/v1/conversations/{first_conversation['id']}/messages",
        headers=headers,
        json={"content": "only once"},
    )
    replay = await client.post(
        f"/v1/conversations/{first_conversation['id']}/messages",
        headers=headers,
        json={"content": "only once"},
    )
    assert first.status_code == replay.status_code == 202
    assert first.json() == replay.json()
    messages = await client.get(
        f"/v1/conversations/{first_conversation['id']}/messages"
    )
    assert [item["content"] for item in messages.json()] == ["only once"]
    conflict = await client.post(
        f"/v1/conversations/{second_conversation['id']}/messages",
        headers=headers,
        json={"content": "wrong conversation"},
    )
    assert conflict.status_code == 409


@pytest.mark.asyncio
async def test_logout_revokes_server_side_session(api_client):
    client = api_client
    await _register(client, "logout@example.com")
    assert (await client.post("/v1/auth/logout")).status_code == 204
    assert (await client.get("/v1/me")).status_code == 401


@pytest.mark.asyncio
async def test_other_user_cannot_observe_conversation_or_run(api_client):
    alice = api_client
    await _register(alice, "alice2@example.com")
    conversation = (await alice.post("/v1/conversations", json={})).json()
    accepted = (
        await alice.post(
            f"/v1/conversations/{conversation['id']}/messages",
            json={"content": "private"},
        )
    ).json()

    transport = alice._transport
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as bob:
        await _register(bob, "bob@example.com")
        messages = await bob.get(f"/v1/conversations/{conversation['id']}/messages")
        run = await bob.get(f"/v1/runs/{accepted['run_id']}")
        events = await bob.get(f"/v1/runs/{accepted['run_id']}/events")
        assert messages.status_code == 404
        assert run.status_code == 404
        assert events.status_code == 404


@pytest.mark.asyncio
async def test_message_rate_limit_and_origin_policy_are_enforced():
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    app = create_app(
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
        settings=CloudSettings(
            "sqlite+aiosqlite://",
            session_cookie_secure=False,
            allowed_origins=("https://app.example.test",),
            message_rate_limit_per_minute=1,
            sse_poll_interval_seconds=0.01,
        ),
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await _register(client, "limited@example.com")
        conversation = (await client.post("/v1/conversations", json={})).json()
        path = f"/v1/conversations/{conversation['id']}/messages"
        assert (await client.post(path, json={"content": "first"})).status_code == 202
        rejected = await client.post(path, json={"content": "second"})
        assert rejected.status_code == 429
        assert int(rejected.headers["retry-after"]) > 0
        forbidden = await client.post(
            "/v1/conversations",
            headers={"Origin": "https://evil.example.test"},
            json={},
        )
        assert forbidden.status_code == 403
        assert forbidden.headers["x-content-type-options"] == "nosniff"
        allowed = await client.post(
            "/v1/conversations",
            headers={"Origin": "https://app.example.test"},
            json={},
        )
        assert allowed.status_code == 201
        assert allowed.headers["x-content-type-options"] == "nosniff"
        preflight = await client.options(
            "/v1/conversations",
            headers={
                "Origin": "https://app.example.test",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "content-type,idempotency-key",
            },
        )
        assert preflight.status_code == 200
        assert preflight.headers["access-control-allow-origin"] == (
            "https://app.example.test"
        )
    await engine.dispose()


@pytest.mark.asyncio
async def test_message_pagination_and_deletion(api_client):
    await _register(api_client, "delete@example.com")
    conversation = (await api_client.post("/v1/conversations", json={})).json()
    conversation_id = conversation["id"]
    for index in range(3):
        response = await api_client.post(
            f"/v1/conversations/{conversation_id}/messages",
            json={"content": f"message-{index}"},
        )
        assert response.status_code == 202
    page = await api_client.get(
        f"/v1/conversations/{conversation_id}/messages?limit=2"
    )
    assert [item["seq"] for item in page.json()] == [2, 3]
    older = await api_client.get(
        f"/v1/conversations/{conversation_id}/messages?before_seq=2&limit=2"
    )
    assert [item["seq"] for item in older.json()] == [1]
    deleted = await api_client.delete(f"/v1/conversations/{conversation_id}")
    assert deleted.status_code == 204
    assert (
        await api_client.get(f"/v1/conversations/{conversation_id}")
    ).status_code == 404
    account = await api_client.delete("/v1/me")
    assert account.status_code == 204
    assert (await api_client.get("/v1/me")).status_code == 401
