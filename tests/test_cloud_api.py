"""HTTP contracts for the first Kirakira Cloud API slice."""

from __future__ import annotations

import httpx
import pytest
import pytest_asyncio
from cryptography.fernet import Fernet
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from cloud.api import create_app
from cloud.database import CloudSettings
from cloud.models import Base
from agent.tools.execution_backend import ExecutionBackendDescriptor
from agent.tools.unified_exec import ExecutionCleanupReport


class _WorkspaceBackend:
    descriptor = ExecutionBackendDescriptor(
        "test-isolated", isolated=True, host_execution=False, workspace_isolated=True
    )

    def __init__(self):
        self.files = {}

    async def probe(self):
        return self.descriptor

    async def write_binary(self, owner, path, content):
        self.files[(owner, path)] = content
        return f"Wrote {path}"

    async def read_binary(self, owner, path):
        return self.files[(owner, path)]

    async def shutdown(self):
        return ExecutionCleanupReport((), (), ())


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
    backend = _WorkspaceBackend()
    app = create_app(
        session_factory=factory,
        settings=CloudSettings("sqlite+aiosqlite://", session_cookie_secure=False),
        workspace_backend=backend,  # type: ignore[arg-type]
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
async def test_upload_file_attach_to_run_and_download_with_tenant_boundary(api_client):
    await _register(api_client, "files@example.com")
    conversation = (await api_client.post("/v1/conversations", json={})).json()
    uploaded = await api_client.post(
        f"/v1/conversations/{conversation['id']}/files",
        json={
            "filename": "pixel.png",
            "content_type": "image/png",
            "content_base64": "iVBORw0KGgo=",
        },
    )
    assert uploaded.status_code == 201
    item = uploaded.json()
    accepted = await api_client.post(
        f"/v1/conversations/{conversation['id']}/messages",
        json={"content": "inspect this", "file_ids": [item["id"]]},
    )
    assert accepted.status_code == 202
    messages = await api_client.get(
        f"/v1/conversations/{conversation['id']}/messages"
    )
    attachment = messages.json()[0]["agent_metadata"]["attachments"][0]
    assert attachment["filename"] == "pixel.png"
    downloaded = await api_client.get(f"/v1/files/{item['id']}")
    assert downloaded.content == __import__("base64").b64decode("iVBORw0KGgo=")

    transport = api_client._transport
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as bob:
        await _register(bob, "files-bob@example.com")
        assert (await bob.get(f"/v1/files/{item['id']}")).status_code == 404


@pytest.mark.asyncio
async def test_cloud_ui_is_served_with_locked_down_csp(api_client):
    response = await api_client.get("/")
    assert response.status_code == 200
    assert "Kirakira Cloud Agent" in response.text
    assert "script-src 'self'" in response.headers["content-security-policy"]
    assert (await api_client.get("/settings.css")).status_code == 200


@pytest.mark.asyncio
async def test_user_skill_crud_is_tenant_scoped(api_client):
    await _register(api_client, "skills-api@example.com")
    created = await api_client.post(
        "/v1/skills",
        json={
            "content": "---\nname: private-skill\ndescription: Private\nalways: true\n---\nUse this workflow."
        },
    )
    assert created.status_code == 201 and created.json()["always"] is True
    assert [item["name"] for item in (await api_client.get("/v1/skills")).json()] == [
        "private-skill"
    ]
    transport = api_client._transport
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as bob:
        await _register(bob, "skills-api-bob@example.com")
        assert (await bob.get("/v1/skills")).json() == []
        assert (
            await bob.delete(f"/v1/skills/{created.json()['id']}")
        ).status_code == 404


@pytest.mark.asyncio
async def test_remote_plugin_registration_validates_manifest_and_hides_headers(
    api_client, monkeypatch
):
    import cloud.api as api_module

    await _register(api_client, "plugin-api@example.com")
    monkeypatch.setenv("KIRAKIRA_CREDENTIAL_KEY", Fernet.generate_key().decode())
    monkeypatch.setattr(api_module, "validate_remote_plugin_url", lambda value: value)

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "phases": ["before_turn"],
                "tools": [
                    {
                        "name": "echo",
                        "input_schema": {"type": "object", "properties": {}},
                    }
                ],
            }

    class IntegrationClient:
        def __init__(self, **kwargs):
            assert kwargs["headers"]["Authorization"] == "Bearer hidden"

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, path):
            assert path == "v1/manifest"
            return Response()

    monkeypatch.setattr(api_module.httpx, "AsyncClient", IntegrationClient)
    created = await api_client.post(
        "/v1/plugins",
        json={
            "name": "demo",
            "base_url": "https://plugin.example.test",
            "headers": {"Authorization": "Bearer hidden"},
        },
    )
    assert created.status_code == 201
    assert "headers" not in created.json() and "encrypted_headers" not in created.json()


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
