from __future__ import annotations

import json

from cryptography.fernet import Fernet
import httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from cloud.credentials import CredentialVault
from cloud.mcp import CloudMcpCapabilities, RemoteMcpClient
from cloud.models import Base
from cloud.store import CloudStore
from agent.plugins.snapshot import SnapshotToolView, get_current_runtime_snapshot
from agent.tools.registry import ToolRegistry
from core.schema import ToolCall


@pytest_asyncio.fixture
async def mcp_store():
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


@pytest.mark.asyncio
async def test_remote_mcp_http_handshake_catalog_and_call() -> None:
    seen = []

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer secret"
        payload = json.loads(request.content)
        seen.append(payload["method"])
        if payload["method"] == "initialize":
            return httpx.Response(
                200,
                headers={"Mcp-Session-Id": "session-1"},
                json={"jsonrpc": "2.0", "id": payload["id"], "result": {}},
            )
        if payload["method"] == "notifications/initialized":
            return httpx.Response(202)
        if payload["method"] == "tools/list":
            assert request.headers["mcp-session-id"] == "session-1"
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": {
                        "tools": [
                            {
                                "name": "echo",
                                "description": "Echo text",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {"text": {"type": "string"}},
                                },
                            }
                        ]
                    },
                },
            )
        if payload["method"] == "tools/call":
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": {"content": [{"type": "text", "text": payload["params"]["arguments"]["text"]}]},
                },
            )
        raise AssertionError(payload)

    client = RemoteMcpClient(
        "demo", "https://mcp.example.test/service", {"Authorization": "Bearer secret"}
    )
    await client.client.aclose()
    client.client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://mcp.example.test/service",
        headers={"Authorization": "Bearer secret"},
    )
    try:
        tools = await client.connect()
        assert [item.name for item in tools] == ["echo"]
        assert await client.call("echo", {"text": "hello"}) == "hello"
        assert seen == ["initialize", "notifications/initialized", "tools/list", "tools/call"]
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_user_mcp_credentials_are_encrypted_and_snapshot_is_tenant_scoped(
    mcp_store, monkeypatch
) -> None:
    key = Fernet.generate_key()
    vault = CredentialVault(key)
    alice = await mcp_store.register_user("mcp-a@example.com", "correct-horse-battery")
    bob = await mcp_store.register_user("mcp-b@example.com", "correct-horse-battery")
    encrypted = vault.encrypt_json({"Authorization": "Bearer top-secret"})
    assert "top-secret" not in encrypted
    await mcp_store.create_mcp_server(
        alice.id,
        name="private",
        base_url="https://mcp.example.test",
        encrypted_headers=encrypted,
    )

    async def fake_connect(self):
        self.tool_infos = [
            __import__("agent.mcp.client", fromlist=["McpToolInfo"]).McpToolInfo(
                "echo", "Echo", {"type": "object", "properties": {}}
            )
        ]
        return self.tool_infos

    async def fake_call(self, name, arguments):
        return f"{self.name}:{name}:{arguments.get('text')}"

    monkeypatch.setattr(RemoteMcpClient, "connect", fake_connect)
    monkeypatch.setattr(RemoteMcpClient, "call", fake_call)
    capabilities = CloudMcpCapabilities(mcp_store, vault)
    base = ToolRegistry()
    async with capabilities.for_user(str(alice.id)):
        snapshot = get_current_runtime_snapshot()
        assert snapshot is not None
        view = SnapshotToolView(base, snapshot)
        assert view.has("mcp_private__echo")
        result = await view.execute_async(
            ToolCall("1", "mcp_private__echo", {"text": "alice"})
        )
        assert result.content == "private:echo:alice"
    async with capabilities.for_user(str(bob.id)):
        snapshot = get_current_runtime_snapshot()
        assert snapshot is not None and snapshot.mcp_tool_names == ()
