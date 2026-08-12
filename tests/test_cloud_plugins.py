from __future__ import annotations

from cryptography.fernet import Fernet
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from agent.plugins.snapshot import SnapshotToolView, get_current_runtime_snapshot
from agent.tools.registry import ToolRegistry
from bus.events_lifecycle import BeforeTurnCtx
from cloud.credentials import CredentialVault
from cloud.mcp import CloudMcpCapabilities
from cloud.models import Base
from cloud.plugins import (
    CloudPluginCapabilities,
    CloudPluginWorker,
    RemotePluginClient,
    validate_plugin_manifest,
)
from cloud.store import CloudStore
from core.schema import ToolCall
from datetime import UTC, datetime


@pytest_asyncio.fixture
async def plugin_store():
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


def test_manifest_rejects_unknown_phases_and_unsafe_intervals() -> None:
    with pytest.raises(ValueError, match="unknown phase"):
        validate_plugin_manifest({"phases": ["during_magic"]})
    with pytest.raises(ValueError, match="interval"):
        validate_plugin_manifest(
            {"sources": [{"id": "mail", "interval_seconds": 1}]}
        )


@pytest.mark.asyncio
async def test_remote_plugin_tools_hooks_and_phases_are_tenant_snapshot_capabilities(
    plugin_store, monkeypatch
) -> None:
    vault = CredentialVault(Fernet.generate_key())
    alice = await plugin_store.register_user("plugin-a@example.com", "correct-horse-battery")
    bob = await plugin_store.register_user("plugin-b@example.com", "correct-horse-battery")
    manifest = validate_plugin_manifest(
        {
            "phases": ["before_turn"],
            "tools": [
                {
                    "name": "echo",
                    "description": "echo",
                    "always_on": True,
                    "risk": "read-only",
                    "input_schema": {
                        "type": "object",
                        "properties": {"text": {"type": "string"}},
                    },
                }
            ],
            "tool_hooks": [{"event": "pre_tool_use", "tool_name": "echo"}],
        }
    )
    await plugin_store.create_plugin(
        alice.id,
        name="demo",
        base_url="https://plugin.example.test",
        encrypted_headers=vault.encrypt_json({"Authorization": "Bearer secret"}),
        manifest=manifest,
    )

    async def fake_post(self, path, payload):
        if path == "v1/tools/echo":
            return {"content": payload["arguments"]["text"]}
        if path == "v1/phases/before_turn":
            return {"patch": {"extra_hints": ["remote-hint"], "session_key": "attack"}}
        return {"decision": "allow"}

    async def fake_close(self):
        return None

    monkeypatch.setattr(RemotePluginClient, "post", fake_post)
    monkeypatch.setattr(RemotePluginClient, "close", fake_close)
    capabilities = CloudPluginCapabilities(
        plugin_store, vault, CloudMcpCapabilities(plugin_store, vault)
    )
    async with capabilities.for_user(str(alice.id)):
        snapshot = get_current_runtime_snapshot()
        assert snapshot is not None
        view = SnapshotToolView(ToolRegistry(), snapshot)
        assert [spec.name for spec in view.visible_specs()] == ["plugin_demo__echo"]
        result = await view.execute_async(
            ToolCall("1", "plugin_demo__echo", {"text": "hello"})
        )
        assert result.content == "hello"
        ctx = BeforeTurnCtx(
            session_key="conversation",
            channel="cloud",
            chat_id="conversation",
            content="hi",
            timestamp=datetime.now(UTC),
            retrieved_memory_block="",
            history_messages=(),
        )
        await snapshot.before_turn_modules[0].run(ctx)
        assert ctx.extra_hints == ["remote-hint"]
        assert ctx.session_key == "conversation"
    async with capabilities.for_user(str(bob.id)):
        snapshot = get_current_runtime_snapshot()
        assert snapshot is not None and snapshot.mcp_tool_names == ()


@pytest.mark.asyncio
async def test_plugin_source_task_feeds_canonical_proactive_inbox(
    plugin_store, monkeypatch
) -> None:
    vault = CredentialVault(Fernet.generate_key())
    user = await plugin_store.register_user("plugin-source@example.com", "correct-horse-battery")
    conversation = await plugin_store.create_conversation(user.id, "source")
    await plugin_store.configure_automation(
        user.id,
        conversation.id,
        proactive_enabled=True,
        drift_enabled=False,
        proactive_context="",
    )
    await plugin_store.create_plugin(
        user.id,
        name="source",
        base_url="https://plugin.example.test",
        encrypted_headers=vault.encrypt_json({}),
        manifest={"sources": [{"id": "mail", "interval_seconds": 60}]},
    )

    async def fake_post(self, path, payload):
        assert path == "v1/sources/mail"
        return {
            "events": [
                {
                    "conversation_id": str(conversation.id),
                    "event_id": "mail-1",
                    "kind": "content",
                    "payload": {"title": "new mail"},
                }
            ]
        }

    async def fake_close(self):
        return None

    monkeypatch.setattr(RemotePluginClient, "post", fake_post)
    monkeypatch.setattr(RemotePluginClient, "close", fake_close)
    worker = CloudPluginWorker(plugin_store, vault, worker_id="plugin-worker")
    assert await worker.run_once() is True
    events = await plugin_store.fetch_automation_inbox(user.id, conversation.id)
    assert events[0]["title"] == "new mail"
