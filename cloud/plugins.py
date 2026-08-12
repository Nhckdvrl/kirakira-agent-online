"""Isolated remote-plugin protocol for tenant-scoped Cloud capabilities."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
import json
import logging
import re
from typing import Any, AsyncIterator
from uuid import UUID

import httpx

from agent.plugins.snapshot import (
    RuntimeSnapshotStore,
    bind_runtime_snapshot,
    compile_snapshot,
    reset_runtime_snapshot,
)
from agent.tool_hooks import HookContext, HookOutcome
from agent.tools.registry import Tool, ToolMeta
from agent.skills import bind_cloud_skills
from cloud.credentials import CredentialVault
from cloud.logging import safe_exception_summary
from cloud.mcp import CloudMcpCapabilities, RemoteMcpClient, validate_remote_mcp_url
from cloud.store import CloudStore
from cloud.skills import skill_overlay
from core.schema import ToolSpec


logger = logging.getLogger("kirakira.cloud.plugins")
PHASES = {
    "before_turn",
    "before_reasoning",
    "prompt_render",
    "before_step",
    "after_step",
    "after_reasoning",
    "after_turn",
}
HOOK_EVENTS = {"pre_tool_use", "post_tool_use", "post_tool_error"}


def validate_remote_plugin_url(url: str) -> str:
    try:
        return validate_remote_mcp_url(url)
    except ValueError as exc:
        raise ValueError(str(exc).replace("MCP", "plugin")) from exc


def validate_plugin_manifest(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("plugin manifest must be an object")
    manifest = dict(value)
    phases = manifest.get("phases", [])
    tools = manifest.get("tools", [])
    hooks = manifest.get("tool_hooks", [])
    if not isinstance(phases, list) or len(phases) > 20:
        raise ValueError("plugin phases must be a list with at most 20 entries")
    for item in phases:
        phase = str(item.get("name") or "") if isinstance(item, dict) else str(item)
        if phase not in PHASES:
            raise ValueError("plugin manifest contains an unknown phase")
        if isinstance(item, dict):
            slot = str(item.get("slot") or "")
            requires = item.get("requires", [])
            if slot and not re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}", slot):
                raise ValueError("plugin phase slot is invalid")
            if not isinstance(requires, list) or len(requires) > 20:
                raise ValueError("plugin phase requires must be a list")
    if not isinstance(tools, list) or len(tools) > 100:
        raise ValueError("plugin tools must be a list with at most 100 entries")
    seen: set[str] = set()
    for spec in tools:
        if not isinstance(spec, dict) or not re.fullmatch(
            r"[A-Za-z0-9_-]{1,100}", str(spec.get("name") or "")
        ):
            raise ValueError("plugin tool name is invalid")
        if spec["name"] in seen:
            raise ValueError("plugin tool names must be unique")
        seen.add(spec["name"])
        schema = spec.get("input_schema", {"type": "object", "properties": {}})
        if not isinstance(schema, dict) or schema.get("type") != "object":
            raise ValueError("plugin tool input_schema must be an object schema")
        if spec.get("risk", "external-side-effect") not in {
            "read-only", "write", "external-side-effect"
        }:
            raise ValueError("plugin tool risk is invalid")
    if not isinstance(hooks, list) or len(hooks) > 100:
        raise ValueError("plugin tool_hooks must be a list with at most 100 entries")
    for hook in hooks:
        if not isinstance(hook, dict) or hook.get("event") not in HOOK_EVENTS:
            raise ValueError("plugin tool hook event is invalid")
    for key in ("jobs", "sources"):
        items = manifest.get(key, [])
        if not isinstance(items, list) or len(items) > 100:
            raise ValueError(f"plugin {key} must be a list with at most 100 entries")
        ids: set[str] = set()
        for item in items:
            if not isinstance(item, dict) or not re.fullmatch(
                r"[A-Za-z0-9_.-]{1,200}", str(item.get("id") or "")
            ):
                raise ValueError(f"plugin {key} id is invalid")
            interval = int(item.get("interval_seconds") or 0)
            if interval < 10 or interval > 2_592_000 or item["id"] in ids:
                raise ValueError(f"plugin {key} interval or duplicate id is invalid")
            ids.add(item["id"])
    mcp_servers = manifest.get("mcp_servers", [])
    if not isinstance(mcp_servers, list) or len(mcp_servers) > 20:
        raise ValueError("plugin mcp_servers must be a list with at most 20 entries")
    for item in mcp_servers:
        if not isinstance(item, dict) or not re.fullmatch(
            r"[A-Za-z0-9_-]{1,100}", str(item.get("name") or "")
        ):
            raise ValueError("plugin MCP server name is invalid")
        validate_remote_mcp_url(str(item.get("base_url") or ""))
    encoded = json.dumps(manifest, ensure_ascii=False)
    if len(encoded) > 1_000_000:
        raise ValueError("plugin manifest exceeds 1 MB")
    return manifest


class RemotePluginClient:
    def __init__(self, name: str, base_url: str, headers: dict[str, str]) -> None:
        self.name = name
        self.client = httpx.AsyncClient(
            base_url=base_url.rstrip("/") + "/",
            headers=headers,
            timeout=30,
            follow_redirects=False,
        )

    async def post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = await self.client.post(path.lstrip("/"), json=payload)
        response.raise_for_status()
        result = response.json()
        if not isinstance(result, dict):
            raise RuntimeError(f"plugin {self.name} returned a non-object response")
        return result

    async def close(self) -> None:
        await self.client.aclose()


class RemotePluginPhase:
    def __init__(
        self, client: RemotePluginClient, phase: str, spec: dict[str, Any] | None = None
    ) -> None:
        self.client = client
        self.phase = phase
        declaration = spec or {}
        self.priority = int(declaration.get("priority") or 0)
        self.slot = str(declaration.get("slot") or "")
        self.requires = tuple(str(item) for item in declaration.get("requires", []))

    async def run(self, ctx: Any) -> Any:
        payload = _jsonable(asdict(ctx) if is_dataclass(ctx) else ctx)
        result = await self.client.post(f"v1/phases/{self.phase}", {"context": payload})
        patch = result.get("patch", {})
        if not isinstance(patch, dict):
            raise RuntimeError("plugin phase patch must be an object")
        if bool(getattr(getattr(ctx, "__dataclass_params__", None), "frozen", False)):
            return ctx
        protected = {"session_key", "channel", "chat_id", "timestamp"}
        for key, value in patch.items():
            if key in protected or not hasattr(ctx, key):
                continue
            current = getattr(ctx, key)
            if isinstance(current, tuple):
                value = tuple(value)
            elif isinstance(current, set):
                value = set(value)
            setattr(ctx, key, value)
        return ctx


class RemotePluginToolHook:
    def __init__(self, client: RemotePluginClient, spec: dict[str, Any]) -> None:
        self.client = client
        self.event = str(spec["event"])
        self.tool_name = str(spec.get("tool_name") or "")
        self.name = f"remote:{client.name}:{self.event}:{self.tool_name or '*'}"

    def matches(self, ctx: HookContext) -> bool:
        return not self.tool_name or ctx.request.tool_name == self.tool_name

    async def run(self, ctx: HookContext) -> HookOutcome:
        result = await self.client.post(
            f"v1/tool-hooks/{self.event}", {"context": _jsonable(asdict(ctx))}
        )
        allowed = {
            "decision", "updated_input", "reason", "extra_message",
            "replay_status", "replay_output", "replay_mobile_attention",
        }
        return HookOutcome(**{key: value for key, value in result.items() if key in allowed})


class CloudPluginCapabilities:
    def __init__(
        self, store: CloudStore, vault: CredentialVault, mcp: CloudMcpCapabilities
    ) -> None:
        self.store = store
        self.vault = vault
        self.mcp = mcp

    @asynccontextmanager
    async def for_user(self, user_id: str) -> AsyncIterator[None]:
        declarations = await self.store.list_plugins(UUID(user_id), enabled_only=True)
        skills = await self.store.list_skills(UUID(user_id), enabled_only=True)
        skill_binding = bind_cloud_skills(skill_overlay(skills))
        skill_binding.__enter__()
        clients: list[RemotePluginClient] = []
        plugin_mcp_clients: list[RemoteMcpClient] = []
        phases: dict[str, list[object]] = {}
        hooks: list[object] = []
        plugin_tools: dict[str, Tool] = {}
        try:
            for declaration in declarations:
                manifest = validate_plugin_manifest(declaration.manifest)
                client = RemotePluginClient(
                    declaration.name,
                    declaration.base_url,
                    self.vault.decrypt_json(declaration.encrypted_headers),
                )
                clients.append(client)
                for phase_spec in manifest.get("phases", []):
                    declaration_spec = phase_spec if isinstance(phase_spec, dict) else {}
                    phase = str(declaration_spec.get("name") or phase_spec)
                    phases.setdefault(f"{phase}_modules", []).append(
                        RemotePluginPhase(client, phase, declaration_spec)
                    )
                hooks.extend(
                    RemotePluginToolHook(client, spec)
                    for spec in manifest.get("tool_hooks", [])
                )
                for spec in manifest.get("tools", []):
                    name = "plugin_%s__%s" % (_safe(declaration.name), _safe(spec["name"]))
                    if name in plugin_tools:
                        raise RuntimeError(f"duplicate plugin tool name: {name}")

                    async def invoke(_client=client, _name=spec["name"], **kwargs: Any) -> str:
                        result = await _client.post(f"v1/tools/{_name}", {"arguments": kwargs})
                        return str(result.get("content") or "")

                    plugin_tools[name] = Tool(
                        ToolSpec(
                            name,
                            f"[Plugin:{declaration.name}] {spec.get('description') or spec['name']}",
                            dict(spec.get("input_schema") or {"type": "object", "properties": {}}),
                        ),
                        invoke,
                        deferred=not bool(spec.get("always_on", False)),
                        meta=ToolMeta(
                            risk=str(spec.get("risk") or "external-side-effect"),
                            always_on=bool(spec.get("always_on", False)),
                            source_type="plugin",
                            source_name=declaration.name,
                        ),
                    )
                plugin_headers = self.vault.decrypt_json(declaration.encrypted_headers)
                for server in manifest.get("mcp_servers", []):
                    mcp_client = RemoteMcpClient(
                        f"{declaration.name}:{server['name']}",
                        validate_remote_mcp_url(str(server["base_url"])),
                        plugin_headers,
                    )
                    plugin_mcp_clients.append(mcp_client)
                    for info in await mcp_client.connect():
                        name = "plugin_%s__mcp_%s__%s" % (
                            _safe(declaration.name),
                            _safe(server["name"]),
                            _safe(info.name),
                        )
                        if name in plugin_tools:
                            raise RuntimeError(f"duplicate plugin MCP tool name: {name}")

                        async def invoke_mcp(
                            _client=mcp_client, _name=info.name, **kwargs: Any
                        ) -> str:
                            return await _client.call(_name, kwargs)

                        plugin_tools[name] = Tool(
                            ToolSpec(
                                name,
                                f"[Plugin MCP:{declaration.name}/{server['name']}] {info.description}",
                                info.input_schema,
                            ),
                            invoke_mcp,
                            deferred=True,
                            meta=ToolMeta(
                                risk="external-side-effect",
                                source_type="plugin",
                                source_name=declaration.name,
                            ),
                        )
            from agent.plugins.manager import PluginManager

            for field, modules in phases.items():
                modules.sort(key=lambda item: -int(getattr(item, "priority", 0)))
                phases[field] = PluginManager._order_phase_modules(
                    field.removesuffix("_modules"), modules
                )
            async with self.mcp.tool_scope(user_id) as mcp_tools:
                overlap = set(plugin_tools).intersection(mcp_tools)
                if overlap:
                    raise RuntimeError(f"duplicate remote capability: {sorted(overlap)[0]}")
                snapshot_store = RuntimeSnapshotStore()
                snapshot = compile_snapshot(
                    phase_modules=phases,
                    tool_hooks=hooks,
                    mcp_tools={**mcp_tools, **plugin_tools},
                    mcp_generation_id=f"user:{user_id}",
                    revision="|".join(str(item.updated_at) for item in declarations),
                )
                await snapshot_store.commit(snapshot_store.publish(snapshot))
                lease = snapshot_store.lease()
                token = bind_runtime_snapshot(lease)
                try:
                    yield
                finally:
                    reset_runtime_snapshot(token)
                    await lease.release()
                    await snapshot_store.close()
        finally:
            await asyncio.gather(*(client.close() for client in clients), return_exceptions=True)
            await asyncio.gather(
                *(client.close() for client in plugin_mcp_clients),
                return_exceptions=True,
            )
            skill_binding.__exit__(None, None, None)


class CloudPluginWorker:
    def __init__(
        self,
        store: CloudStore,
        vault: CredentialVault,
        *,
        worker_id: str,
        model_client: Any | None = None,
        model: str = "",
    ) -> None:
        self.store = store
        self.vault = vault
        self.worker_id = worker_id[:200]
        self.model_client = model_client
        self.model = model
        self._stop = asyncio.Event()

    async def run_once(self) -> bool:
        claimed = await self.store.claim_next_plugin_task(self.worker_id)
        if claimed is None:
            return False
        task, plugin = claimed
        error = ""
        client = RemotePluginClient(
            plugin.name, plugin.base_url, self.vault.decrypt_json(plugin.encrypted_headers)
        )
        try:
            result = await client.post(
                f"v1/{task.kind}s/{task.task_id}",
                {"triggered_at": datetime.now(UTC).isoformat()},
            )
            requests = result.get("llm_requests", [])
            if requests:
                if self.model_client is None or not isinstance(requests, list) or len(requests) > 5:
                    raise RuntimeError("plugin job returned unsupported LLM requests")
                answers = []
                for request in requests:
                    if not isinstance(request, dict):
                        raise RuntimeError("plugin LLM request must be an object")
                    prompt = str(request.get("prompt") or "")[:100_000]
                    if not prompt:
                        raise RuntimeError("plugin LLM request prompt is empty")
                    kwargs = dict(
                        messages=[{"role": "user", "content": prompt}],
                        tools=[],
                        system=str(request.get("system") or "")[:20_000],
                        model=self.model,
                        max_tokens=max(1, min(8192, int(request.get("max_tokens") or 2048))),
                        tool_choice="none",
                    )
                    complete = getattr(self.model_client, "acomplete", None)
                    response = (
                        await complete(**kwargs)
                        if callable(complete)
                        else await asyncio.to_thread(self.model_client.complete, **kwargs)
                    )
                    answers.append(
                        {
                            "id": str(request.get("id") or len(answers)),
                            "content": response.text,
                            "usage": dict(response.usage or {}),
                        }
                    )
                result = await client.post(
                    f"v1/{task.kind}s/{task.task_id}/complete",
                    {"llm_results": answers},
                )
            for event in result.get("events", []):
                if not isinstance(event, dict):
                    continue
                await self.store.ingest_proactive_event(
                    task.user_id,
                    UUID(str(event["conversation_id"])),
                    kind=str(event.get("kind") or "content"),
                    source_id=f"plugin:{plugin.name}:{task.task_id}",
                    event_id=str(event["event_id"]),
                    payload=dict(event.get("payload") or {}),
                )
        except Exception as exc:  # durable retry records and isolates failures
            error = safe_exception_summary(exc)
            logger.warning("plugin task failed: %s/%s: %s", plugin.name, task.task_id, error)
        finally:
            await client.close()
            await self.store.finish_plugin_task(task.id, self.worker_id, error=error)
        return True

    async def run_forever(self) -> None:
        while not self._stop.is_set():
            if not await self.run_once():
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=2)
                except TimeoutError:
                    pass

    def stop(self) -> None:
        self._stop.set()


def _jsonable(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def _safe(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "_", value)
