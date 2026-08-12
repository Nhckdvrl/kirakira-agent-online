"""Tenant-scoped remote MCP transport and per-turn capability snapshots."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import ipaddress
import json
import os
import re
import socket
from typing import Any, AsyncIterator
from urllib.parse import urlparse
from uuid import UUID

import httpx

from agent.mcp.client import McpToolInfo
from agent.plugins.snapshot import (
    RuntimeSnapshotStore,
    bind_runtime_snapshot,
    compile_snapshot,
    reset_runtime_snapshot,
)
from agent.tools.registry import Tool, ToolMeta
from cloud.credentials import CredentialVault
from cloud.store import CloudStore
from core.schema import ToolSpec


def validate_remote_mcp_url(url: str) -> str:
    parsed = urlparse(url.strip())
    if parsed.scheme != "https" or not parsed.hostname or parsed.username:
        raise ValueError("remote MCP URL must be an https URL without userinfo")
    allowlist = {
        item.strip().lower()
        for item in os.getenv("KIRAKIRA_MCP_DOMAIN_ALLOWLIST", "").split(",")
        if item.strip()
    }
    host = parsed.hostname.lower()
    if allowlist and not any(host == item or host.endswith("." + item) for item in allowlist):
        raise ValueError("remote MCP host is not in KIRAKIRA_MCP_DOMAIN_ALLOWLIST")
    try:
        addresses = {item[4][0] for item in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)}
    except socket.gaierror as exc:
        raise ValueError("remote MCP host cannot be resolved") from exc
    if any(
        ipaddress.ip_address(address).is_private
        or ipaddress.ip_address(address).is_loopback
        or ipaddress.ip_address(address).is_link_local
        or ipaddress.ip_address(address).is_reserved
        for address in addresses
    ):
        raise ValueError("remote MCP host resolves to a private or reserved address")
    return url.strip()


class RemoteMcpClient:
    def __init__(self, name: str, base_url: str, headers: dict[str, str]) -> None:
        self.name = name
        self.client = httpx.AsyncClient(
            base_url=base_url,
            headers={
                "Accept": "application/json, text/event-stream",
                "Content-Type": "application/json",
                **headers,
            },
            timeout=30,
            follow_redirects=False,
        )
        self._next_id = 1
        self._session_id = ""
        self.tool_infos: list[McpToolInfo] = []

    async def connect(self) -> list[McpToolInfo]:
        await self._request(
            "initialize",
            {
                "protocolVersion": "2025-03-26",
                "capabilities": {"tools": {}},
                "clientInfo": {"name": "kirakira-cloud", "version": "0.1.0"},
            },
        )
        await self._notify("notifications/initialized", {})
        payload = await self._request("tools/list", {})
        raw = payload.get("result", {}).get("tools", [])
        if not isinstance(raw, list):
            raise RuntimeError(f"MCP {self.name} returned an invalid tool list")
        self.tool_infos = [
            McpToolInfo(
                name=str(item.get("name") or ""),
                description=str(item.get("description") or ""),
                input_schema=dict(item.get("inputSchema") or {"type": "object"}),
            )
            for item in raw
            if isinstance(item, dict) and item.get("name")
        ]
        return list(self.tool_infos)

    async def call(self, name: str, arguments: dict[str, Any]) -> str:
        payload = await self._request(
            "tools/call", {"name": name, "arguments": dict(arguments)}
        )
        if "error" in payload:
            return f"Error: MCP {self.name}/{name}: {payload['error']}"
        result = payload.get("result")
        if not isinstance(result, dict) or not isinstance(result.get("content", []), list):
            return f"Error: MCP {self.name}/{name} returned an invalid result"
        parts = []
        for item in result.get("content", []):
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(str(item.get("text") or ""))
            elif isinstance(item, dict):
                parts.append(json.dumps(item, ensure_ascii=False))
        text = "\n".join(parts)
        return f"Error: MCP {self.name}/{name}: {text}" if result.get("isError") else text

    async def _request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        request_id = self._next_id
        self._next_id += 1
        headers = {"Mcp-Session-Id": self._session_id} if self._session_id else {}
        response = await self.client.post(
            "", headers=headers, json={"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        )
        response.raise_for_status()
        self._session_id = response.headers.get("Mcp-Session-Id", self._session_id)
        payload = _mcp_payload(response)
        if payload.get("id") != request_id:
            raise RuntimeError(f"MCP {self.name} response id mismatch")
        return payload

    async def _notify(self, method: str, params: dict[str, Any]) -> None:
        headers = {"Mcp-Session-Id": self._session_id} if self._session_id else {}
        response = await self.client.post(
            "", headers=headers, json={"jsonrpc": "2.0", "method": method, "params": params}
        )
        response.raise_for_status()

    async def close(self) -> None:
        await self.client.aclose()


def _mcp_payload(response: httpx.Response) -> dict[str, Any]:
    if "text/event-stream" not in response.headers.get("content-type", ""):
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("MCP response must be an object")
        return payload
    for line in response.text.splitlines():
        if line.startswith("data:"):
            payload = json.loads(line[5:].strip())
            if isinstance(payload, dict):
                return payload
    raise RuntimeError("MCP event stream contained no JSON-RPC response")


class CloudMcpCapabilities:
    def __init__(self, store: CloudStore, vault: CredentialVault) -> None:
        self.store = store
        self.vault = vault

    @asynccontextmanager
    async def tool_scope(self, user_id: str) -> AsyncIterator[dict[str, Tool]]:
        declarations = await self.store.list_mcp_servers(UUID(user_id), enabled_only=True)
        clients: list[RemoteMcpClient] = []
        tools: dict[str, Tool] = {}
        try:
            for declaration in declarations:
                client = RemoteMcpClient(
                    declaration.name,
                    declaration.base_url,
                    self.vault.decrypt_json(declaration.encrypted_headers),
                )
                clients.append(client)
                infos = await client.connect()
                for info in infos:
                    name = "mcp_%s__%s" % (_safe(declaration.name), _safe(info.name))
                    if name in tools:
                        raise RuntimeError(f"duplicate MCP tool name: {name}")

                    async def invoke(_client=client, _name=info.name, **kwargs: Any) -> str:
                        return await _client.call(_name, kwargs)

                    tools[name] = Tool(
                        ToolSpec(name, f"[MCP:{declaration.name}] {info.description}", info.input_schema),
                        invoke,
                        deferred=True,
                        meta=ToolMeta(
                            risk="external-side-effect",
                            always_on=False,
                            source_type="mcp",
                            source_name=declaration.name,
                        ),
                    )
            yield tools
        finally:
            await asyncio.gather(*(client.close() for client in clients), return_exceptions=True)

    @asynccontextmanager
    async def for_user(self, user_id: str) -> AsyncIterator[None]:
        async with self.tool_scope(user_id) as tools:
            snapshot_store = RuntimeSnapshotStore()
            snapshot = compile_snapshot(
                mcp_tools=tools,
                mcp_generation_id=f"user:{user_id}",
                revision="mcp",
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


def _safe(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "_", value)
