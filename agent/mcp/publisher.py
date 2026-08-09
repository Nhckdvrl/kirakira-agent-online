"""Atomic MCP generation swaps behind runtime snapshots.

MCP 工具不进共享注册表，而是挂在快照上：换代时新 turn 立刻看到新工具，在途 turn 继续
用旧快照里的旧连接；旧进程只有等最后一个租约释放后才断开。
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Dict, Mapping, Tuple
from uuid import uuid4

from agent.mcp.host import McpGenerationHost, PreparedMcpCatalog
from core.schema import ToolSpec
from agent.plugins.snapshot import (
    RuntimeSnapshot,
    RuntimeSnapshotStore,
    derive_snapshot,
)
from agent.tools.registry import Tool, ToolMeta

logger = logging.getLogger(__name__)


def _sanitize(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "_", value)


class McpCatalogPublisher:
    """按代际把 MCP catalog 编译进快照并原子发布。"""

    def __init__(
        self,
        store: RuntimeSnapshotStore,
        *,
        host: McpGenerationHost | None = None,
    ) -> None:
        self._store = store
        self._host = host or McpGenerationHost()
        self._active_servers: Tuple[str, ...] = ()
        # workspace 声明和插件声明是两个来源，但共用一代 MCP catalog：
        # 任一来源变化都要带着另一来源的现有 server 一起重新发布。
        self._sources: Dict[str, Dict[str, Mapping[str, Any]]] = {}
        self._lock = asyncio.Lock()

    @property
    def host(self) -> McpGenerationHost:
        return self._host

    async def publish(
        self,
        server_specs: Mapping[str, Mapping[str, Any]],
        *,
        source: str = "workspace",
    ) -> str:
        """连接并发布一整批候选；失败时旧快照继续服务。"""

        async with self._lock:
            return await self._publish_locked(server_specs, source)

    async def _publish_locked(
        self,
        server_specs: Mapping[str, Mapping[str, Any]],
        source: str,
    ) -> str:
        merged: Dict[str, Mapping[str, Any]] = {}
        for name, specs in {**self._sources, source: dict(server_specs)}.items():
            for server_name, spec in specs.items():
                if server_name in merged:
                    raise RuntimeError(
                        "MCP server name claimed by two sources: %s" % server_name
                    )
                merged[server_name] = spec

        # 1. 先把整批候选连接好；任一失败都不会影响正在服务的旧代际。
        generation_id = uuid4().hex[:12]
        catalog = await self._host.prepare(
            generation_id,
            server_specs=merged,
            required_tools={},
        )

        # 2. 编译成新快照并发布；这次只换 MCP，其余能力从当前快照原样继承。
        candidate = derive_snapshot(
            self._store.current,
            mcp_tools=self._build_tools(catalog),
            mcp_generation_id=generation_id,
            revision=generation_id,
        )
        transaction = self._store.publish(candidate)
        try:
            await self._store.commit(transaction)
        except BaseException:
            await self._store.rollback(transaction)
            await self._host.close(generation_id)
            raise

        self._sources[source] = dict(server_specs)
        self._active_servers = tuple(sorted(catalog.servers))
        return generation_id

    async def drain_snapshot(self, snapshot: RuntimeSnapshot) -> None:
        """快照退休且租约排空后才断开它持有的 MCP 进程。"""

        if snapshot.mcp_generation_id is None:
            return
        try:
            await self._host.close(snapshot.mcp_generation_id)
        except RuntimeError as error:
            logger.error("stale MCP generation cleanup failed: %s", error)

    async def shutdown(self) -> None:
        self._active_servers = ()
        await self._host.close_all()

    def _build_tools(self, catalog: PreparedMcpCatalog) -> Dict[str, Tool]:
        tools: Dict[str, Tool] = {}
        for server in catalog.servers.values():
            for info in server.tools:
                name = "mcp_%s__%s" % (_sanitize(server.name), _sanitize(info.name))
                if name in tools:
                    raise RuntimeError("duplicate MCP tool name: %s" % name)
                tools[name] = Tool(
                    spec=ToolSpec(
                        name,
                        "[MCP:%s] %s" % (server.name, info.description),
                        info.input_schema,
                    ),
                    handler=self._tool_handler(server, info.name),
                    deferred=True,
                    meta=ToolMeta(
                        risk="external-side-effect",
                        always_on=False,
                        source_type="mcp",
                        source_name=server.name,
                    ),
                )
        return tools

    @staticmethod
    def _tool_handler(server: Any, remote_name: str) -> Any:
        async def invoke(**kwargs: Any) -> str:
            return await server.client.call(remote_name, kwargs)

        return invoke

    def status(self) -> Dict[str, Any]:
        snapshot = self._store.current
        return {
            "generationId": snapshot.mcp_generation_id if snapshot else None,
            "snapshotId": snapshot.snapshot_id if snapshot else None,
            "servers": list(self._active_servers),
            "tools": list(snapshot.mcp_tool_names) if snapshot else [],
        }
