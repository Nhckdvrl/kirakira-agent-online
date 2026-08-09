"""把插件声明的 ProactiveSourceSpec 编译成真实 ProactiveSource。

照 Reference `proactive_v2/mcp_sources.py` 移植:插件只**声明**"哪个 MCP server 的哪个
工具能拉事件/确认事件",由 runtime 在正确的工具代际里编译成 SourceRegistry 认识的对象。
这条链路让主动推送从"内置文件源示例"变成可被插件扩展的运行时。

与 Reference 的差异只在网关适配:kirakira 的 ToolRegistry 用 `execute_async(ToolCall)`,
MCP 工具注册名为 `mcp_<server>__<tool>`。
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Protocol, Sequence
from uuid import uuid4

from agent.plugins.specs import (
    RegisteredProactiveSource,
    proactive_source_key,
)
from core.schema import ToolCall

logger = logging.getLogger(__name__)

_MAX_PAGES = 256


class McpGateway(Protocol):
    async def call(
        self,
        server: str,
        tool_name: str,
        args: Dict[str, Any],
    ) -> Any: ...


class ToolRegistryMcpGateway:
    """通过 ToolRegistry 调用 MCP 工具。工具不在当前代际时直接失败,不静默降级。

    MCP 工具只挂在 runtime snapshot 上,不进基础注册表。tick 开始时 loop 把租到的
    快照钉在这里(`pin_snapshot`),本轮所有 source 的 fetch/ack 都用这一代工具视图——
    这是"tick 期间工具代际固定"的另一半;只拿租约但仍读基础注册表,主动源永远看不到
    快照工具,也谈不上代际一致。
    """

    def __init__(self, tools: Any) -> None:
        self._tools = tools
        self._pinned_snapshot: Any = None

    def pin_snapshot(self, snapshot: Any) -> None:
        """由 ProactiveLoop 在 tick 开始/结束时设置与清除。单事件循环内 tick 串行,无并发。"""
        self._pinned_snapshot = snapshot

    def _view(self) -> Any:
        if self._pinned_snapshot is not None:
            from agent.plugins.snapshot import SnapshotToolView

            return SnapshotToolView(self._tools, self._pinned_snapshot)
        return self._tools

    async def call(
        self,
        server: str,
        tool_name: str,
        args: Dict[str, Any],
    ) -> Any:
        if self._tools is None:
            raise RuntimeError("共享 ToolRegistry 不可用")
        tools = self._view()
        available = set(tools.names())
        registered = tool_name if tool_name in available else "mcp_%s__%s" % (server, tool_name)
        if registered not in available:
            raise RuntimeError("MCP tool 不可用: %s.%s" % (server, tool_name))
        result = await tools.execute_async(
            ToolCall(id="proactive-%s" % uuid4().hex[:8], name=registered, arguments=dict(args))
        )
        text = getattr(result, "content", None)
        if text is None:
            text = str(result)
        if getattr(result, "is_error", False):
            raise RuntimeError("MCP tool 调用失败: %s.%s: %s" % (server, tool_name, text))
        stripped = str(text).strip()
        if stripped.startswith(("[", "{")):
            return json.loads(stripped)
        return text


class McpProactiveSource:
    """由插件声明编译出来的主动数据源,实现 ProactiveSource 协议。"""

    def __init__(
        self,
        registered: RegisteredProactiveSource,
        gateway: McpGateway,
    ) -> None:
        self._registered = registered
        self._gateway = gateway
        self.id = proactive_source_key(registered)
        self.channels: Sequence[str] = tuple(registered.spec.channels)

    async def fetch(self) -> List[Dict[str, Any]]:
        """拉取并严格校验;失败原样抛出,由 SourceRegistry 决定部分可用语义。"""
        spec = self._registered.spec
        data = (
            await self._fetch_pages()
            if spec.fetch_page_size > 0
            else await self._gateway.call(spec.server, spec.fetch_tool, {})
        )
        # 纯 context 源允许返回单个 dict 快照(没有 event_id 概念)。
        if "context" in spec.channels and isinstance(data, dict):
            item = dict(data)
            item.setdefault("kind", "context")
            item.setdefault("_source", self.id)
            return [item]
        if not isinstance(data, list):
            raise RuntimeError("source 返回值必须是 list 或 context dict: %s" % self.id)

        events: List[Dict[str, Any]] = []
        for raw in data:
            if not isinstance(raw, dict):
                raise RuntimeError(
                    "source item 必须是 object: %s (%s)" % (self.id, type(raw).__name__)
                )
            kind = str(raw.get("kind") or "").strip()
            if not kind and len(spec.channels) == 1:
                kind = spec.channels[0]
            if kind not in spec.channels:
                continue
            item = dict(raw)
            item["kind"] = kind
            if kind == "context":
                item.setdefault("_source", self.id)
            else:
                # alert/content 必须有稳定 id,否则无法去重与 ACK。
                event_id = str(raw.get("event_id") or raw.get("id") or "").strip()
                if not event_id:
                    raise RuntimeError("source item 缺少 event_id/id: %s" % self.id)
                item["event_id"] = event_id
                item.setdefault("ack_server", self.id)
            events.append(item)
        return events

    async def _fetch_pages(self) -> List[Any]:
        spec = self._registered.spec
        page_size = spec.fetch_page_size
        result: List[Any] = []
        offset = 0
        for _ in range(_MAX_PAGES):
            page = await self._gateway.call(
                spec.server,
                spec.fetch_tool,
                {"offset": offset, "limit": page_size},
            )
            if not isinstance(page, list):
                raise RuntimeError("分页 source 返回值必须是 list: %s" % self.id)
            result.extend(page)
            if len(page) < page_size:
                return result
            offset += len(page)
        raise RuntimeError("分页 source 超过 %d 页: %s" % (_MAX_PAGES, self.id))

    async def ack(self, event_ids: Sequence[str]) -> None:
        """只按 event_id 确认。未声明 ack_tool 的源是只读源,ACK 为空操作。"""
        spec = self._registered.spec
        clean = [str(item).strip() for item in event_ids if str(item).strip()]
        if not spec.ack_tool or not clean:
            return
        await self._gateway.call(spec.server, spec.ack_tool, {"event_ids": clean})


def compile_proactive_sources(
    registered: Sequence[RegisteredProactiveSource],
    tools: Any,
    *,
    gateway: ToolRegistryMcpGateway | None = None,
) -> List[McpProactiveSource]:
    """把插件声明编译成真实 source 列表。

    传入共享 gateway 时全部源复用它——ProactiveLoop 靠这个共享实例做 tick 级快照钉定。
    """
    shared = gateway or ToolRegistryMcpGateway(tools)
    return [McpProactiveSource(item, shared) for item in registered]
