"""插件声明式规格(照 Reference `agent/plugins/specs.py` 移植)。

插件用这些 frozen dataclass **声明**它能提供什么(MCP server、托管服务、主动数据源),
由 PluginManager 收集、runtime 编译成真实对象。声明与实现分离是 Reference 插件生态的
架构核心:插件不自己去 new 一个 source,而是描述它,让 runtime 在正确的代际里装配。

Reference 的 MobileUi* 未移植:kirakira 没有 mobile 运行时,按"能力以运行时为准"原则
不引入没有承载的声明。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

ProactiveChannel = Literal["alert", "content", "context"]


@dataclass(frozen=True)
class McpServerSpec:
    """插件用代码声明的 MCP server;path 一律相对插件根解析。"""

    name: str
    command: tuple[str, ...]
    env: dict[str, str] = field(default_factory=dict)
    cwd: str = "."


@dataclass(frozen=True)
class ManagedServiceSpec:
    """插件声明的长驻子进程服务,由 runtime 负责拉起/就绪探测/关停。"""

    id: str
    command: tuple[str, ...]
    env: dict[str, str] = field(default_factory=dict)
    cwd: str = "."
    readiness_url: str = ""
    startup_timeout_seconds: float = 15


@dataclass(frozen=True)
class ProactiveSourceSpec:
    """插件声明的主动数据源:由某个 MCP server 的 fetch/ack 工具承载。

    这条声明让主动链路可扩展——插件只描述"哪个 server 的哪个工具能拉事件",
    runtime 把它编译成 SourceRegistry 认识的真实 ProactiveSource。
    """

    id: str
    channels: tuple[ProactiveChannel, ...]
    server: str
    fetch_tool: str
    ack_tool: str = ""
    fetch_page_size: int = 0


@dataclass(frozen=True)
class RegisteredProactiveSource:
    plugin_id: str
    spec: ProactiveSourceSpec


def proactive_source_key(source: RegisteredProactiveSource) -> str:
    return f"{source.plugin_id}:{source.spec.id}"


@dataclass(frozen=True)
class PluginSemanticCheck:
    """插件静态/就绪语义检查结果。failed 的检查会阻止插件进入可用代际。"""

    check_id: str
    passed: bool
    detail: str = ""

    @classmethod
    def ok(cls, check_id: str, detail: str = "") -> "PluginSemanticCheck":
        return cls(check_id=check_id, passed=True, detail=detail)

    @classmethod
    def fail(cls, check_id: str, detail: str) -> "PluginSemanticCheck":
        return cls(check_id=check_id, passed=False, detail=detail)


@dataclass(frozen=True)
class PluginReadinessContext:
    """就绪检查上下文:插件可据此判断依赖的运行时能力是否真的在。"""

    workspace_tool_names: tuple[str, ...] = ()
    mcp_server_names: tuple[str, ...] = ()
