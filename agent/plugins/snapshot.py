"""Runtime capability snapshots with per-turn leases.

热重载和被动 turn 是两个并发的时间线：turn 跑到一半时 MCP 声明可能变了、插件可能换代。
如果直接在共享注册表上增删工具，模型会在同一个 turn 内看到工具凭空消失，甚至拿着已经
断开的连接去调用。

这里用“代际快照 + 租约”解决：

┌─ RuntimeSnapshotStore
│  ├─ current            当前对新 turn 生效的快照
│  ├─ publish/commit     换代是事务：候选先就绪，再原子切换，失败可回滚
│  └─ retire + drain     旧快照只有在最后一个租约释放后才真正销毁资源
└─ RuntimeSnapshotLease
   └─ 一个 turn 开始时取一份租约，整个 turn 都看同一份能力集合

因此：新 turn 立刻用新能力；在途 turn 用完旧能力才让旧资源下线。
"""

from __future__ import annotations

import asyncio
import hashlib
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
)

from core.schema import JsonDict, ToolCall, ToolResult, ToolSpec

if TYPE_CHECKING:  # tools.registry 会反向依赖本模块，运行时按需惰性导入。
    from agent.tools.registry import Tool, ToolRegistry

SnapshotState = str  # "compiled" | "published" | "retired" | "drained"

_PHASE_FIELDS = (
    "before_turn_modules",
    "before_reasoning_modules",
    "prompt_render_modules",
    "before_step_modules",
    "after_step_modules",
    "after_reasoning_modules",
    "after_turn_modules",
)


@dataclass
class RuntimeSnapshot:
    """一代运行时能力的不可变视图。"""

    snapshot_id: str
    before_turn_modules: Tuple[object, ...] = ()
    before_reasoning_modules: Tuple[object, ...] = ()
    prompt_render_modules: Tuple[object, ...] = ()
    before_step_modules: Tuple[object, ...] = ()
    after_step_modules: Tuple[object, ...] = ()
    after_reasoning_modules: Tuple[object, ...] = ()
    after_turn_modules: Tuple[object, ...] = ()
    tool_hooks: Tuple[object, ...] = ()
    # MCP 工具只挂在快照上，不进共享注册表，避免换代时把在途 turn 的工具抽走。
    mcp_tools: Mapping[str, Tool] = field(
        default_factory=lambda: MappingProxyType({})
    )
    mcp_generation_id: Optional[str] = None
    state: SnapshotState = "compiled"
    lease_count: int = 0

    @property
    def mcp_tool_names(self) -> Tuple[str, ...]:
        return tuple(sorted(self.mcp_tools))


def compile_snapshot(
    *,
    phase_modules: Mapping[str, List[object]] | None = None,
    tool_hooks: List[object] | None = None,
    mcp_tools: Mapping[str, Tool] | None = None,
    mcp_generation_id: str | None = None,
    revision: str = "",
) -> RuntimeSnapshot:
    """把当前各来源的能力编译成一份带稳定 id 的快照。"""

    phases = phase_modules or {}
    for key in phases:
        if key not in _PHASE_FIELDS:
            raise ValueError("unknown phase field: %s" % key)
    tools = MappingProxyType(dict(mcp_tools or {}))
    identity = "|".join(
        [
            "mcp:%s" % (mcp_generation_id or ""),
            "tools:%s" % ",".join(sorted(tools)),
            "revision:%s" % revision,
        ]
        + [
            "%s:%d" % (name, len(phases.get(name, ())))
            for name in _PHASE_FIELDS
        ]
    )
    snapshot_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return RuntimeSnapshot(
        snapshot_id=snapshot_id,
        **{name: tuple(phases.get(name, ())) for name in _PHASE_FIELDS},
        tool_hooks=tuple(tool_hooks or ()),
        mcp_tools=tools,
        mcp_generation_id=mcp_generation_id,
    )


def derive_snapshot(
    base: Optional[RuntimeSnapshot],
    *,
    mcp_tools: Mapping[str, Tool] | None = None,
    mcp_generation_id: str | None = None,
    revision: str = "",
) -> RuntimeSnapshot:
    """在保留 base 其余能力的前提下换掉一部分能力，编译出新候选快照。"""

    phases = (
        {name: list(getattr(base, name)) for name in _PHASE_FIELDS}
        if base is not None
        else {}
    )
    return compile_snapshot(
        phase_modules=phases,
        tool_hooks=list(base.tool_hooks) if base is not None else [],
        mcp_tools=mcp_tools if mcp_tools is not None else (base.mcp_tools if base else {}),
        mcp_generation_id=(
            mcp_generation_id
            if mcp_generation_id is not None
            else (base.mcp_generation_id if base else None)
        ),
        revision=revision,
    )


@dataclass(frozen=True)
class SnapshotTransaction:
    previous: Optional[RuntimeSnapshot]
    candidate: RuntimeSnapshot


class RuntimeSnapshotLease:
    """一份租约代表“这一路执行仍在使用该快照”。"""

    def __init__(self, store: "RuntimeSnapshotStore", snapshot: RuntimeSnapshot) -> None:
        self._store = store
        self.snapshot = snapshot
        self._released = False

    @property
    def active(self) -> bool:
        return not self._released

    def fork(self) -> "RuntimeSnapshotLease":
        return self._store.fork_lease(self)

    async def __aenter__(self) -> RuntimeSnapshot:
        return self.snapshot

    async def __aexit__(self, *exc_info: object) -> None:
        await self.release()

    async def release(self) -> None:
        if self._released:
            return
        self._released = True
        await self._store.release_lease(self.snapshot)


@dataclass(frozen=True)
class _SnapshotBinding:
    lease: RuntimeSnapshotLease
    owner_task: Optional["asyncio.Task[Any]"]


_current_binding: ContextVar[Optional[_SnapshotBinding]] = ContextVar(
    "kirakira_runtime_snapshot", default=None
)


def bind_runtime_snapshot(lease: RuntimeSnapshotLease) -> Token:
    return _current_binding.set(
        _SnapshotBinding(lease=lease, owner_task=asyncio.current_task())
    )


def reset_runtime_snapshot(token: Token) -> None:
    _current_binding.reset(token)


def get_current_runtime_snapshot() -> Optional[RuntimeSnapshot]:
    """只有绑定它的那个 task 才能读到快照，子任务必须自己 fork 租约。"""

    binding = _current_binding.get()
    if (
        binding is None
        or not binding.lease.active
        or binding.owner_task is not asyncio.current_task()
    ):
        return None
    return binding.lease.snapshot


def get_current_runtime_lease() -> Optional[RuntimeSnapshotLease]:
    binding = _current_binding.get()
    if (
        binding is None
        or not binding.lease.active
        or binding.owner_task is not asyncio.current_task()
    ):
        return None
    return binding.lease


class RuntimeSnapshotStore:
    """持有当前快照，并保证旧快照资源在租约排空后才销毁。"""

    def __init__(
        self,
        *,
        on_drain: Callable[[RuntimeSnapshot], Awaitable[None]] | None = None,
    ) -> None:
        self._current: Optional[RuntimeSnapshot] = None
        self._pending: Optional[SnapshotTransaction] = None
        self._snapshots: Dict[str, RuntimeSnapshot] = {}
        self._on_drain = on_drain
        self._condition = asyncio.Condition()

    @property
    def current(self) -> Optional[RuntimeSnapshot]:
        return self._current

    def set_drain_handler(
        self, on_drain: Callable[[RuntimeSnapshot], Awaitable[None]]
    ) -> None:
        """资源持有者（如 MCP publisher）在装配期注册自己的回收动作。"""

        self._on_drain = on_drain

    def publish(self, candidate: RuntimeSnapshot) -> SnapshotTransaction:
        """开启一次换代事务；新 turn 立刻看到候选，旧快照进入退休流程。"""

        if self._pending is not None:
            raise RuntimeError("a snapshot transaction is already in flight")
        if candidate.state != "compiled":
            raise RuntimeError("only a compiled snapshot can be published")
        transaction = SnapshotTransaction(previous=self._current, candidate=candidate)
        candidate.state = "published"
        self._snapshots[candidate.snapshot_id] = candidate
        self._current = candidate
        self._pending = transaction
        return transaction

    async def commit(self, transaction: SnapshotTransaction) -> None:
        if self._pending is not transaction:
            raise RuntimeError("unknown snapshot transaction")
        self._pending = None
        previous = transaction.previous
        if previous is not None and previous is not transaction.candidate:
            previous.state = "retired"
            await self._drain_if_ready(previous)

    async def rollback(self, transaction: SnapshotTransaction) -> None:
        """候选发布后校验失败时回到旧快照，并销毁候选自己的资源。"""

        if self._pending is not transaction:
            raise RuntimeError("unknown snapshot transaction")
        self._pending = None
        self._current = transaction.previous
        candidate = transaction.candidate
        candidate.state = "retired"
        await self._drain_if_ready(candidate)

    def lease(self) -> RuntimeSnapshotLease:
        snapshot = self._current
        if snapshot is None:
            raise RuntimeError("no runtime snapshot is available")
        if snapshot.state != "published":
            raise RuntimeError("snapshot is not leasable: %s" % snapshot.state)
        return self._claim(snapshot)

    def fork_lease(self, source: RuntimeSnapshotLease) -> RuntimeSnapshotLease:
        snapshot = source.snapshot
        if not source.active or self._snapshots.get(snapshot.snapshot_id) is not snapshot:
            raise RuntimeError("snapshot lease cannot be forked")
        return self._claim(snapshot)

    def _claim(self, snapshot: RuntimeSnapshot) -> RuntimeSnapshotLease:
        snapshot.lease_count += 1
        return RuntimeSnapshotLease(self, snapshot)

    async def release_lease(self, snapshot: RuntimeSnapshot) -> None:
        if snapshot.lease_count <= 0:
            raise RuntimeError(
                "snapshot lease count underflow: %s" % snapshot.snapshot_id
            )
        snapshot.lease_count -= 1
        await self._drain_if_ready(snapshot)
        async with self._condition:
            self._condition.notify_all()

    async def wait_drained(self, snapshot: RuntimeSnapshot) -> None:
        async with self._condition:
            while snapshot.lease_count:
                await self._condition.wait()

    async def close(self) -> None:
        if self._pending is not None:
            raise RuntimeError("a snapshot transaction is still in flight")
        current = self._current
        self._current = None
        if current is not None:
            current.state = "retired"
            await self._drain_if_ready(current)

    async def _drain_if_ready(self, snapshot: RuntimeSnapshot) -> None:
        """退休且没有租约时才真正回收资源，保证在途 turn 用完旧 MCP 再断开。"""

        if snapshot.state != "retired" or snapshot.lease_count:
            return
        snapshot.state = "drained"
        self._snapshots.pop(snapshot.snapshot_id, None)
        if self._on_drain is not None:
            await self._on_drain(snapshot)


class SnapshotToolView:
    """基础注册表 + 当前快照 MCP 工具的只读组合视图。"""

    def __init__(self, base: ToolRegistry, snapshot: Optional[RuntimeSnapshot]) -> None:
        self._base = base
        self._overlay: Mapping[str, Tool] = (
            snapshot.mcp_tools if snapshot is not None else MappingProxyType({})
        )

    def visible_specs(self, unlocked: set[str] | None = None) -> List[ToolSpec]:
        allowed = unlocked or set()
        specs = self._base.visible_specs(allowed)
        specs.extend(
            tool.spec for name, tool in self._overlay.items() if name in allowed
        )
        return specs

    def specs(self) -> List[ToolSpec]:
        return self._base.specs() + [tool.spec for tool in self._overlay.values()]

    def is_deferred(self, name: str) -> bool:
        if name in self._overlay:
            return True
        return self._base.is_deferred(name)

    def has(self, name: str) -> bool:
        return name in self._overlay or self._base.has(name)

    def names(self) -> List[str]:
        return sorted(set(self._base.names()) | set(self._overlay))

    def get_tool(self, name: str) -> Optional[Tool]:
        tool = self._overlay.get(name)
        return tool if tool is not None else self._base.get_tool(name)

    def set_context(self, **kwargs: Any) -> Token:
        return self._base.set_context(**kwargs)

    def reset_context(self, token: Token) -> None:
        self._base.reset_context(token)

    @property
    def context(self) -> JsonDict:
        return self._base.context

    async def execute_async(self, call: ToolCall) -> ToolResult:
        tool = self._overlay.get(call.name)
        if tool is None:
            return await self._base.execute_async(call)
        return await _execute_overlay_tool(tool, call)


async def _execute_overlay_tool(tool: Tool, call: ToolCall) -> ToolResult:
    from agent.tools.registry import _validate_arguments, _looks_like_error

    validation_error = _validate_arguments(tool.spec, call.arguments)
    if validation_error:
        return ToolResult(call.id, validation_error, is_error=True)
    try:
        output = await tool.handler(**call.arguments)
    except Exception as exc:
        return ToolResult(call.id, "Error: %s" % exc, is_error=True)
    if isinstance(output, ToolResult):
        return ToolResult(call.id, output.content, output.is_error)
    text = str(output)
    return ToolResult(call.id, text, is_error=_looks_like_error(text))
