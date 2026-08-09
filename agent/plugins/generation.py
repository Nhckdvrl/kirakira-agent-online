"""插件代际(照 Reference `agent/plugins/generation.py` 移植)。

现有全局 `RuntimeSnapshot` 保护的是"一代运行时能力"整体;本模块补上**每个插件自己的代际**:
一个插件的贡献(phase 模块、工具钩子、MCP、服务、主动源、作业、skill 根)在装载时被
**冻结**成 `PluginContributions`,连同源码/配置 revision 一起构成 `PluginGeneration`。

有了 per-plugin 代际才谈得上热重载:换代时只替换单个插件的代际,在途 turn 仍持有旧代际
的租约(`lease_count`),旧代际要等租约归零才真正 quiesce。语义检查(`GateResult`)决定
一个候选代际是否被准入——检查不过就不发布,旧代际继续服务。
"""

from __future__ import annotations

import hashlib
import asyncio
import logging
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, Literal, Tuple

logger = logging.getLogger(__name__)

from agent.plugins.specs import (
    PluginSemanticCheck,
    RegisteredProactiveSource,
)

GateStatus = Literal["passed", "failed"]
GenerationState = Literal["compiled", "active", "retired", "quiesced"]


@dataclass(frozen=True)
class GateResult:
    """插件代际准入结果。任一检查失败即 failed,该候选代际不得发布。"""

    plugin_id: str
    candidate_revision: str
    status: GateStatus
    checks: Tuple[PluginSemanticCheck, ...] = ()
    failure_reason: str = ""

    @property
    def passed(self) -> bool:
        return self.status == "passed"

    @classmethod
    def from_checks(
        cls,
        *,
        plugin_id: str,
        candidate_revision: str,
        checks: Tuple[PluginSemanticCheck, ...],
    ) -> "GateResult":
        failed = [check for check in checks if not check.passed]
        return cls(
            plugin_id=plugin_id,
            candidate_revision=candidate_revision,
            status="failed" if failed else "passed",
            checks=tuple(checks),
            failure_reason="; ".join(
                "%s: %s" % (check.check_id, check.detail) for check in failed
            ),
        )


@dataclass(frozen=True)
class PluginContributions:
    """一个插件在某一代际里贡献的全部能力,装载后不再变化。"""

    skill_roots: Tuple[Path, ...] = ()
    drift_skill_roots: Tuple[Path, ...] = ()
    mcp_servers: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    managed_services: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    before_turn_modules: Tuple[object, ...] = ()
    before_reasoning_modules: Tuple[object, ...] = ()
    prompt_render_modules: Tuple[object, ...] = ()
    before_step_modules: Tuple[object, ...] = ()
    after_step_modules: Tuple[object, ...] = ()
    after_reasoning_modules: Tuple[object, ...] = ()
    after_turn_modules: Tuple[object, ...] = ()
    tool_hooks: Tuple[object, ...] = ()
    proactive_sources: Tuple[RegisteredProactiveSource, ...] = ()
    jobs: Tuple[object, ...] = ()
    channels: Tuple[object, ...] = ()


@dataclass
class PluginGeneration:
    """一个插件的一代。lease_count 决定它何时可以被安全销毁。"""

    plugin_id: str
    generation_id: str
    module_path: str
    source_revision: str
    config_revision: str
    instance: object
    contributions: PluginContributions
    gate_result: GateResult
    state: GenerationState = "compiled"
    lease_count: int = 0

    @property
    def revision(self) -> str:
        return "%s:%s" % (self.source_revision, self.config_revision)

    @property
    def can_quiesce(self) -> bool:
        """只有退休且无在途租约的代际才能真正销毁。"""
        return self.state == "retired" and self.lease_count <= 0

    def acquire(self) -> None:
        if self.state not in ("compiled", "active"):
            raise RuntimeError(
                "plugin generation 不再接受租约: %s (%s)" % (self.plugin_id, self.state)
            )
        self.lease_count += 1

    def release(self) -> None:
        if self.lease_count <= 0:
            raise RuntimeError("plugin generation 租约重复释放: %s" % self.plugin_id)
        self.lease_count -= 1


def file_revision(path: Path) -> str:
    """按内容算 revision;文件不存在时返回空串,调用方据此判断是否可换代。"""
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    except OSError:
        return ""


def compute_generation_id(
    *,
    plugin_id: str,
    source_revision: str,
    config_revision: str,
) -> str:
    identity = "%s|%s|%s" % (plugin_id, source_revision, config_revision)
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]


class PluginGenerationRegistry:
    """持有每个插件的当前代际与已退休待 quiesce 的代际。"""

    def __init__(self) -> None:
        self._current: Dict[str, PluginGeneration] = {}
        self._retired: list[PluginGeneration] = []
        self._publication_in_progress = False
        self._publication_condition = asyncio.Condition()

    @property
    def publication_in_progress(self) -> bool:
        return self._publication_in_progress

    async def begin_publication(self) -> None:
        async with self._publication_condition:
            if self._publication_in_progress:
                raise RuntimeError("plugin publication transaction already in flight")
            self._publication_in_progress = True

    async def finish_publication(self) -> None:
        async with self._publication_condition:
            self._publication_in_progress = False
            self._publication_condition.notify_all()

    @asynccontextmanager
    async def lease_committed(self):
        """Admit only after every capability surface reaches one commit point."""

        leased: list[PluginGeneration] = []
        async with self._publication_condition:
            while self._publication_in_progress:
                await self._publication_condition.wait()
            for generation in self.active:
                generation.acquire()
                leased.append(generation)
        try:
            yield tuple(leased)
        finally:
            for generation in reversed(leased):
                generation.release()

    def publish(self, generation: PluginGeneration) -> PluginGeneration | None:
        """发布新代际,返回被它取代的旧代际(已置为 retired)。

        gate 未通过的候选代际不得发布——旧代际继续服务比半新状态更安全。
        """
        if not generation.gate_result.passed:
            raise ValueError(
                "plugin gate 未通过,拒绝发布代际: %s (%s)"
                % (generation.plugin_id, generation.gate_result.failure_reason)
            )
        previous = self._current.get(generation.plugin_id)
        generation.state = "active"
        self._current[generation.plugin_id] = generation
        if previous is not None:
            previous.state = "retired"
            self._retired.append(previous)
        return previous

    def current(self, plugin_id: str) -> PluginGeneration | None:
        return self._current.get(plugin_id)

    def retire(self, plugin_id: str) -> PluginGeneration | None:
        """插件被卸载/禁用时退休它的当前代际;仍有租约的代际等归零后才 quiesce。"""
        generation = self._current.pop(plugin_id, None)
        if generation is not None:
            generation.state = "retired"
            self._retired.append(generation)
        return generation

    @property
    def active(self) -> Tuple[PluginGeneration, ...]:
        return tuple(self._current[key] for key in sorted(self._current))

    @property
    def retired(self) -> Tuple[PluginGeneration, ...]:
        return tuple(self._retired)

    @contextmanager
    def lease_active(self) -> "Iterator[Tuple[PluginGeneration, ...]]":
        """为一次在途 turn 持有当前所有活跃代际的租约。

        turn 期间即使发生热重载,这些代际也只会转 retired 而不会被销毁,
        turn 结束释放后才可 quiesce——这就是"换代不抽走在途能力"的实现。
        """
        leased: list[PluginGeneration] = []
        try:
            for generation in self.active:
                generation.acquire()
                leased.append(generation)
            yield tuple(leased)
        finally:
            for generation in reversed(leased):
                try:
                    generation.release()
                except RuntimeError:  # pragma: no cover - 释放期异常不掩盖主错误
                    logger.warning(
                        "plugin generation lease release failed: %s",
                        generation.plugin_id,
                    )

    def drain_quiescible(self) -> Tuple[PluginGeneration, ...]:
        """取出所有租约已归零的退休代际,交由调用方释放资源。"""
        ready = tuple(gen for gen in self._retired if gen.can_quiesce)
        if ready:
            done = {id(gen) for gen in ready}
            self._retired = [gen for gen in self._retired if id(gen) not in done]
            for gen in ready:
                gen.state = "quiesced"
        return ready

    def retire_all(self) -> Tuple[PluginGeneration, ...]:
        for generation in self._current.values():
            generation.state = "retired"
            self._retired.append(generation)
        self._current.clear()
        return tuple(self._retired)
