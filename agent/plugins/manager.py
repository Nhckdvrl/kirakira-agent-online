"""Small plugin loader for lifecycle modules, tool hooks, tools, and channels."""

from __future__ import annotations

import asyncio
import time
import hashlib
import importlib.util
import inspect
import json
import logging
import os
import re
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from bus.event_bus import EventBus
from agent.config import load_toml_config
from agent.lifecycle.phase import inspect_phase, topo_sort_modules
from agent.plugins.generation import (
    GateResult,
    PluginContributions,
    PluginGeneration,
    PluginGenerationRegistry,
    compute_generation_id,
)
from agent.plugins.jobs import PluginJobHost, PluginJobSpec
from agent.plugins.reload_journal import ReloadJournal
from agent.plugins.registry import plugin_registry
from agent.plugins.service_host import PluginServiceHost
from agent.plugins.specs import (
    ManagedServiceSpec,
    McpServerSpec,
    PluginReadinessContext,
    PluginSemanticCheck,
    ProactiveSourceSpec,
    RegisteredProactiveSource,
    proactive_source_key,
)
from agent.plugins.manifest import (
    MANIFEST_NAME,
    PluginEnablement,
    discover_plugin_roots,
    is_enabled,
    load_manifest,
    normalize_command_item,
    resolve_skill_roots,
    safe_child,
)
from agent.plugins.decorators import (
    get_bindings,
    on_after_reasoning,
    on_after_step,
    on_after_turn,
    on_before_reasoning,
    on_before_step,
    on_before_turn,
    on_prompt_render,
    on_tool_pre,
    tool,
)
from core.schema import ToolSpec
from agent.tool_hooks import HookContext, HookOutcome, ToolHook
from agent.tools.registry import ToolRegistry, object_schema

logger = logging.getLogger(__name__)


def _path_content_digest(path: Path) -> bytes:
    """按内容取指纹;文件缺失返回固定标记,让"删除"本身也算一次变化。"""
    try:
        return hashlib.sha256(path.read_bytes()).digest()
    except OSError:
        return b"\x00missing"


def _plugin_source_revision(root: Path) -> str:
    """按插件包的全部可分发源码/资源取指纹。

    MCP server、Drift skill 和脚本都是插件行为的一部分；只哈希
    ``plugin.py`` 会让这些文件修改后运行时继续使用旧代。
    """
    digest = hashlib.sha256()
    if not root.exists():
        return ""
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        if "__pycache__" in relative.parts or path.suffix in {".pyc", ".pyo"}:
            continue
        if path.name in {"config.toml", "config.local.toml"}:
            continue
        digest.update(relative.as_posix().encode("utf-8"))
        digest.update(_path_content_digest(path))
    return digest.hexdigest()[:16]


def _plugin_config_revision(root: Path, data_dir: Path) -> str:
    digest = hashlib.sha256()
    for path in (
        root / "config.toml",
        root / "config.local.toml",
        data_dir / "config.local.toml",
    ):
        digest.update(str(path.name).encode("utf-8"))
        digest.update(_path_content_digest(path))
    return digest.hexdigest()[:16]


def _source_plugin_name(source: str) -> str:
    """插件身份取来源目录名/仓库名；目录名必须与插件 name 一致。"""

    text = source.strip().rstrip("/")
    if text.endswith(".git"):
        text = text[: -len(".git")]
    return Path(text).name


class PluginKVStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def get(self, key: str, default: Any = None) -> Any:
        return self._read().get(key, default)

    def set(self, key: str, value: Any) -> None:
        data = self._read()
        data[key] = value
        self._write(data)

    def increment(self, key: str, delta: int = 1) -> int:
        data = self._read()
        value = int(data.get(key, 0)) + int(delta)
        data[key] = value
        self._write(data)
        return value

    def _read(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {}
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Plugin KV store must contain a JSON object")
        return payload

    def _write(self, data: Dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_name(".%s.%s.tmp" % (self.path.name, uuid4().hex))
        try:
            temp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(temp, self.path)
        finally:
            try:
                temp.unlink()
            except FileNotFoundError:
                pass


@dataclass
class PluginContext:
    event_bus: EventBus
    tool_registry: ToolRegistry
    plugin_id: str
    plugin_dir: Path
    data_dir: Path
    kv_store: PluginKVStore
    workspace: Path
    session_manager: Any
    memory: Any
    config: Any = None

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)


@dataclass
class ActivePlugin:
    plugin_id: str
    root: Path
    instance: Optional["Plugin"]

    @property
    def version(self) -> str:
        return str(getattr(self.instance, "version", "") or "")

    @property
    def desc(self) -> str:
        return str(getattr(self.instance, "desc", "") or "")


class DecoratedToolHook:
    event = "pre_tool_use"

    def __init__(self, name: str, method, tool_name: Optional[str], priority: int) -> None:
        self.name = name
        self.method = method
        self.tool_name = tool_name
        self.priority = priority

    def matches(self, ctx: HookContext) -> bool:
        return self.tool_name is None or self.tool_name == ctx.request.tool_name

    async def run(self, ctx: HookContext) -> HookOutcome:
        result = self.method(ctx)
        if inspect.isawaitable(result):
            result = await result
        if result is None:
            return HookOutcome()
        if isinstance(result, HookOutcome):
            return result
        if isinstance(result, dict):
            return HookOutcome(
                decision="deny" if result.get("decision") == "deny" else "allow",
                updated_input=result.get("updated_input"),
                reason=str(result.get("reason") or ""),
                extra_message=str(result.get("extra_message") or ""),
            )
        if result is False:
            return HookOutcome(decision="deny", reason="blocked by plugin hook")
        return HookOutcome()


class Plugin:
    api_version: int = 2
    name: str = ""
    version: str = ""
    desc: str = ""
    author: str = ""
    ConfigModel: Any = None

    def __init_subclass__(cls, **kwargs: object) -> None:
        # 照 Reference:插件类定义即注册,manager 不必扫描类名。
        super().__init_subclass__(**kwargs)
        plugin_registry.register_class(cls)

    async def initialize(self) -> None:
        return None

    async def prepare(self) -> None:
        """Prepare a Runtime API v2 candidate before it becomes visible.

        Existing Kirakira plugins that only implement ``initialize`` keep
        working through this compatibility seam.
        """

        await self.initialize()

    def activate(self) -> None:
        return None

    def retire(self) -> None:
        return None

    async def terminate(self) -> None:
        return None

    # --- 语义检查:能力以运行时为准,检查不过的插件不进入可用代际 ---

    def static_semantic_checks(self) -> List[PluginSemanticCheck]:
        return []

    async def readiness_semantic_checks(
        self,
        context: PluginReadinessContext,
    ) -> List[PluginSemanticCheck]:
        return []

    @classmethod
    def skill_roots(cls) -> tuple[str, ...]:
        return ()

    @classmethod
    def drift_skill_roots(cls) -> tuple[str, ...]:
        return ()

    @classmethod
    def mcp_servers(cls) -> List[McpServerSpec]:
        return []

    @classmethod
    def managed_services(cls) -> List[ManagedServiceSpec]:
        return []

    def proactive_sources(self) -> List[ProactiveSourceSpec]:
        return []

    def jobs(self) -> List[PluginJobSpec]:
        return []

    def before_turn_modules(self) -> List[object]:
        return []

    def before_reasoning_modules(self) -> List[object]:
        return []

    def prompt_render_modules(self) -> List[object]:
        return []

    def before_step_modules(self) -> List[object]:
        return []

    def after_step_modules(self) -> List[object]:
        return []

    def after_reasoning_modules(self) -> List[object]:
        return []

    def after_turn_modules(self) -> List[object]:
        return []

    def tool_hooks(self) -> List[ToolHook]:
        return []

    def register_tools(self, registry: ToolRegistry) -> None:
        return None

    def channels(self) -> List[object]:
        return []


class PluginManager:
    def __init__(
        self,
        plugin_dirs: List[Path],
        *,
        event_bus: EventBus,
        tool_registry: ToolRegistry,
        workspace: Path,
        session_manager: Any,
    memory: Any,
        mcp_publisher: Any = None,
        skill_loader: Any = None,
    ) -> None:
        self.plugin_dirs = plugin_dirs
        self.event_bus = event_bus
        self.tool_registry = tool_registry
        self.workspace = workspace
        self.session_manager = session_manager
        self.memory = memory
        self.mcp_publisher = mcp_publisher
        self.skill_loader = skill_loader
        self.instances: List[Plugin] = []
        self.active: List[ActivePlugin] = []
        self.errors: Dict[str, str] = {}
        self._terminated = False
        self._decorated_modules: Dict[str, List[tuple[int, object]]] = {}
        self._decorated_hooks: List[DecoratedToolHook] = []
        # 声明式扩展点的运行时宿主:插件只声明,生命周期由 manager 持有。
        self.service_host = PluginServiceHost()
        self.job_host = PluginJobHost(event_bus=event_bus)
        # per-plugin 代际:换代时在途 turn 仍持旧代际租约,租约归零才 quiesce。
        self.generations = PluginGenerationRegistry()
        self.reload_journal = ReloadJournal(workspace)
        self._reload_drains: Dict[str, str] = {}
        self._publication_dirty = False
        self._reconcile_lock = asyncio.Lock()
        # watcher 注入的唤醒回调:安装/卸载/启停后立即热重载,不必重启进程。
        self.reload_hook: Optional[Any] = None
        self._register_management_tools()

    async def _activate_plugin(self, root: Path, manifest: Dict[str, Any]) -> str | None:
        """装载并初始化单个插件目录;返回激活后的 plugin_id,跳过时返回 None。"""

        plugin = self._load_one(root / "plugin.py")
        if plugin is None:
            raise ValueError("plugin.py declares no Plugin subclass")
        name = str(getattr(plugin, "name", "") or root.name).strip()
        if any(record.plugin_id == name for record in self.active):
            logger.warning("duplicate plugin name skipped: %s", name)
            return None
        if not is_enabled(manifest, name):
            logger.info("plugin disabled by manifest: %s", name)
            return None
        await self._initialize_plugin(name, root, plugin)
        self.active.append(ActivePlugin(name, root, plugin))
        return name

    async def _deactivate_plugin(self, plugin_id: str) -> None:
        """卸载/禁用一个插件:终止实例并退休它的代际,不影响其余插件。"""

        record = next(
            (item for item in self.active if item.plugin_id == plugin_id), None
        )
        if record is None:
            return
        if record.instance is not None:
            try:
                record.instance.retire()
            except Exception:
                logger.exception("plugin retire failed: %s", plugin_id)
            if record.instance in self.instances:
                self.instances.remove(record.instance)
        self.active.remove(record)
        current = self.generations.current(plugin_id)
        if current is not None:
            self.generations.retire(plugin_id)

    async def load_all(self) -> None:
        # 清单只决定启停；损坏时整体失败，不静默退化成“全部启用”。
        manifest = load_manifest(self.workspace / ".kirakira" / MANIFEST_NAME)
        roots = tuple(discover_plugin_roots(self.plugin_dirs))
        discovered = {root.name: root for root in roots}
        recovery = self.reload_journal.pending_recovery()
        for action in recovery:
            if action.action != "restore_committed":
                continue
            root = discovered.get(action.plugin_id)
            if root is None:
                raise RuntimeError(
                    f"ReloadTransaction 恢复缺少插件: {action.plugin_id}"
                )
            actual = _plugin_source_revision(root)
            if actual != action.source_revision:
                raise RuntimeError(
                    "ReloadTransaction 恢复源码不一致: "
                    f"{action.plugin_id} expected={action.source_revision} actual={actual}"
                )
        for action in recovery:
            if action.action == "discard_candidate":
                self.reload_journal.finish_recovery(action)
        for root in roots:
            try:
                await self._activate_plugin(root, manifest)
            except Exception as exc:
                self.errors[root.name] = str(exc)
                logger.exception("plugin failed to load: %s", root)
        self._sync_skill_links()
        for record in self.active:
            (self.workspace / ".kirakira" / "plugin-data" / record.plugin_id).mkdir(
                parents=True, exist_ok=True
            )
        if self.skill_loader is not None:
            self.skill_loader.reload()
        if self.mcp_publisher is not None:
            # 插件 MCP 与 workspace MCP 共用换代语义：整批发布，失败保持旧代际。
            await self.mcp_publisher.publish(self.mcp_servers, source="plugins")
        # 冻结每个插件这一代的贡献;gate 未过的插件不发布代际。
        await self.publish_generations()
        for action in recovery:
            if action.action != "restore_committed":
                continue
            current = self.generations.current(action.plugin_id)
            if current is None or current.generation_id != action.generation_id:
                raise RuntimeError(
                    "ReloadTransaction 恢复代际不一致: "
                    f"{action.plugin_id} expected={action.generation_id} "
                    f"actual={current.generation_id if current else None}"
                )
            self.reload_journal.finish_recovery(action)
        # 托管服务整批启动:任一失败会回滚已起的进程,不留半启动状态。
        self.service_host.bind_plugin_services(self.managed_services)
        await self.service_host.start_all()
        # 作业注册后统一由 host 驱动,插件不自己起 task。
        self.register_jobs(self.job_host)
        self.job_host.start()

    async def terminate_all(self) -> None:
        if self._terminated:
            return
        self._terminated = True
        # 先停宿主再终止插件:作业/服务不应在插件已 terminate 后还被触发。
        try:
            await self.job_host.aclose()
        except Exception:
            logger.exception("plugin job host shutdown failed")
        try:
            await self.service_host.stop_all()
        except Exception:
            logger.exception("plugin managed service shutdown failed")
        for plugin in reversed(self.instances):
            try:
                plugin.retire()
            except Exception:
                logger.exception("plugin retire failed: %s", plugin.name or type(plugin).__name__)
            try:
                await plugin.terminate()
            except Exception:
                logger.exception("plugin terminate failed: %s", plugin.name or type(plugin).__name__)
        if self.mcp_publisher is not None:
            await self.mcp_publisher.shutdown()

    @property
    def tool_hooks(self) -> List[ToolHook]:
        hooks: List[ToolHook] = []
        for plugin in self.instances:
            hooks.extend(plugin.tool_hooks())
        hooks.extend(sorted(self._decorated_hooks, key=lambda item: -item.priority))
        return hooks

    @property
    def before_turn_modules(self) -> List[object]:
        return self._collect("before_turn_modules")

    @property
    def before_reasoning_modules(self) -> List[object]:
        return self._collect("before_reasoning_modules")

    @property
    def prompt_render_modules(self) -> List[object]:
        return self._collect("prompt_render_modules")

    @property
    def before_step_modules(self) -> List[object]:
        return self._collect("before_step_modules")

    @property
    def after_step_modules(self) -> List[object]:
        return self._collect("after_step_modules")

    @property
    def after_reasoning_modules(self) -> List[object]:
        return self._collect("after_reasoning_modules")

    @property
    def after_turn_modules(self) -> List[object]:
        return self._collect("after_turn_modules")

    def _collect(self, name: str) -> List[object]:
        modules: List[object] = []
        for plugin in self.instances:
            getter = getattr(plugin, name)
            modules.extend(getter())
        phase = name.removesuffix("_modules")
        modules.extend(
            module
            for _priority, module in sorted(
                self._decorated_modules.get(phase, []), key=lambda item: -item[0]
            )
        )
        return self._order_phase_modules(phase, modules)

    @staticmethod
    def _order_phase_modules(phase: str, modules: List[object]) -> List[object]:
        """全部模块都声明了 slot 时按依赖图排序,否则保持注册/优先级顺序。

        只在"全员声明"时启用是刻意的:混用会让未声明 slot 的老模块被隐式重排,
        那种偶然的顺序变化比顺序不可控更难排查。插件全部迁移到 slot 后自动生效。
        """
        if not modules or not all(
            isinstance(getattr(module, "slot", None), str)
            and getattr(module, "slot")
            for module in modules
        ):
            return modules
        try:
            return topo_sort_modules(modules)
        except RuntimeError as error:
            # 依赖成环/重复 slot 是插件的声明错误:保持原顺序并大声报错,
            # 不能让一个坏插件把整个相位打挂。
            logger.error("phase %s slot ordering failed: %s", phase, error)
            return modules

    @property
    def mcp_servers(self) -> Dict[str, Dict[str, Any]]:
        """把各插件代码声明的 MCP server 规范化成 publisher 需要的 spec。"""

        merged: Dict[str, Dict[str, Any]] = {}
        for record in self.active:
            if record.instance is None:
                continue
            for spec in record.instance.mcp_servers():
                if spec.name in merged:
                    logger.warning("duplicate plugin MCP server skipped: %s", spec.name)
                    continue
                if not spec.command:
                    raise ValueError("plugin MCP server has no command: %s" % spec.name)
                data_dir = self.plugin_data_dir(record.plugin_id)
                env = dict(spec.env)
                env.setdefault("KIRAKIRA_PLUGIN_DATA_DIR", str(data_dir))
                merged[spec.name] = {
                    "command": [
                        normalize_command_item(record.root, item)
                        for item in spec.command
                    ],
                    "cwd": str(safe_child(record.root, spec.cwd or ".")),
                    "env": env,
                }
        return merged

    def plugin_data_dir(self, plugin_id: str) -> Path:
        return self.workspace / ".kirakira" / "plugin-data" / plugin_id

    # --- per-plugin 代际 ---

    def capture_contributions(self, record: ActivePlugin) -> PluginContributions:
        """冻结一个插件当前贡献的全部能力,作为它这一代的不可变内容。"""

        plugin = record.instance
        if plugin is None:
            return PluginContributions()
        services = self.managed_services.get(record.plugin_id, {})
        mcp = {
            name: spec
            for name, spec in self.mcp_servers.items()
            if any(
                declared.name == name for declared in plugin.mcp_servers()
            )
        }
        return PluginContributions(
            skill_roots=tuple(resolve_skill_roots(record.root, plugin.skill_roots())),
            drift_skill_roots=tuple(
                resolve_skill_roots(record.root, plugin.drift_skill_roots())
            ),
            mcp_servers=mcp,
            managed_services=dict(services),
            before_turn_modules=tuple(plugin.before_turn_modules()),
            before_reasoning_modules=tuple(plugin.before_reasoning_modules()),
            prompt_render_modules=tuple(plugin.prompt_render_modules()),
            before_step_modules=tuple(plugin.before_step_modules()),
            after_step_modules=tuple(plugin.after_step_modules()),
            after_reasoning_modules=tuple(plugin.after_reasoning_modules()),
            after_turn_modules=tuple(plugin.after_turn_modules()),
            tool_hooks=tuple(plugin.tool_hooks()),
            proactive_sources=tuple(
                RegisteredProactiveSource(plugin_id=record.plugin_id, spec=spec)
                for spec in plugin.proactive_sources()
            ),
            jobs=tuple(plugin.jobs()),
            channels=tuple(plugin.channels()),
        )

    async def build_generation(self, record: ActivePlugin) -> PluginGeneration:
        """为一个已装载插件编译候选代际,并跑语义检查决定是否准入。"""

        plugin = record.instance
        source_revision = _plugin_source_revision(record.root)
        config_revision = _plugin_config_revision(
            record.root, self.plugin_data_dir(record.plugin_id)
        )
        checks: List[PluginSemanticCheck] = []
        if plugin is not None:
            checks.extend(plugin.static_semantic_checks())
            readiness = PluginReadinessContext(
                workspace_tool_names=tuple(self.tool_registry.names())
                if self.tool_registry is not None
                else (),
                mcp_server_names=tuple(self.mcp_servers),
            )
            try:
                checks.extend(await plugin.readiness_semantic_checks(readiness))
            except Exception as exc:  # noqa: BLE001 - 检查失败本身就是一次失败结果
                checks.append(PluginSemanticCheck.fail("readiness_error", str(exc)))
        gate = GateResult.from_checks(
            plugin_id=record.plugin_id,
            candidate_revision="%s:%s" % (source_revision, config_revision),
            checks=tuple(checks),
        )
        return PluginGeneration(
            plugin_id=record.plugin_id,
            generation_id=compute_generation_id(
                plugin_id=record.plugin_id,
                source_revision=source_revision,
                config_revision=config_revision,
            ),
            module_path=type(plugin).__module__ if plugin is not None else "",
            source_revision=source_revision,
            config_revision=config_revision,
            instance=plugin,
            contributions=self.capture_contributions(record),
            gate_result=gate,
        )

    def _publish_generation_transaction(
        self, generation: PluginGeneration
    ) -> PluginGeneration | None:
        """Publish one admitted generation with a durable phase record."""

        current = self.generations.current(generation.plugin_id)
        tx_id = self.reload_journal.begin(
            plugin_id=generation.plugin_id,
            base_snapshot_id=current.generation_id if current is not None else None,
            generation_id=generation.generation_id,
            source_revision=generation.source_revision,
            config_revision=generation.config_revision,
        )
        try:
            self.reload_journal.advance(
                tx_id,
                "prepared",
                candidate_snapshot_id=generation.generation_id,
            )
            self.reload_journal.advance(tx_id, "validating")
            self.reload_journal.advance(tx_id, "commit_started")
            previous = self.generations.publish(generation)
            self.reload_journal.advance(tx_id, "committed")
            if previous is None:
                self.reload_journal.advance(tx_id, "complete")
            else:
                self.reload_journal.advance(tx_id, "draining")
                self._reload_drains[previous.generation_id] = tx_id
            return previous
        except BaseException as exc:
            phase = self.reload_journal.get(tx_id).phase
            if phase in {"preparing", "prepared", "validating", "commit_started"}:
                self.reload_journal.advance(
                    tx_id,
                    "aborted",
                    error=str(exc) or type(exc).__name__,
                )
            raise

    async def _finish_reload_drains(
        self, drained: tuple[PluginGeneration, ...]
    ) -> None:
        active_instances = {id(generation.instance) for generation in self.generations.active}
        for generation in drained:
            tx_id = self._reload_drains.pop(generation.generation_id, None)
            if tx_id is not None:
                record = self.reload_journal.get(tx_id)
                if record.phase == "draining":
                    self.reload_journal.advance(tx_id, "complete")
            if id(generation.instance) in active_instances:
                continue
            terminate = getattr(generation.instance, "terminate", None)
            if callable(terminate):
                result = terminate()
                if inspect.isawaitable(result):
                    await result

    def _request_reload(self) -> None:
        """请求 watcher 立即热重载。未接 watcher 时是空操作(退回轮询/重启)。"""
        hook = self.reload_hook
        if hook is None:
            return
        try:
            hook()
        except Exception:
            logger.exception("plugin reload hook failed")

    def watch_revision(self) -> str:
        """所有插件源码 + 配置 + manifest 的内容指纹,用于热重载变化检测。"""

        digest = hashlib.sha256()
        digest.update(_path_content_digest(self._manifest_path()))
        for root in discover_plugin_roots(self.plugin_dirs):
            digest.update(root.name.encode("utf-8"))
            digest.update(_plugin_source_revision(root).encode("ascii"))
            digest.update(
                _plugin_config_revision(root, self.plugin_data_dir(root.name)).encode(
                    "ascii"
                )
            )
        return digest.hexdigest()[:32]

    async def reconcile_changed(self) -> List[Dict[str, Any]]:
        """对 revision 变化的插件重建候选代际并换代。

        逐插件独立处理:某个插件 gate 未过或构建失败时保留它的旧代际,
        其余插件照常换代——半新状态只局限在单个插件,不污染整个运行时。
        """

        async with self._reconcile_lock:
            owns_publication = not self.generations.publication_in_progress
            if owns_publication:
                await self.generations.begin_publication()
            results: List[Dict[str, Any]] = []
            try:
                manifest = load_manifest(self._manifest_path())
                discovered = {
                    root.name: root for root in discover_plugin_roots(self.plugin_dirs)
                }
                active_ids = {record.plugin_id for record in self.active}
                desired = {
                    name for name in discovered if is_enabled(manifest, name)
                }

                for plugin_id in sorted(active_ids - desired):
                    await self._deactivate_plugin(plugin_id)
                    self._publication_dirty = True
                    results.append({"plugin_id": plugin_id, "state": "deactivated"})

                for plugin_id in sorted(desired - active_ids):
                    try:
                        activated = await self._activate_plugin(
                            discovered[plugin_id], manifest
                        )
                    except Exception as exc:  # noqa: BLE001
                        self.errors[plugin_id] = str(exc)
                        logger.exception("plugin activation failed: %s", plugin_id)
                        results.append(
                            {"plugin_id": plugin_id, "state": "activate_failed"}
                        )
                        continue
                    if activated is not None:
                        self._publication_dirty = True
                        results.append(
                            {"plugin_id": plugin_id, "state": "activated"}
                        )

                for record in list(self.active):
                    current = self.generations.current(record.plugin_id)
                    try:
                        candidate = await self.build_generation(record)
                    except Exception as exc:  # noqa: BLE001
                        self.errors[record.plugin_id] = str(exc)
                        logger.exception(
                            "plugin generation rebuild failed: %s", record.plugin_id
                        )
                        results.append(
                            {"plugin_id": record.plugin_id, "state": "build_failed"}
                        )
                        continue
                    if current is not None and candidate.revision == current.revision:
                        continue
                    if not candidate.gate_result.passed:
                        self.errors[record.plugin_id] = (
                            "semantic gate failed: %s"
                            % candidate.gate_result.failure_reason
                        )
                        logger.warning(
                            "plugin hot reload gate failed, keeping old generation: %s",
                            record.plugin_id,
                        )
                        results.append(
                            {"plugin_id": record.plugin_id, "state": "gate_failed"}
                        )
                        continue
                    previous = self._publish_generation_transaction(candidate)
                    self._publication_dirty = True
                    self.errors.pop(record.plugin_id, None)
                    results.append(
                        {
                            "plugin_id": record.plugin_id,
                            "state": "swapped",
                            "generation_id": candidate.generation_id,
                            "previous_generation_id": (
                                previous.generation_id if previous else None
                            ),
                            "previous_leases": previous.lease_count if previous else 0,
                        }
                    )

                if self._publication_dirty:
                    await self._republish_after_reconcile()
                    self._publication_dirty = False

                drained = self.generations.drain_quiescible()
                if drained:
                    await self._finish_reload_drains(drained)
                    results.append(
                        {
                            "state": "drained",
                            "generation_ids": [gen.generation_id for gen in drained],
                        }
                    )
                await self.generations.finish_publication()
                return results
            except BaseException:
                # A failed cross-surface publish stays admission-gated. The
                # watcher retries this same revision until republish succeeds.
                if not self._publication_dirty and owns_publication:
                    await self.generations.finish_publication()
                raise

    async def _republish_after_reconcile(self) -> None:
        """换代后把 skill / MCP / 托管服务重新发布到当前活跃集合。"""

        self._sync_skill_links()
        if self.skill_loader is not None:
            self.skill_loader.reload()
        if self.mcp_publisher is not None:
            await self.mcp_publisher.publish(self.mcp_servers, source="plugins")
        # 服务按新集合整批重启。失败上抛，admission gate 保持关闭并由 watcher 重试。
        await self.service_host.stop_all()
        self.service_host.bind_plugin_services(self.managed_services)
        await self.service_host.start_all()

    async def publish_generations(self) -> List[str]:
        """为所有已装载插件发布代际;gate 未过的插件保留旧代际并记录错误。"""

        published: List[str] = []
        for record in self.active:
            try:
                generation = await self.build_generation(record)
                if not generation.gate_result.passed:
                    self.errors[record.plugin_id] = (
                        "semantic gate failed: %s"
                        % generation.gate_result.failure_reason
                    )
                    logger.warning(
                        "plugin semantic gate failed, keeping previous generation: %s (%s)",
                        record.plugin_id,
                        generation.gate_result.failure_reason,
                    )
                    continue
                self._publish_generation_transaction(generation)
                published.append(generation.generation_id)
            except Exception as exc:  # noqa: BLE001 - 单插件代际失败不影响其余插件
                self.errors[record.plugin_id] = str(exc)
                logger.exception("plugin generation build failed: %s", record.plugin_id)
        return published

    @staticmethod
    def _validate_declarations(root: Path, plugin: Plugin) -> None:
        """在插件自己的加载边界内校验声明路径，越界立即失败。"""

        resolve_skill_roots(root, plugin.skill_roots())
        for spec in plugin.mcp_servers():
            if not spec.command:
                raise ValueError("plugin MCP server has no command: %s" % spec.name)
            safe_child(root, spec.cwd or ".")
            for item in spec.command:
                normalize_command_item(root, item)

    @property
    def channels(self) -> List[object]:
        channels: List[object] = []
        for plugin in self.instances:
            channels.extend(plugin.channels())
        return channels

    # --- 声明式扩展点:插件只声明,runtime 负责编译与生命周期 ---

    @property
    def proactive_sources(self) -> List[RegisteredProactiveSource]:
        """收集插件声明的主动数据源。编译成真实 source 由 proactive.mcp_sources 负责。"""

        sources: List[RegisteredProactiveSource] = []
        seen: set[str] = set()
        for record in self.active:
            if record.instance is None:
                continue
            for spec in record.instance.proactive_sources():
                if not isinstance(spec, ProactiveSourceSpec):
                    raise ValueError(
                        "插件 %s.proactive_sources 返回值不是 ProactiveSourceSpec"
                        % record.plugin_id
                    )
                registered = RegisteredProactiveSource(
                    plugin_id=record.plugin_id, spec=spec
                )
                key = proactive_source_key(registered)
                if key in seen:
                    logger.warning("duplicate plugin proactive source skipped: %s", key)
                    continue
                seen.add(key)
                sources.append(registered)
        return sources

    @property
    def managed_services(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """收集插件声明的长驻服务,规范化成 PluginServiceHost 需要的 binding。

        命令与 cwd 都按插件根解析并做越界校验,和 MCP 声明同一套安全边界。
        """

        merged: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for record in self.active:
            if record.instance is None:
                continue
            for spec in record.instance.managed_services():
                if not spec.command:
                    raise ValueError("plugin managed service has no command: %s" % spec.id)
                plugin_services = merged.setdefault(record.plugin_id, {})
                if spec.id in plugin_services:
                    logger.warning(
                        "duplicate plugin managed service skipped: %s:%s",
                        record.plugin_id,
                        spec.id,
                    )
                    continue
                env = dict(spec.env)
                env.setdefault(
                    "KIRAKIRA_PLUGIN_DATA_DIR",
                    str(self.plugin_data_dir(record.plugin_id)),
                )
                plugin_services[spec.id] = {
                    "command": [
                        normalize_command_item(record.root, item)
                        for item in spec.command
                    ],
                    "cwd": str(safe_child(record.root, spec.cwd or ".")),
                    "env": env,
                    "readiness_url": spec.readiness_url,
                    "startup_timeout_seconds": spec.startup_timeout_seconds,
                }
        return merged

    def register_jobs(self, host: PluginJobHost) -> List[str]:
        """把插件声明的作业注册进 host。host 拥有生命周期,插件不自己起 task。"""

        keys: List[str] = []
        for record in self.active:
            if record.instance is None:
                continue
            for spec in record.instance.jobs():
                if not isinstance(spec, PluginJobSpec):
                    raise ValueError(
                        "插件 %s.jobs 返回值不是 PluginJobSpec" % record.plugin_id
                    )
                keys.append(host.register(record.plugin_id, spec))
        return keys

    @property
    def drift_skill_roots(self) -> List[Path]:
        roots: List[Path] = []
        for record in self.active:
            if record.instance is None:
                continue
            roots.extend(resolve_skill_roots(record.root, record.instance.drift_skill_roots()))
        return roots

    async def semantic_checks(
        self,
        context: PluginReadinessContext | None = None,
    ) -> Dict[str, List[PluginSemanticCheck]]:
        """收集静态 + 就绪语义检查。失败项交由调用方决定是否降级/停用插件。"""

        readiness = context or PluginReadinessContext(
            workspace_tool_names=tuple(self.tool_registry.names())
            if self.tool_registry is not None
            else (),
        )
        report: Dict[str, List[PluginSemanticCheck]] = {}
        for record in self.active:
            if record.instance is None:
                continue
            checks = list(record.instance.static_semantic_checks())
            try:
                checks.extend(await record.instance.readiness_semantic_checks(readiness))
            except Exception as exc:  # noqa: BLE001 - 检查自身失败也是一次失败结果
                checks.append(
                    PluginSemanticCheck.fail("readiness_error", str(exc))
                )
            if checks:
                report[record.plugin_id] = checks
        return report

    async def _initialize_plugin(self, name: str, root: Path, plugin: Plugin) -> None:
        # 能力声明在这里就校验，坏插件在自己的 try 内失败，不牵连其他插件。
        if getattr(plugin, "api_version", None) != 2:
            raise ValueError(
                "plugin Runtime API version must be 2: %s" % getattr(plugin, "api_version", None)
            )
        self._validate_declarations(root, plugin)
        data_dir = self.plugin_data_dir(name)
        context = PluginContext(
            event_bus=self.event_bus,
            tool_registry=self.tool_registry,
            plugin_id=name,
            plugin_dir=root,
            data_dir=data_dir,
            kv_store=PluginKVStore(data_dir / "kv.json"),
            workspace=self.workspace,
            session_manager=self.session_manager,
            memory=self.memory,
            config=self._load_plugin_config(root, data_dir, plugin),
        )
        plugin.context = context  # type: ignore[attr-defined]
        before = set(self.tool_registry.names())
        pending_modules: Dict[str, List[tuple[int, object]]] = {}
        pending_hooks: List[DecoratedToolHook] = []
        try:
            plugin.register_tools(self.tool_registry)
            self._register_decorated(
                name, plugin, pending_modules=pending_modules, pending_hooks=pending_hooks
            )
            await plugin.prepare()
            plugin.activate()
        except Exception:
            for tool_name in set(self.tool_registry.names()) - before:
                self.tool_registry.unregister(tool_name)
            try:
                await plugin.terminate()
            except Exception:
                logger.exception("plugin rollback terminate failed: %s", name)
            raise
        self.instances.append(plugin)
        for phase, modules in pending_modules.items():
            self._decorated_modules.setdefault(phase, []).extend(modules)
        self._decorated_hooks.extend(pending_hooks)

    @staticmethod
    def _load_plugin_config(root: Path, data_dir: Path, plugin: Plugin) -> Any:
        merged: Dict[str, Any] = {}
        # 插件包只携带可分发默认值；用户私有配置由 plugin-data 拥有。
        # 根目录 config.local.toml 仅作旧插件兼容，plugin-data 始终最后覆盖。
        for path in (
            root / "config.toml",
            root / "config.local.toml",
            data_dir / "config.local.toml",
        ):
            payload = load_toml_config(path)
            merged.update(payload)
        config_model = getattr(plugin, "ConfigModel", None)
        if config_model is not None:
            return config_model(**merged)
        return merged

    def _register_decorated(
        self,
        plugin_name: str,
        plugin: Plugin,
        *,
        pending_modules: Dict[str, List[tuple[int, object]]],
        pending_hooks: List[DecoratedToolHook],
    ) -> None:
        seen: set[str] = set()
        for cls in type(plugin).mro():
            for attribute_name, raw_method in cls.__dict__.items():
                if attribute_name in seen:
                    continue
                bindings = get_bindings(raw_method)
                if not bindings:
                    continue
                seen.add(attribute_name)
                method = getattr(plugin, attribute_name)
                for binding in bindings:
                    if binding.kind == "phase":
                        pending_modules.setdefault(binding.phase, []).append(
                            (binding.priority, method)
                        )
                    elif binding.kind == "tool_hook":
                        pending_hooks.append(
                            DecoratedToolHook(
                                "%s.%s" % (plugin_name, attribute_name),
                                method,
                                binding.hook_tool_name,
                                binding.priority,
                            )
                        )
                    elif binding.kind == "tool":
                        self.tool_registry.register(
                            ToolSpec(
                                binding.tool_name,
                                binding.tool_description,
                                dict(binding.tool_schema or {}),
                            ),
                            self._decorated_tool_handler(method),
                            deferred=binding.deferred,
                            source_type="plugin",
                            source_name=plugin_name,
                        )

    def _decorated_tool_handler(self, method):
        parameters = list(inspect.signature(method).parameters)

        async def invoke(**kwargs: Any):
            if parameters and parameters[0] == "event":
                result = method(self.tool_registry.context, **kwargs)
            else:
                result = method(**kwargs)
            if inspect.isawaitable(result):
                result = await result
            return result

        return invoke

    def _sync_skill_links(self) -> None:
        skills_dir = self.workspace / "skills"
        expected: Dict[str, Path] = {}
        plugin_roots = {record.root.resolve() for record in self.active}
        for record in self.active:
            declared = record.instance.skill_roots() if record.instance else ()
            roots = list(resolve_skill_roots(record.root, declared))
            fallback = record.root / "skills"
            if not roots and fallback.is_dir():
                roots.append(fallback)
            for root in roots:
                candidates = [root] if (root / "SKILL.md").is_file() else list(root.iterdir())
                for skill in sorted(candidates):
                    if not skill.is_dir() or not (skill / "SKILL.md").is_file():
                        continue
                    expected.setdefault(skill.name, skill.resolve())
        if expected:
            skills_dir.mkdir(parents=True, exist_ok=True)
        for name, target in expected.items():
            link = skills_dir / name
            if link.is_symlink() and link.resolve() == target:
                continue
            if link.exists() or link.is_symlink():
                logger.warning("plugin skill path collision, keeping existing path: %s", link)
                continue
            link.symlink_to(target, target_is_directory=True)
        if not skills_dir.exists():
            return
        for link in skills_dir.iterdir():
            if not link.is_symlink() or link.name in expected:
                continue
            try:
                target = link.resolve(strict=False)
                managed = any(target == root or root in target.parents for root in plugin_roots)
            except OSError:
                managed = False
            if managed:
                link.unlink()

    def _load_one(self, path: Path) -> Plugin | None:
        module_name = "kirakira_plugin_%s" % path.parent.name.replace("-", "_")
        spec = importlib.util.spec_from_file_location(
            module_name,
            path,
            submodule_search_locations=[str(path.parent)],
        )
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)  # type: ignore[union-attr]
        factory = getattr(module, "create_plugin", None)
        if callable(factory):
            return factory()
        for value in module.__dict__.values():
            if isinstance(value, type) and issubclass(value, Plugin) and value is not Plugin:
                return value()
        return None

    def _register_management_tools(self) -> None:
        self.tool_registry.register(
            ToolSpec(
                "plugin_list",
                "List active plugins and plugin load errors.",
                object_schema({}, []),
            ),
            self.list_plugins,
            deferred=True,
        )
        self.tool_registry.register(
            ToolSpec(
                "plugin_doctor",
                "Validate installed plugin manifests, lifecycle entries, skills, and MCP declarations without executing them.",
                object_schema({"name": {"type": "string"}}, []),
            ),
            self.doctor,
            deferred=True,
        )
        self.tool_registry.register(
            ToolSpec(
                "plugin_install",
                "Install an Akashic-compatible plugin from a local directory or HTTPS Git repository. Applied by hot reload, no restart needed.",
                object_schema({"source": {"type": "string"}}, ["source"]),
            ),
            self.install,
            deferred=True,
        )
        self.tool_registry.register(
            ToolSpec(
                "plugin_enable",
                "Enable an installed plugin in the manifest. Applied by hot reload, no restart needed.",
                object_schema({"name": {"type": "string"}}, ["name"]),
            ),
            self.enable_plugin,
            deferred=True,
        )
        self.tool_registry.register(
            ToolSpec(
                "plugin_disable",
                "Disable an installed plugin in the manifest. Applied by hot reload, no restart needed.",
                object_schema({"name": {"type": "string"}}, ["name"]),
            ),
            self.disable_plugin,
            deferred=True,
        )
        self.tool_registry.register(
            ToolSpec(
                "plugin_uninstall",
                "Remove an installed plugin directory and its manifest entry. Plugin data is preserved. Applied by hot reload.",
                object_schema({"name": {"type": "string"}}, ["name"]),
            ),
            self.uninstall,
            deferred=True,
        )

    def _manifest_path(self) -> Path:
        return self.workspace / ".kirakira" / MANIFEST_NAME

    @staticmethod
    def _valid_plugin_id(plugin_id: str) -> bool:
        return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._@-]*", plugin_id))

    def _write_manifest(self, manifest: Dict[str, PluginEnablement]) -> None:
        path = self._manifest_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        lines: List[str] = []
        for plugin_id in sorted(manifest):
            # 插件 id 允许 . @ -，会破坏 TOML 裸键，统一用带引号的点号键段。
            lines.append("[plugins.%s]" % json.dumps(plugin_id))
            lines.append("enabled = %s" % ("true" if manifest[plugin_id].enabled else "false"))
            lines.append("")
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text("\n".join(lines), encoding="utf-8")
        os.replace(tmp, path)

    def _set_enabled(self, name: str, enabled: bool) -> str:
        name = name.strip()
        if not self._valid_plugin_id(name):
            return "Error: invalid plugin name: %r" % name
        manifest = load_manifest(self._manifest_path())
        manifest[name] = PluginEnablement(name, enabled)
        self._write_manifest(manifest)
        verb = "enabled" if enabled else "disabled"
        self._request_reload()
        return "Plugin %r %s in manifest. Hot reload will apply it shortly." % (name, verb)

    def enable_plugin(self, name: str) -> str:
        return self._set_enabled(name, True)

    def disable_plugin(self, name: str) -> str:
        return self._set_enabled(name, False)

    async def reconcile_disabled_and_drain(
        self, plugin_id: str, *, timeout: float = 30.0
    ) -> str:
        """停用插件并**等到它的代际真正排空**才返回。

        与 ``disable_plugin`` 的区别是后者只改清单就返回,在途 turn 可能还握着
        这个插件的租约。控制面 ``plugin/disable-and-drain`` 需要"返回即已下线"
        的强语义(照 Reference bootstrap/app.py:_disable_and_drain_plugin)。
        """
        plugin_id = plugin_id.strip()
        if not plugin_id:
            raise ValueError("缺少插件 ID")
        known = {record.plugin_id for record in self.active}
        if plugin_id not in known:
            raise ValueError("插件未加载: %s" % plugin_id)
        self.disable_plugin(plugin_id)
        # 控制面要求“返回即已下线”，不能只唤醒 watcher 后就返回。
        # 本次调用内直接 reconcile，完成 active record、MCP/skill/服务发布
        # 的收敛；在途代际则继续等待租约归零。
        await self.reconcile_changed()
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            self.generations.drain_quiescible()
            pending = {
                generation.plugin_id
                for generation in self.generations.retired
            }
            if plugin_id not in pending:
                return "插件已停用并排空: %s" % plugin_id
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    "插件 %s 仍有在途 turn 持有租约,排空超时" % plugin_id
                )
            await asyncio.sleep(0.05)

    async def uninstall(self, name: str) -> str:
        name = name.strip()
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
            return "Error: invalid plugin name: %r" % name
        target = self.workspace / ".kirakira" / "plugins" / name
        if not target.is_dir():
            return "Error: plugin %r is not installed" % name
        await asyncio.to_thread(shutil.rmtree, target)
        manifest = load_manifest(self._manifest_path())
        if name in manifest:
            del manifest[name]
            self._write_manifest(manifest)
        self._request_reload()
        return (
            "Uninstalled plugin %r. Data under .kirakira/plugin-data/%s is preserved. "
            "Hot reload will unload it shortly." % (name, name)
        )

    def list_plugins(self) -> str:
        return json.dumps(
            {
                "active": [
                    {
                        "name": record.plugin_id,
                        "root": str(record.root),
                        "version": record.version,
                        "desc": record.desc,
                        "lifecycle": record.instance is not None,
                        "skills": len(
                            resolve_skill_roots(
                                record.root,
                                record.instance.skill_roots() if record.instance else (),
                            )
                        ),
                        "mcp_servers": sorted(
                            spec.name
                            for spec in (
                                record.instance.mcp_servers() if record.instance else []
                            )
                        ),
                    }
                    for record in self.active
                ],
                "errors": dict(self.errors),
            },
            ensure_ascii=False,
            indent=2,
        )

    def doctor(self, name: str = "") -> str:
        """检查已发现插件的结构，以及已加载插件用代码声明的能力。"""

        loaded = {record.root: record for record in self.active}
        reports = []
        for root in discover_plugin_roots(self.plugin_dirs):
            record = loaded.get(root)
            plugin_name = record.plugin_id if record else root.name
            if name and name not in (plugin_name, root.name):
                continue
            errors: List[str] = []
            warnings: List[str] = []
            if not (root / "plugin.py").is_file():
                errors.append("plugin.py is missing")
            if root.name in self.errors:
                errors.append(self.errors[root.name])
            # 能力声明只有在插件已加载时才可信；未加载的插件不在这里执行其代码。
            if record is None or record.instance is None:
                warnings.append("plugin is not loaded; capability checks skipped")
            else:
                errors.extend(self._check_declared_skills(record, warnings))
                errors.extend(self._check_declared_mcp(record))
            reports.append(
                {
                    "name": plugin_name,
                    "root": str(root),
                    "ok": not errors,
                    "errors": errors,
                    "warnings": warnings,
                }
            )
        return json.dumps(
            {"plugins": reports, "phases": self.phase_report()},
            ensure_ascii=False,
            indent=2,
        )

    def phase_report(self) -> Dict[str, str]:
        """各相位的实际执行顺序与依赖关系,用于排查"插件为什么没按预期顺序跑"。"""

        report: Dict[str, str] = {}
        for phase in (
            "before_turn",
            "before_reasoning",
            "prompt_render",
            "before_step",
            "after_step",
            "after_reasoning",
            "after_turn",
        ):
            modules = self._collect("%s_modules" % phase)
            if not modules:
                continue
            if all(getattr(module, "slot", None) for module in modules):
                try:
                    report[phase] = inspect_phase(modules)
                except RuntimeError as error:
                    report[phase] = "slot ordering failed: %s" % error
            else:
                report[phase] = "注册顺序(未全员声明 slot): %s" % ", ".join(
                    str(getattr(module, "slot", type(module).__name__))
                    for module in modules
                )
        return report

    def _check_declared_skills(
        self, record: ActivePlugin, warnings: List[str]
    ) -> List[str]:
        assert record.instance is not None
        try:
            roots = resolve_skill_roots(record.root, record.instance.skill_roots())
        except ValueError as exc:
            return [str(exc)]
        for skill_root in roots:
            candidates = (
                [skill_root]
                if (skill_root / "SKILL.md").is_file()
                else [item for item in skill_root.iterdir() if item.is_dir()]
            )
            for candidate in candidates:
                if not (candidate / "SKILL.md").is_file():
                    warnings.append("skill has no SKILL.md: %s" % candidate)
        return []

    def _check_declared_mcp(self, record: ActivePlugin) -> List[str]:
        assert record.instance is not None
        errors: List[str] = []
        for spec in record.instance.mcp_servers():
            if not spec.command:
                errors.append("MCP server has no command: %s" % spec.name)
            try:
                safe_child(record.root, spec.cwd or ".")
            except ValueError as exc:
                errors.append(str(exc))
        return errors

    async def install(self, source: str) -> str:
        source = source.strip()
        if not source:
            return "Error: plugin source is empty"
        install_root = self.workspace / ".kirakira" / "plugins"
        install_root.mkdir(parents=True, exist_ok=True)
        staging = install_root / (".install-%s" % uuid4().hex)
        try:
            local = Path(source).expanduser()
            if local.is_dir():
                await asyncio.to_thread(shutil.copytree, local.resolve(), staging)
            else:
                if not source.startswith("https://"):
                    return "Error: remote plugin source must be an HTTPS Git URL"
                process = await asyncio.create_subprocess_exec(
                    "git",
                    "clone",
                    "--depth",
                    "1",
                    "--",
                    source,
                    str(staging),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )
                try:
                    output, _ = await asyncio.wait_for(process.communicate(), timeout=120)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
                    return "Error: plugin clone timed out"
                if process.returncode:
                    return "Error: plugin clone failed: %s" % output.decode(
                        "utf-8", errors="replace"
                    )[-2000:]
            # 插件身份来自来源目录名：安装期不导入 plugin.py，绝不热执行刚下载的代码。
            if not (staging / "plugin.py").is_file():
                return "Error: plugin must contain plugin.py at its root"
            plugin_name = _source_plugin_name(source)
            if not re.fullmatch(r"[A-Za-z0-9_.-]+", plugin_name):
                return "Error: invalid plugin name: %s" % plugin_name
            target = install_root / plugin_name
            upgrading = target.exists()
            git_dir = staging / ".git"
            if git_dir.exists():
                await asyncio.to_thread(shutil.rmtree, git_dir)
            if upgrading:
                # 升级:旧目录先挪走再原子换入,失败可回滚,不会留下半个插件。
                backup = install_root / (".backup-%s-%s" % (plugin_name, uuid4().hex))
                os.replace(target, backup)
                try:
                    os.replace(staging, target)
                except BaseException:
                    os.replace(backup, target)
                    raise
                await asyncio.to_thread(shutil.rmtree, backup, True)
            else:
                os.replace(staging, target)
            self._request_reload()
            return (
                "%s plugin %r at %s. Hot reload will apply it shortly."
                % ("Upgraded" if upgrading else "Installed", plugin_name, target)
            )
        finally:
            if staging.exists():
                await asyncio.to_thread(shutil.rmtree, staging, True)
