"""记忆子系统的依赖注入缝(Phase 2)。

对照 Reference `agent/looping/ports.py:MemoryServices` —— runtime 只认识这个薄服务包,
不认识具体引擎实现,换引擎不再连锁改调用点。工厂对照 Reference `bootstrap/memory.py`:
memory 启用且配了 embedding 就用 DefaultMemoryEngine,否则退化成 DisabledMemoryEngine
(语义检索关闭,不发无谓的失败 embedding 请求;配好 [memory.embedding] 后自动切回)。
"""

from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agent.config_models import Config, build_config
from core.net.http import SharedHttpResources
from infra.providers.model_client_adapter import LLMProvider
from plugins.default_memory.engine import DefaultMemoryEngine
from plugins.default_memory.config import DefaultMemoryConfig
from core.memory.engine import MemoryEngine
from core.memory.markdown import (
    MemoryLifecycleBindRequest,
    build_markdown_memory_runtime,
)
from core.memory.plugin import DisabledMemoryEngine

logger = logging.getLogger(__name__)


@dataclass
class MemoryServices:
    """runtime 消费的记忆服务包。只暴露引擎接口,不暴露实现。

    `store` 是引擎拥有的 MemoryStore2。暴露它不是为了让业务代码绕过引擎,而是让
    Dashboard 与过渡期的旧 MemoryRuntime **共享同一个 SQLite 连接**——否则同一个
    coremem.db 会被打开两次,产生锁竞争与不一致视图。引擎未承重时为 None。
    """

    engine: MemoryEngine | None = None
    store: Any = None
    # 四文件 Markdown 长期档案 + consolidation 维护(Reference 里与 engine 并列)。
    markdown: Any = None

    async def aclose(self) -> None:
        """关闭引擎持有的资源(store / embedder / 事件订阅)。

        照 Reference `core/memory/runtime.py:MemoryRuntime.aclose`:逆序释放,
        单个 closeable 失败不掩盖其余,首个异常在全部尝试后抛出。
        """
        closeables = list(getattr(self.engine, "closeables", []) or [])
        first_error: BaseException | None = None
        for closeable in reversed(closeables):
            try:
                if hasattr(closeable, "aclose"):
                    result = closeable.aclose()
                    if inspect.isawaitable(result):
                        await result
                elif hasattr(closeable, "close"):
                    closeable.close()
            except Exception as exc:  # noqa: BLE001 - 关停期不因单个资源中断
                if first_error is None:
                    first_error = exc
                logger.warning(
                    "memory closeable shutdown failed for %s: %s",
                    type(closeable).__name__,
                    exc,
                )
        if first_error is not None:
            raise first_error


def memory_engine_enabled(config: Config) -> bool:
    """DefaultMemoryEngine 只在 memory 启用且配了 embedding 端点时启用。"""
    return bool(config.memory.enabled and config.memory.embedding.base_url)


def resolve_memory_plugin(name: str) -> Any:
    """按名字解析记忆引擎插件(照 Reference `bootstrap/wiring.resolve_memory_plugin`)。

    这条路由此前是断的——插件名从未被读,等于 `MemoryPlugin` 协议
    是个没人走的缝。补上之后 akasha 才谈得上"可选中"。未知名字 fail loud:
    配错引擎名应当立刻可见,而不是静默回落到 default 让人以为在用另一套。
    """
    key = (name or "").strip() or "default"
    if key == "default":
        from plugins.default_memory.plugin import MemoryPlugin as DefaultPlugin

        return DefaultPlugin()
    if key == "akasha":
        from plugins.akasha import MemoryPlugin as AkashaPlugin

        return AkashaPlugin()
    raise ValueError(
        "未知记忆引擎: %r(可选 default / akasha)" % name
    )


def build_memory_engine(
    *,
    app_config: dict[str, Any],
    workspace: Path,
    provider: LLMProvider,
    light_provider: LLMProvider | None = None,
    http_resources: SharedHttpResources | None = None,
    event_publisher: Any = None,
    markdown: Any = None,
) -> MemoryEngine:
    """构造记忆引擎。

    门控与路由是两件事:先看**是否承重**(memory.enabled + 配了 embedding),
    再看**用哪套引擎**(`[memory].plugin`)。前者不成立时一律 DisabledMemoryEngine,
    因为两套引擎读写都要向量。
    """
    config = build_config(app_config)
    if not memory_engine_enabled(config):
        return DisabledMemoryEngine()
    runtime = build_memory_plugin_runtime(
        app_config=app_config,
        workspace=workspace,
        provider=provider,
        light_provider=light_provider,
        http_resources=http_resources,
        event_publisher=event_publisher,
        markdown=markdown,
    )
    return runtime.engine


def build_memory_plugin_runtime(
    *,
    app_config: dict[str, Any],
    workspace: Path,
    provider: LLMProvider,
    light_provider: LLMProvider | None = None,
    http_resources: SharedHttpResources | None = None,
    event_publisher: Any = None,
    markdown: Any = None,
) -> Any:
    """统一插件构造入口(照 Reference `bootstrap/memory.py:_build_memory_plugin_runtime`)。"""
    from core.memory.plugin import MemoryPluginBuildDeps

    config = build_config(app_config)
    plugin = resolve_memory_plugin(config.memory.plugin)
    return plugin.build(
        MemoryPluginBuildDeps(
            config=config,
            workspace=workspace,
            provider=provider,
            light_provider=light_provider,
            http_resources=http_resources or SharedHttpResources(),
            event_publisher=event_publisher,
            markdown=markdown,
        )
    )


def memory_keep_count(memory_window: int) -> int:
    """照 Reference `bootstrap/memory.py:_memory_keep_count`:向下取偶,至少 2。"""
    return max(2, ((max(1, memory_window) + 1) // 2) * 2)


def build_memory_services(
    *,
    app_config: dict[str, Any],
    workspace: Path,
    provider: LLMProvider,
    light_provider: LLMProvider | None = None,
    http_resources: SharedHttpResources | None = None,
    event_publisher: Any = None,
    session_manager: Any = None,
    memory_window: int = 40,
) -> MemoryServices:
    config = build_config(app_config)
    # 顺序刻意:markdown 先建。它与 engine 并列(订阅 TurnCommitted 做 consolidation,
    # 提交后发 ConsolidationCommitted,引擎靠这个事件做长期事实提取),而
    # `MemoryPluginBuildDeps` 要求把它传给插件——所以不能等 engine 建完再建。
    markdown = build_markdown_memory_runtime(
        workspace=workspace,
        provider=provider,
        model=config.model,
        keep_count=memory_keep_count(memory_window),
        # 订阅 TurnCommitted 驱动归档;runtime 侧的旧 consolidation 调用已在有维护器时关闭,
        # 因此不会重复归档。见 decisions/0002。
        event_bus=event_publisher,
        recent_context_provider=light_provider or provider,
        recent_context_model=config.light_model or config.model,
    )
    if session_manager is not None:
        # 维护队列需要读写 Session 才能推进 consolidation 游标。
        markdown.maintenance.bind_lifecycle(
            MemoryLifecycleBindRequest(
                get_session=session_manager.get_or_create,
                save_session=session_manager.save_async,
            )
        )
    engine = build_memory_engine(
        app_config=app_config,
        workspace=workspace,
        provider=provider,
        light_provider=light_provider,
        http_resources=http_resources,
        event_publisher=event_publisher,
        markdown=markdown,
    )
    # 引擎是记忆库的唯一 owner;把它的 store 一并暴露,过渡期消费者共享同一连接。
    # akasha 没有 `_v2_store`(它有自己的 AkashaStore),此时 store 为 None——
    # Dashboard 会因此走 engine 的 admin 协议,正是我们想要的。
    return MemoryServices(
        engine=engine,
        store=getattr(engine, "_v2_store", None),
        markdown=markdown,
    )
