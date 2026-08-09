"""默认记忆引擎的 MemoryPlugin 包装(照 Reference `plugins/default_memory/memory_plugin.py`)。

引擎路由补上之后,default 与 akasha 必须是**同一种形状**才能被同一个 `resolve_memory_plugin`
选中——否则路由就退化成一个 if/else,协议还是没人走。所以默认引擎也包成 MemoryPlugin。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from plugins.default_memory.engine import DefaultMemoryEngine
from plugins.default_memory.config import DefaultMemoryConfig
from core.memory.plugin import MemoryPluginBuildDeps, MemoryPluginRuntime


class MemoryPlugin:
    plugin_id = "default"

    def ensure_workspace_storage(
        self,
        *,
        config: Any,
        workspace: Path,
    ) -> list[tuple[Path, bool]]:
        """默认引擎的存储在 `DefaultMemoryEngine` 构造时按需建;这里只报告路径。"""
        _ = config
        db_path = Path(workspace) / "memory" / "coremem.db"
        return [(db_path, db_path.exists())]

    def build(self, deps: MemoryPluginBuildDeps) -> MemoryPluginRuntime:
        engine = DefaultMemoryEngine(
            config=deps.config,
            default_config=DefaultMemoryConfig(),
            workspace=deps.workspace,
            provider=deps.provider,
            light_provider=deps.light_provider,
            http_resources=deps.http_resources,
            event_publisher=deps.event_publisher,
        )
        return MemoryPluginRuntime(
            engine=engine,
            closeables=list(getattr(engine, "closeables", []) or []),
            admin=engine,
            embedding_api=getattr(engine, "embedding_api", None),
        )
