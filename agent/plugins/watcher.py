"""插件热重载 watcher(照 Reference `agent/plugins/watcher.py`,形状对齐本仓 MCP watcher)。

轮询插件源码/配置/manifest 的内容指纹,变化后触发一次 `reconcile_changed()`。
换代本身的安全性由 per-plugin 代际保证:gate 未过保留旧代际,旧代际要等在途租约
归零才 quiesce,所以 watcher 只负责"发现变化并触发",不自己判断能不能换。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class PluginWatcher:
    def __init__(
        self,
        manager: Any,
        *,
        interval_seconds: float = 1.0,
    ) -> None:
        self._manager = manager
        self._interval_seconds = interval_seconds
        self._running = True
        self._wake = asyncio.Event()
        self._forced = False
        self._last_revision: str | None = None
        self._stopped = asyncio.Event()
        self.last_error: str | None = None
        self.last_results: List[Dict[str, Any]] = []

    async def reconcile(self) -> List[Dict[str, Any]]:
        results = await self._manager.reconcile_changed()
        self.last_results = results
        self.last_error = None
        return results

    def status(self) -> Dict[str, Any]:
        generations = getattr(self._manager, "generations", None)
        active = getattr(generations, "active", ()) if generations else ()
        retired = getattr(generations, "retired", ()) if generations else ()
        return {
            "revision": self._last_revision,
            "lastError": self.last_error,
            "activeGenerations": {
                gen.plugin_id: gen.generation_id for gen in active
            },
            "retiredPending": [
                {
                    "pluginId": gen.plugin_id,
                    "generationId": gen.generation_id,
                    "leases": gen.lease_count,
                }
                for gen in retired
            ],
        }

    async def run(self) -> None:
        try:
            while self._running:
                try:
                    await asyncio.wait_for(
                        self._wake.wait(), timeout=self._interval_seconds
                    )
                except (asyncio.TimeoutError, TimeoutError):
                    pass
                self._wake.clear()
                if not self._running:
                    break
                forced = self._forced
                self._forced = False
                try:
                    revision = self._manager.watch_revision()
                except OSError as error:
                    # 安装器原子替换目录时可能扫到瞬时缺口;下一轮自然恢复。
                    self.last_error = str(error)
                    logger.error("plugin watch revision scan failed: %s", error)
                    continue
                # 照 Reference:第一次成功扫描一律触发一次 reconcile,建立基线。
                # 启动时扫描失败、之后恢复的情况也走这条路径,不会漏掉恢复后的换代。
                if self._last_revision is None:
                    forced = True
                if not forced and revision == self._last_revision:
                    continue
                try:
                    await self.reconcile()
                except (OSError, ValueError, RuntimeError) as error:
                    self.last_error = str(error)
                    logger.error("plugin hot reload failed: %s", error)
                    # 不接受失败 revision 为新基线；同一文件版本下一轮必须重试。
                    continue
                self._last_revision = revision
        finally:
            self._stopped.set()

    def wake(self) -> None:
        self._forced = True
        self._wake.set()

    def stop(self) -> None:
        self._running = False
        self._wake.set()

    async def wait_stopped(self) -> None:
        await self._stopped.wait()

    async def shutdown(self) -> None:
        self.stop()
        await self.wait_stopped()
