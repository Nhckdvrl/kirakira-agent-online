"""Poll workspace MCP declarations and publish content changes.

watcher 只负责“内容变了没有”和“什么时候重试”，真正的换代原子性由
`McpCatalogPublisher` 拥有。声明非法或 server 连不上时不换代，旧 MCP 继续服务，修好
文件后下一轮轮询自动恢复。
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Dict

from agent.mcp.declarations import (
    declarations_input_revision,
    load_workspace_mcp_declarations,
)
from agent.mcp.publisher import McpCatalogPublisher

logger = logging.getLogger(__name__)


class WorkspaceMcpWatcher:
    """轮询 workspace MCP 声明并串行发布内容变化。"""

    def __init__(
        self,
        declarations_dir: Path,
        publisher: McpCatalogPublisher,
        *,
        mcp_root: Path | None = None,
        interval_seconds: float = 1.0,
    ) -> None:
        self._declarations_dir = declarations_dir
        self._publisher = publisher
        self._mcp_root = mcp_root or declarations_dir.parent
        self._interval_seconds = interval_seconds
        self._active_revision: str | None = None
        self._running = True
        self._wake = asyncio.Event()
        self._forced = False
        self._last_input_revision: str | None = None
        self._stopped = asyncio.Event()
        self._reconcile_lock = asyncio.Lock()
        self.last_error: str | None = None

    async def reconcile(self) -> bool:
        """发布变化后的完整声明；失败时调用方获得原始异常。"""

        async with self._reconcile_lock:
            desired = load_workspace_mcp_declarations(
                self._declarations_dir,
                mcp_root=self._mcp_root,
            )
            if desired.revision == self._active_revision:
                return False
            await self._publisher.publish(desired.specs)
            self._active_revision = desired.revision
            self.last_error = None
            return True

    def status(self) -> Dict[str, Any]:
        """返回当前已发布 workspace MCP 代际的只读状态。"""

        return {
            **self._publisher.status(),
            "revision": self._active_revision,
            "lastError": self.last_error,
        }

    async def run(self) -> None:
        """持续检查内容 revision，并允许失败版本在修复后恢复。"""

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
                    input_revision = declarations_input_revision(
                        self._declarations_dir,
                        mcp_root=self._mcp_root,
                    )
                except OSError as error:
                    self.last_error = str(error)
                    logger.error("workspace MCP declaration scan failed: %s", error)
                    continue
                # 内容指纹没变就不重连；forced 用于写入声明后立即发布。
                if not forced and input_revision == self._last_input_revision:
                    continue
                self._last_input_revision = input_revision
                try:
                    await self.reconcile()
                except (OSError, ValueError, RuntimeError) as error:
                    self.last_error = str(error)
                    logger.error("workspace MCP hot reload failed: %s", error)
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
        await self._publisher.shutdown()
