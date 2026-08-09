"""插件托管服务 host(照 Reference `agent/plugins/service_host.py` 移植,MVP 深度)。

插件用 `ManagedServiceSpec` **声明**长驻子进程,由 host 负责拉起、就绪探测、关停与
启动失败回滚。插件不自己 spawn 进程,所以卸载/换代时进程能被确定性回收。

与 Reference 的差异:本轮未移植 `swap_plugin_services` 的按代际热替换(依赖 per-plugin
generation,属于后续 snapshot 工作);其余启动/就绪/回滚/关停语义保持一致。
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Tuple

logger = logging.getLogger(__name__)


@dataclass
class _RunningService:
    spec: Dict[str, Any]
    process: asyncio.subprocess.Process


def _url_ready(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=1):
            return True
    except OSError:
        return False


def _signal_process(process: asyncio.subprocess.Process, sig: signal.Signals) -> None:
    try:
        if os.name == "nt":
            if sig == signal.SIGTERM:
                process.terminate()
            else:
                process.kill()
        else:
            os.killpg(process.pid, sig)
    except ProcessLookupError:
        pass


class PluginServiceHost:
    """持有所有插件声明的长驻服务进程。"""

    def __init__(self) -> None:
        self._bindings: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._running: Dict[Tuple[str, str], _RunningService] = {}

    def bind_plugin_services(
        self,
        services: Dict[str, Dict[str, Dict[str, Any]]],
    ) -> None:
        self._bindings = {
            plugin_id: dict(plugin_services)
            for plugin_id, plugin_services in services.items()
        }

    @property
    def running_keys(self) -> Tuple[Tuple[str, str], ...]:
        return tuple(self._running)

    async def start_all(self) -> None:
        """整批启动;任一失败则回滚已启动的服务,不留半启动状态。"""
        started: list[Tuple[str, str]] = []
        try:
            for plugin_id, services in sorted(self._bindings.items()):
                for service_id, spec in sorted(services.items()):
                    await self._start(plugin_id, service_id, spec)
                    started.append((plugin_id, service_id))
        except BaseException as start_error:
            rollback_errors: list[str] = []
            for key in reversed(started):
                try:
                    await self._stop(*key)
                except (asyncio.CancelledError, Exception) as error:
                    rollback_errors.append("%s:%s: %s" % (key[0], key[1], error))
            if rollback_errors:
                raise start_error from RuntimeError(
                    "managed service 启动回滚失败: " + "; ".join(rollback_errors)
                )
            raise

    async def stop_all(self) -> None:
        errors: list[str] = []
        cancellation: asyncio.CancelledError | None = None
        for plugin_id, service_id in reversed(tuple(self._running)):
            try:
                await self._stop(plugin_id, service_id)
            except asyncio.CancelledError as error:
                if cancellation is None:
                    cancellation = error
            except Exception as error:
                errors.append("%s:%s: %s" % (plugin_id, service_id, error))
        if cancellation is not None:
            raise cancellation
        if errors:
            raise RuntimeError("managed service 停止失败: " + "; ".join(errors))

    async def _start(
        self,
        plugin_id: str,
        service_id: str,
        spec: Dict[str, Any],
    ) -> None:
        key = (plugin_id, service_id)
        if key in self._running:
            raise RuntimeError("managed service 已运行: %s:%s" % (plugin_id, service_id))
        readiness_url = str(spec.get("readiness_url") or "")
        # 端口已被占用说明有残留进程或冲突,直接失败,不接管别人的服务。
        if readiness_url and await asyncio.to_thread(_url_ready, readiness_url):
            raise RuntimeError(
                "managed service readiness endpoint 已被占用: %s" % readiness_url
            )
        process = await asyncio.create_subprocess_exec(
            *spec["command"],
            cwd=spec.get("cwd") or ".",
            env={**os.environ, **dict(spec.get("env") or {})},
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            start_new_session=os.name != "nt",
        )
        running = _RunningService(spec=dict(spec), process=process)
        self._running[key] = running
        try:
            await self._wait_ready(running)
        except BaseException:
            await self._stop(plugin_id, service_id)
            raise

    async def _wait_ready(self, service: _RunningService) -> None:
        timeout = float(service.spec.get("startup_timeout_seconds") or 15)
        deadline = asyncio.get_running_loop().time() + timeout
        readiness_url = str(service.spec.get("readiness_url") or "")
        if not readiness_url:
            # 没有就绪探针时,只确认进程没有立刻退出。
            try:
                exit_code = await asyncio.wait_for(
                    asyncio.shield(service.process.wait()),
                    timeout=min(0.2, timeout),
                )
            except (TimeoutError, asyncio.TimeoutError):
                return
            raise RuntimeError("managed service 启动失败: exit=%s" % exit_code)
        while asyncio.get_running_loop().time() < deadline:
            if service.process.returncode is not None:
                raise RuntimeError(
                    "managed service 启动失败: exit=%s" % service.process.returncode
                )
            if await asyncio.to_thread(_url_ready, readiness_url):
                await asyncio.sleep(0)
                if service.process.returncode is not None:
                    raise RuntimeError(
                        "managed service 启动失败: exit=%s" % service.process.returncode
                    )
                return
            await asyncio.sleep(0.1)
        raise RuntimeError("managed service 启动超时")

    async def _stop(self, plugin_id: str, service_id: str) -> None:
        running = self._running.get((plugin_id, service_id))
        if running is None:
            return
        key = (plugin_id, service_id)

        async def reap() -> None:
            process = running.process
            try:
                if process.returncode is None:
                    _signal_process(process, signal.SIGTERM)
                    try:
                        await asyncio.wait_for(process.wait(), timeout=5)
                    except (TimeoutError, asyncio.TimeoutError):
                        _signal_process(process, signal.SIGKILL)
                        await process.wait()
            finally:
                self._running.pop(key, None)

        # shield 保证即使调用方被取消,子进程也会被回收干净,不留孤儿。
        task = asyncio.create_task(
            reap(), name="stop_service:%s:%s" % (plugin_id, service_id)
        )
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise
