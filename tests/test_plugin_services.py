"""插件托管服务 host 契约(照 Reference agent/plugins/service_host.py)。

用真实子进程验证:启动/关停/启动失败回滚/重复启动拒绝。
不依赖网络,就绪探针场景用"进程是否立刻退出"这条路径覆盖。
"""

from __future__ import annotations

import asyncio
import sys
import unittest

from agent.plugins.service_host import PluginServiceHost


def _sleep_spec(seconds: float = 30) -> dict:
    return {
        "command": [sys.executable, "-c", f"import time; time.sleep({seconds})"],
        "cwd": ".",
        "env": {},
        "readiness_url": "",
        "startup_timeout_seconds": 5,
    }


def _instant_exit_spec() -> dict:
    return {
        "command": [sys.executable, "-c", "raise SystemExit(3)"],
        "cwd": ".",
        "env": {},
        "readiness_url": "",
        "startup_timeout_seconds": 5,
    }


class PluginServiceHostTests(unittest.TestCase):
    def test_start_and_stop_all(self) -> None:
        async def scenario() -> None:
            host = PluginServiceHost()
            host.bind_plugin_services({"demo": {"svc": _sleep_spec()}})
            await host.start_all()
            self.assertEqual(host.running_keys, (("demo", "svc"),))
            await host.stop_all()
            self.assertEqual(host.running_keys, ())

        asyncio.run(scenario())

    def test_service_that_exits_immediately_fails_start(self) -> None:
        async def scenario() -> None:
            host = PluginServiceHost()
            host.bind_plugin_services({"demo": {"svc": _instant_exit_spec()}})
            with self.assertRaises(RuntimeError):
                await host.start_all()
            # 启动失败后不留运行记录
            self.assertEqual(host.running_keys, ())

        asyncio.run(scenario())

    def test_failed_start_rolls_back_earlier_services(self) -> None:
        async def scenario() -> None:
            host = PluginServiceHost()
            # a 正常长驻,b 立刻退出 → 整批失败,a 必须被回滚停掉
            host.bind_plugin_services(
                {"demo": {"a_ok": _sleep_spec(), "b_bad": _instant_exit_spec()}}
            )
            with self.assertRaises(RuntimeError):
                await host.start_all()
            self.assertEqual(host.running_keys, ())

        asyncio.run(scenario())

    def test_stop_all_is_idempotent(self) -> None:
        async def scenario() -> None:
            host = PluginServiceHost()
            host.bind_plugin_services({"demo": {"svc": _sleep_spec()}})
            await host.start_all()
            await host.stop_all()
            await host.stop_all()
            self.assertEqual(host.running_keys, ())

        asyncio.run(scenario())

    def test_empty_bindings_start_and_stop_cleanly(self) -> None:
        async def scenario() -> None:
            host = PluginServiceHost()
            host.bind_plugin_services({})
            await host.start_all()
            await host.stop_all()
            self.assertEqual(host.running_keys, ())

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
