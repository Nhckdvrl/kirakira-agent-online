"""记忆服务关停契约:引擎持有的资源必须被释放。

之前 DefaultMemoryEngine 的 closeables(SQLite store / embedder / 事件订阅)
从未被关闭,只靠进程退出兜底——这在长跑进程与测试里都是真实泄漏。
"""

from __future__ import annotations

import asyncio
import unittest

from core.memory.services import MemoryServices


class _Closeable:
    def __init__(self, name: str, sink: list[str], *, boom: bool = False) -> None:
        self.name = name
        self._sink = sink
        self._boom = boom

    def close(self) -> None:
        if self._boom:
            raise RuntimeError("close failed: %s" % self.name)
        self._sink.append(self.name)


class _AsyncCloseable:
    def __init__(self, name: str, sink: list[str]) -> None:
        self.name = name
        self._sink = sink

    async def aclose(self) -> None:
        self._sink.append(self.name)


class _Engine:
    def __init__(self, closeables: list[object]) -> None:
        self.closeables = closeables


class MemoryServicesShutdownTests(unittest.TestCase):
    def test_closeables_released_in_reverse_order(self) -> None:
        async def scenario() -> None:
            closed: list[str] = []
            engine = _Engine(
                [_Closeable("store", closed), _AsyncCloseable("embedder", closed)]
            )
            await MemoryServices(engine=engine).aclose()
            # 逆序释放:后建的先关
            self.assertEqual(closed, ["embedder", "store"])

        asyncio.run(scenario())

    def test_one_failure_does_not_block_the_rest_and_is_raised(self) -> None:
        async def scenario() -> None:
            closed: list[str] = []
            engine = _Engine(
                [
                    _Closeable("store", closed),
                    _Closeable("bad", closed, boom=True),
                    _AsyncCloseable("embedder", closed),
                ]
            )
            with self.assertRaises(RuntimeError):
                await MemoryServices(engine=engine).aclose()
            # 失败的那个跳过,其余仍被关闭
            self.assertEqual(closed, ["embedder", "store"])

        asyncio.run(scenario())

    def test_no_engine_or_no_closeables_is_noop(self) -> None:
        async def scenario() -> None:
            await MemoryServices().aclose()
            await MemoryServices(engine=_Engine([])).aclose()

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
