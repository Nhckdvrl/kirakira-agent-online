"""DI 服务包契约(照 Reference agent/looping/ports.py)。

要点是 Reference 的那个区分:**配置是值,服务是有生命周期的对象**,分开放。
pipeline 只依赖服务包这一层,替换实现时不必改调用点。
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace

from core.memory.services import memory_keep_count
from agent.looping.ports import (
    ContextServices,
    LLMConfig,
    LLMServices,
    MemoryConfig,
    MemoryServices,
    SessionServices,
)
from agent.core.runtime import PassiveTurnPipeline, RuntimeConfig


class ConfigTests(unittest.TestCase):
    def test_keep_count_matches_the_memory_package_formula(self) -> None:
        # 两处公式必须一致,否则上下文携带条数与归档保留条数会对不上
        for window in (1, 5, 40, 41, 100):
            self.assertEqual(
                MemoryConfig(window=window).keep_count, memory_keep_count(window)
            )

    def test_keep_count_is_even_and_at_least_two(self) -> None:
        for window in (0, 1, 2, 3):
            value = MemoryConfig(window=window).keep_count
            self.assertGreaterEqual(value, 2)
            self.assertEqual(value % 2, 0)

    def test_llm_config_is_plain_values(self) -> None:
        cfg = LLMConfig(model="m", light_model="l")
        self.assertEqual(cfg.model, "m")
        self.assertEqual(cfg.light_model, "l")


class ServiceGroupTests(unittest.TestCase):
    def test_light_client_falls_back_to_main(self) -> None:
        main = SimpleNamespace(name="main")
        self.assertIs(LLMServices(client=main).light, main)

    def test_explicit_light_client_wins(self) -> None:
        main = SimpleNamespace(name="main")
        light = SimpleNamespace(name="light")
        self.assertIs(LLMServices(client=main, light_client=light).light, light)

    def test_session_services_hold_objects(self) -> None:
        manager = SimpleNamespace(name="sm")
        services = SessionServices(transcript_store=manager)
        self.assertIs(services.session_manager, manager)
        self.assertIsNone(services.presence)

    def test_context_services_default_to_engine_retrieval(self) -> None:
        # retrieval_pipeline 留空 = 检索走记忆引擎
        services = ContextServices(context=SimpleNamespace())
        self.assertIsNone(services.retrieval_pipeline)


class PipelineConsumesServicePacksTests(unittest.TestCase):
    def _pipeline(self, **kwargs) -> PassiveTurnPipeline:
        return PassiveTurnPipeline(
            bus=SimpleNamespace(),
            event_bus=SimpleNamespace(),
            memory=SimpleNamespace(),
            tools=SimpleNamespace(),
            reasoner=SimpleNamespace(),
            config=RuntimeConfig(model="m"),
            **kwargs,
        )

    def test_session_manager_comes_from_the_service_pack(self) -> None:
        manager = SimpleNamespace(name="injected")
        pipeline = self._pipeline(
            session_manager=SimpleNamespace(name="ignored"),
            session_services=SessionServices(transcript_store=manager),
        )
        self.assertIs(pipeline.session_manager, manager)

    def test_bare_session_manager_is_wrapped_for_minimal_construction(self) -> None:
        manager = SimpleNamespace(name="bare")
        pipeline = self._pipeline(session_manager=manager)
        # 未注入服务包时用具体对象兜底,但对外仍是服务包这一层
        self.assertIsInstance(pipeline.session_services, SessionServices)
        self.assertIs(pipeline.session_manager, manager)

    def test_memory_services_are_optional(self) -> None:
        pipeline = self._pipeline(session_manager=SimpleNamespace())
        self.assertIsNone(pipeline.memory_services)
        pipeline = self._pipeline(
            session_manager=SimpleNamespace(), memory_services=MemoryServices()
        )
        self.assertIsInstance(pipeline.memory_services, MemoryServices)


if __name__ == "__main__":
    unittest.main()
