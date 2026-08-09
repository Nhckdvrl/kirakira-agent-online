"""DefaultMemoryEngine 契约测试。

对照 Reference `tests/test_memory_engine_contract.py` 的核心行为子集,
用 mock 的 retriever/memorizer/worker 单独验证引擎的 query/mutate/ingest 语义,
不依赖真实 store / embedder / provider。事件总线接线在 Stage 2 运行时集成里验证。
"""

from __future__ import annotations

import asyncio
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

from plugins.default_memory.engine import DefaultMemoryEngine
from plugins.default_memory.config import DefaultMemoryConfig
from core.memory.engine import (
    EngineProfile,
    MemoryCapability,
    MemoryIngestRequest,
    MemoryMutation,
    MemoryQuery,
    MemoryQueryFilters,
    MemoryScope,
)


def _make_default_engine(
    *,
    retriever: Any = None,
    memorizer: Any = None,
    post_response_worker: Any = None,
) -> DefaultMemoryEngine:
    engine = DefaultMemoryEngine.__new__(DefaultMemoryEngine)
    engine._config = SimpleNamespace(model="lm")
    engine._default_config = DefaultMemoryConfig()
    engine._workspace = Path(".")
    engine._provider = None
    engine._light_provider = None
    engine._light_model = ""
    engine._v2_store = None
    engine._embedder = None
    engine._memorizer = memorizer
    engine._retriever = retriever
    engine._tagger = None
    engine._post_response_worker = post_response_worker
    engine._event_bus = None
    engine.closeables = []
    engine._event_wired = False
    engine._wire_memory2_events()
    return engine


class EngineContractTests(unittest.TestCase):
    def test_context_query_maps_hits_and_text_block(self) -> None:
        async def scenario() -> None:
            retriever = SimpleNamespace(
                retrieve=AsyncMock(
                    return_value=[
                        {
                            "id": "m1",
                            "summary": "记住用户偏好中文回复",
                            "score": 0.88,
                            "source_ref": "cli:1@seed",
                            "memory_type": "preference",
                            "extra_json": {"origin": "test"},
                        }
                    ]
                ),
                build_injection_block=lambda items: ("注入块", ["m1"]),
            )
            engine = _make_default_engine(retriever=cast(Any, retriever))

            result = await engine.query(
                MemoryQuery(
                    text="中文回复",
                    intent="context",
                    scope=MemoryScope(channel="cli", chat_id="1"),
                    filters=MemoryQueryFilters(
                        kinds=("preference",),
                        hints={"require_scope_match": True},
                    ),
                    limit=3,
                )
            )

            self.assertEqual(result.text_block, "注入块")
            self.assertEqual(len(result.records), 1)
            self.assertEqual(result.records[0].id, "m1")
            self.assertTrue(result.records[0].injected)
            self.assertEqual(result.records[0].engine_kind, "default")
            self.assertEqual(result.records[0].kind, "preference")
            self.assertEqual(
                result.trace["profile"], EngineProfile.RICH_MEMORY_ENGINE.value
            )

        asyncio.run(scenario())

    def test_interest_query_preserves_read_only_effect(self) -> None:
        async def scenario() -> None:
            retriever = SimpleNamespace(
                retrieve=AsyncMock(
                    return_value=[
                        {
                            "id": "p1",
                            "summary": "用户偏好中文回复",
                            "score": 0.8,
                            "source_ref": "telegram:1@seed",
                            "memory_type": "preference",
                            "extra_json": {},
                        }
                    ]
                ),
                build_injection_block=lambda items: ("", []),
            )
            engine = _make_default_engine(retriever=cast(Any, retriever))

            result = await engine.query(
                MemoryQuery(
                    text="中文回复",
                    intent="interest",
                    effect="read_only",
                    scope=MemoryScope(session_key="telegram:1"),
                    limit=2,
                )
            )

            self.assertEqual(result.trace["intent"], "interest")
            self.assertEqual(result.trace["effect"], "read_only")
            self.assertEqual(result.records[0].id, "p1")
            retriever.retrieve.assert_awaited_once()

        asyncio.run(scenario())

    def test_remember_uses_memorizer(self) -> None:
        async def scenario() -> None:
            memorizer = SimpleNamespace(
                save_item_with_supersede=AsyncMock(return_value="new:memu-1")
            )
            engine = _make_default_engine(
                retriever=cast(Any, SimpleNamespace()),
                memorizer=cast(Any, memorizer),
            )

            result = await engine.mutate(
                MemoryMutation(
                    kind="remember",
                    summary="以后用中文回复",
                    memory_kind="preference",
                    scope=MemoryScope(session_key="cli:1", channel="cli", chat_id="1"),
                )
            )

            self.assertEqual(result.item_id, "memu-1")
            self.assertEqual(result.status, "new")
            memorizer.save_item_with_supersede.assert_awaited_once()
            self.assertEqual(
                memorizer.save_item_with_supersede.await_args.kwargs["extra"],
                {
                    "tool_requirement": None,
                    "steps": [],
                    "scope_channel": "cli",
                    "scope_chat_id": "1",
                },
            )

        asyncio.run(scenario())

    def test_ingest_delegates_to_post_worker(self) -> None:
        async def scenario() -> None:
            worker = SimpleNamespace(run=AsyncMock())
            engine = _make_default_engine(
                retriever=cast(Any, SimpleNamespace()),
                post_response_worker=cast(Any, worker),
            )

            result = await engine.ingest(
                MemoryIngestRequest(
                    content={
                        "user_message": "以后用中文",
                        "assistant_response": "好的",
                        "tool_chain": [{"text": "memo", "calls": []}],
                    },
                    source_kind="conversation_turn",
                    scope=MemoryScope(session_key="cli:1"),
                )
            )

            self.assertTrue(result.accepted)
            self.assertEqual(result.raw["engine"], "default")
            worker.run.assert_awaited_once()

        asyncio.run(scenario())

    def test_descriptor_keeps_messages_capability_only(self) -> None:
        descriptor = DefaultMemoryEngine.DESCRIPTOR
        self.assertEqual(descriptor.profile, EngineProfile.RICH_MEMORY_ENGINE)
        self.assertIn(MemoryCapability.INGEST_MESSAGES, descriptor.capabilities)
        self.assertNotIn(MemoryCapability.INGEST_TEXT, descriptor.capabilities)


if __name__ == "__main__":
    unittest.main()
