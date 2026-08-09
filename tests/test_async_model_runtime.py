"""Phase 1 async-native model runtime 契约。

验证 ModelClientProvider.chat 优先走异步原生 acomplete,同步 stub 客户端回退到
to_thread(complete);两条路都不经过对方。这样 memory/proactive/drift 的 LLM 调用
在真实客户端上是异步原生的,而老式同步 stub 仍可用。
"""

from __future__ import annotations

import asyncio
import unittest

from infra.providers.model_client_adapter import ModelClientProvider
from infra.providers.llm_provider import OpenAICompatibleClient
from agent.model_runtime.types import ContentSafetyError, ContextLengthError
from core.schema import ModelResponse


class _AsyncClient:
    """异步原生客户端:只有 acomplete,没有同步 complete。"""

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def acomplete(self, messages, tools, system, model, max_tokens):
        self.calls.append((tuple(messages), system, model, max_tokens))
        return ModelResponse(text="async-ok", reasoning_content="think")


class _SyncClient:
    """老式同步 stub:只有 complete。"""

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def complete(self, messages, tools, system, model, max_tokens):
        self.calls.append((tuple(messages), system, model, max_tokens))
        return ModelResponse(text="sync-ok")


class AsyncProviderContractTests(unittest.TestCase):
    def test_provider_prefers_async_acomplete(self) -> None:
        async def scenario() -> None:
            client = _AsyncClient()
            provider = ModelClientProvider(client)
            resp = await provider.chat(
                messages=[
                    {"role": "system", "content": "sys"},
                    {"role": "user", "content": "hi"},
                ],
                tools=[],
                model="m",
                max_tokens=64,
            )
            self.assertEqual(resp.content, "async-ok")
            self.assertEqual(resp.thinking, "think")
            self.assertEqual(len(client.calls), 1)
            # system 被拆出、非 system 消息保留
            sent_messages, sent_system, sent_model, _ = client.calls[0]
            self.assertEqual(sent_system, "sys")
            self.assertEqual(sent_model, "m")
            self.assertEqual(sent_messages, ({"role": "user", "content": "hi"},))

        asyncio.run(scenario())

    def test_provider_falls_back_to_sync_complete(self) -> None:
        async def scenario() -> None:
            client = _SyncClient()
            provider = ModelClientProvider(client)
            resp = await provider.chat(
                messages=[{"role": "user", "content": "hi"}],
                tools=[],
                model="m",
                max_tokens=64,
            )
            self.assertEqual(resp.content, "sync-ok")
            self.assertEqual(len(client.calls), 1)

        asyncio.run(scenario())


class HttpErrorClassificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = OpenAICompatibleClient(base_url="http://x/v1", api_key="k")

    def test_context_length_error(self) -> None:
        err = self.client._http_error_for(400, "maximum context length exceeded")
        self.assertIsInstance(err, ContextLengthError)

    def test_content_safety_error(self) -> None:
        err = self.client._http_error_for(400, "content_filter triggered")
        self.assertIsInstance(err, ContentSafetyError)

    def test_retryable_returns_none(self) -> None:
        self.assertIsNone(self.client._http_error_for(429, "rate limited"))
        self.assertIsNone(self.client._http_error_for(503, "unavailable"))

    def test_other_status_is_runtime_error(self) -> None:
        err = self.client._http_error_for(404, "not found")
        self.assertIsInstance(err, RuntimeError)


class SharedStreamParseTests(unittest.TestCase):
    """complete_stream 与 acomplete_stream 共用的 SSE 解析核心。"""

    def setUp(self) -> None:
        self.client = OpenAICompatibleClient(base_url="http://x/v1", api_key="k")

    def _run(self, lines: list[str]):
        state = self.client._new_stream_state()
        deltas: list[str] = []
        for line in lines:
            if not self.client._consume_stream_line(
                line, state, lambda ct, rs: deltas.append(ct)
            ):
                break
        return self.client._finalize_stream(state), deltas

    def test_content_stream_accumulates_and_stops_on_done(self) -> None:
        resp, deltas = self._run(
            [
                'data: {"choices":[{"delta":{"content":"Hel"}}]}',
                ':heartbeat comment ignored',
                'data: {"choices":[{"delta":{"content":"lo"},"finish_reason":"stop"}]}',
                "data: [DONE]",
                'data: {"choices":[{"delta":{"content":"IGNORED_AFTER_DONE"}}]}',
            ]
        )
        self.assertEqual(resp.text, "Hello")
        self.assertEqual(deltas, ["Hel", "lo"])
        self.assertEqual(resp.stop_reason, "end_turn")

    def test_tool_calls_accumulate_across_chunks(self) -> None:
        resp, _ = self._run(
            [
                'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"sea","arguments":"{\\"q\\":"}}]}}]}',
                'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"name":"rch","arguments":"1}"}}]},"finish_reason":"tool_calls"}]}',
                "data: [DONE]",
            ]
        )
        self.assertEqual(resp.stop_reason, "tool_use")
        self.assertEqual(len(resp.tool_calls), 1)
        self.assertEqual(resp.tool_calls[0].id, "c1")
        self.assertEqual(resp.tool_calls[0].name, "search")
        self.assertEqual(resp.tool_calls[0].arguments, {"q": 1})


if __name__ == "__main__":
    unittest.main()
