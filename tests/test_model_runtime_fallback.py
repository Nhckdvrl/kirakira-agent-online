"""Light model fallback boundaries."""

from __future__ import annotations

import asyncio
import unittest

from agent.model_runtime.fallback import ResilientModelClient
from agent.model_runtime.types import ContextLengthError, RetryableModelError
from core.schema import ModelResponse


class _Client:
    def __init__(self, result: ModelResponse | BaseException, *, delta: str = "") -> None:
        self.result = result
        self.delta = delta
        self.models: list[str] = []

    async def acomplete(self, messages, tools, system, model, max_tokens, tool_choice="auto"):
        self.models.append(model)
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result

    async def acomplete_stream(
        self,
        messages,
        tools,
        system,
        model,
        max_tokens,
        on_delta=None,
        tool_choice="auto",
    ):
        self.models.append(model)
        if self.delta and on_delta is not None:
            on_delta(self.delta, "")
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


class ResilientModelClientTests(unittest.TestCase):
    def test_recoverable_light_failure_falls_back_to_main(self) -> None:
        async def scenario():
            light = _Client(RetryableModelError("temporary"))
            main = _Client(ModelResponse(text="main result"))
            client = ResilientModelClient(
                primary=light,
                primary_model="light-model",
                fallback=main,
                fallback_model="main-model",
            )

            response = await client.acomplete([], [], "", "ignored", 100)

            self.assertEqual(response.text, "main result")
            self.assertEqual(light.models, ["light-model"])
            self.assertEqual(main.models, ["main-model"])

        asyncio.run(scenario())

    def test_semantic_context_failure_does_not_fallback(self) -> None:
        async def scenario():
            light = _Client(ContextLengthError("too long"))
            main = _Client(ModelResponse(text="must not run"))
            client = ResilientModelClient(
                primary=light,
                primary_model="light-model",
                fallback=main,
                fallback_model="main-model",
            )

            with self.assertRaises(ContextLengthError):
                await client.acomplete([], [], "", "ignored", 100)
            self.assertEqual(main.models, [])

        asyncio.run(scenario())

    def test_stream_does_not_fallback_after_visible_delta(self) -> None:
        async def scenario():
            light = _Client(RetryableModelError("stream broke"), delta="visible")
            main = _Client(ModelResponse(text="duplicate"))
            client = ResilientModelClient(
                primary=light,
                primary_model="light-model",
                fallback=main,
                fallback_model="main-model",
            )
            deltas: list[str] = []

            with self.assertRaises(RetryableModelError):
                await client.acomplete_stream(
                    [], [], "", "ignored", 100, lambda content, _reasoning: deltas.append(content)
                )

            self.assertEqual(deltas, ["visible"])
            self.assertEqual(main.models, [])

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
