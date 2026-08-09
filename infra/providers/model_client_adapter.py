"""Adapter from Kirakira's model client to the memory-provider protocol."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Protocol

from agent.model_runtime.types import ModelClient


@dataclass
class LLMResponse:
    content: str = ""
    thinking: str = ""
    tool_calls: list[object] = field(default_factory=list)
    usage: dict[str, object] = field(default_factory=dict)


class LLMProvider(Protocol):
    async def chat(
        self,
        *,
        messages: list[dict[str, Any]],
        tools: list[object],
        model: str,
        max_tokens: int,
        **kwargs: Any,
    ) -> LLMResponse: ...


class ModelClientProvider:
    def __init__(self, client: ModelClient) -> None:
        self._client = client

    async def chat(
        self,
        *,
        messages: list[dict[str, Any]],
        tools: list[object],
        model: str,
        max_tokens: int,
        **_kwargs: Any,
    ) -> LLMResponse:
        system_parts = [
            str(item.get("content") or "")
            for item in messages
            if item.get("role") == "system"
        ]
        request_messages = [
            dict(item) for item in messages if item.get("role") != "system"
        ]
        system = "\n\n".join(system_parts)
        # 优先走异步原生 acomplete；同步 stub 客户端回退到 to_thread(complete)。
        acomplete = getattr(self._client, "acomplete", None)
        if callable(acomplete):
            response = await acomplete(request_messages, [], system, model, max_tokens)
        else:
            response = await asyncio.to_thread(
                self._client.complete,
                request_messages,
                [],
                system,
                model,
                max_tokens,
            )
        return LLMResponse(
            content=response.text,
            thinking=response.reasoning_content,
            tool_calls=list(response.tool_calls),
            usage=dict(response.usage),
        )
