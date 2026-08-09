"""Recoverable light-runtime fallback to the main model client."""

from __future__ import annotations

import logging
from typing import Any, Callable

from agent.model_runtime.types import RetryableModelError
from core.schema import JsonDict, ModelResponse, ToolSpec

logger = logging.getLogger(__name__)
_RECOVERABLE = (RetryableModelError, TimeoutError)


class ResilientModelClient:
    """Use a light client first and fall back only before visible output exists."""

    def __init__(
        self,
        *,
        primary: Any,
        primary_model: str,
        fallback: Any,
        fallback_model: str,
        primary_runtime_id: str = "light",
    ) -> None:
        self.primary = primary
        self.primary_model = primary_model
        self.fallback = fallback
        self.fallback_model = fallback_model
        self.primary_runtime_id = primary_runtime_id

    async def acomplete(
        self,
        messages: list[JsonDict],
        tools: list[ToolSpec],
        system: str,
        model: str,
        max_tokens: int,
        tool_choice: Any = "auto",
    ) -> ModelResponse:
        del model
        try:
            return await self.primary.acomplete(
                messages,
                tools,
                system,
                self.primary_model,
                max_tokens,
                tool_choice,
            )
        except _RECOVERABLE as exc:
            self._log_fallback(exc)
            return await self.fallback.acomplete(
                messages,
                tools,
                system,
                self.fallback_model,
                max_tokens,
                tool_choice,
            )

    def complete(
        self,
        messages: list[JsonDict],
        tools: list[ToolSpec],
        system: str,
        model: str,
        max_tokens: int,
        tool_choice: Any = "auto",
    ) -> ModelResponse:
        del model
        try:
            return self.primary.complete(
                messages, tools, system, self.primary_model, max_tokens, tool_choice
            )
        except _RECOVERABLE as exc:
            self._log_fallback(exc)
            return self.fallback.complete(
                messages, tools, system, self.fallback_model, max_tokens, tool_choice
            )

    async def acomplete_stream(
        self,
        messages: list[JsonDict],
        tools: list[ToolSpec],
        system: str,
        model: str,
        max_tokens: int,
        on_delta: Callable[[str, str], None] | None = None,
        tool_choice: Any = "auto",
    ) -> ModelResponse:
        del model
        emitted = False

        def track(content: str, reasoning: str) -> None:
            nonlocal emitted
            emitted = emitted or bool(content or reasoning)
            if on_delta is not None:
                on_delta(content, reasoning)

        try:
            return await self.primary.acomplete_stream(
                messages,
                tools,
                system,
                self.primary_model,
                max_tokens,
                track,
                tool_choice,
            )
        except _RECOVERABLE as exc:
            if emitted:
                raise
            self._log_fallback(exc)
            return await self.fallback.acomplete_stream(
                messages,
                tools,
                system,
                self.fallback_model,
                max_tokens,
                on_delta,
                tool_choice,
            )

    def complete_stream(
        self,
        messages: list[JsonDict],
        tools: list[ToolSpec],
        system: str,
        model: str,
        max_tokens: int,
        on_delta: Callable[[str, str], None] | None = None,
        tool_choice: Any = "auto",
    ) -> ModelResponse:
        del model
        emitted = False

        def track(content: str, reasoning: str) -> None:
            nonlocal emitted
            emitted = emitted or bool(content or reasoning)
            if on_delta is not None:
                on_delta(content, reasoning)

        try:
            return self.primary.complete_stream(
                messages,
                tools,
                system,
                self.primary_model,
                max_tokens,
                track,
                tool_choice,
            )
        except _RECOVERABLE as exc:
            if emitted:
                raise
            self._log_fallback(exc)
            return self.fallback.complete_stream(
                messages,
                tools,
                system,
                self.fallback_model,
                max_tokens,
                on_delta,
                tool_choice,
            )

    def _log_fallback(self, exc: BaseException) -> None:
        logger.warning(
            "light runtime fallback primary_runtime=%s primary_model=%s "
            "error=%s fallback_model=%s",
            self.primary_runtime_id,
            self.primary_model,
            type(exc).__name__,
            self.fallback_model,
        )
