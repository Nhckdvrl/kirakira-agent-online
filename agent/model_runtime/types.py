"""Kirakira Agent learning harness module."""

from typing import Any, List, Protocol

from core.schema import JsonDict, ModelResponse, ToolSpec


class ModelRequestError(RuntimeError):
    """Base class for provider errors with runtime handling semantics."""


class ContextLengthError(ModelRequestError):
    pass


class ContentSafetyError(ModelRequestError):
    pass


class RetryableModelError(ModelRequestError):
    """Transport/rate-limit/server failure safe for a fresh provider attempt."""


class ModelClient(Protocol):
    def complete(
        self,
        messages: List[JsonDict],
        tools: List[ToolSpec],
        system: str,
        model: str,
        max_tokens: int,
        tool_choice: Any = "auto",
    ) -> ModelResponse:
        """tool_choice 照 Reference provider.chat:"auto" / "required" /
        {"type":"function","function":{"name":...}}。调用方只在需要强制时才传,
        因此仅实现旧签名的测试替身在不使用该能力时依然兼容。"""
        ...
