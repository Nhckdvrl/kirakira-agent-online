"""Derive context window settings from a model's advertised capacity.

历史窗口与输出预留不再手写常量，而是按 1M 基准等比例缩放：小上下文模型自动收紧
历史条数，大上下文模型自动放宽，配置里只需要写模型真实的 context_window。
"""

from __future__ import annotations

import math
import json
from dataclasses import dataclass
from typing import Any

_REFERENCE_CONTEXT_WINDOW = 1_000_000
_REFERENCE_EFFECTIVE_CONTEXT_PERCENT = 0.9
_REFERENCE_MEMORY_WINDOW = 160
_REFERENCE_OUTPUT_RESERVE = 32_768
_MIN_MEMORY_WINDOW = 20
_MIN_OUTPUT_RESERVE = 4_096


@dataclass(frozen=True)
class ContextWindowSettings:
    memory_window: int
    output_reserve: int


def recommended_context_settings(
    context_window: int,
    effective_context_percent: float = _REFERENCE_EFFECTIVE_CONTEXT_PERCENT,
) -> ContextWindowSettings:
    """按 1M 基准等比例计算历史窗口与输出预留。"""

    # 1. 在配置边界拒绝无效模型容量。
    if context_window <= 0:
        raise ValueError("context_window must be greater than 0")
    if not 0 < effective_context_percent <= 1:
        raise ValueError("effective_context_percent must be within (0, 1]")

    effective_context = context_window * effective_context_percent
    reference_effective_context = (
        _REFERENCE_CONTEXT_WINDOW * _REFERENCE_EFFECTIVE_CONTEXT_PERCENT
    )

    # 2. 历史条数按四条对齐，避免不同上下文档位产生细碎配置。
    scaled_memory = round(
        effective_context * _REFERENCE_MEMORY_WINDOW / reference_effective_context
    )
    memory_window = max(_MIN_MEMORY_WINDOW, ((scaled_memory + 2) // 4) * 4)

    # 3. 输出预留按 1024 tokens 向下对齐，且不超过 1M 基准值。
    scaled_output = int(
        effective_context * _REFERENCE_OUTPUT_RESERVE / reference_effective_context
    )
    output_reserve = max(
        _MIN_OUTPUT_RESERVE,
        min(_REFERENCE_OUTPUT_RESERVE, scaled_output // 1024 * 1024),
    )
    return ContextWindowSettings(memory_window, output_reserve)


@dataclass(frozen=True)
class ContextBudget:
    effective_context: int
    input_budget: int
    reserved_output: int


def build_runtime_context_budget(
    context_window: int,
    effective_context_percent: float,
    max_output_tokens: int,
) -> ContextBudget:
    """按 runtime 实际配置计算统一输入预算。"""

    if context_window <= 0:
        raise ValueError("context_window must be greater than 0")
    if not 0 < effective_context_percent <= 1:
        raise ValueError("effective_context_percent must be within (0, 1]")
    effective = math.floor(context_window * effective_context_percent)
    output = max_output_tokens
    if output <= 0:
        raise ValueError("max_output_tokens must be greater than 0")
    if output >= effective:
        raise ValueError("max_output_tokens must be smaller than effective context")
    return ContextBudget(effective, effective - output, output)


def estimate_context_tokens(
    messages: list[dict[str, Any]],
    tools: list[Any],
    *,
    system_prompt: str = "",
) -> int:
    """Conservative provider-facing estimate including schemas and image blocks."""

    text_chars = len(system_prompt)
    schemas = []
    for tool in tools:
        if hasattr(tool, "name"):
            schemas.append(
                {
                    "name": getattr(tool, "name", ""),
                    "description": getattr(tool, "description", ""),
                    "parameters": getattr(tool, "input_schema", {}),
                }
            )
        else:
            schemas.append(tool)
    text_chars += len(json.dumps(schemas, ensure_ascii=False, default=str))
    image_tokens = 0
    for message in messages:
        content = message.get("content")
        if isinstance(content, list):
            for block in content:
                if isinstance(block, dict) and block.get("type") in {
                    "image_url",
                    "input_image",
                }:
                    detail = block.get("detail")
                    image = block.get("image_url")
                    if isinstance(image, dict):
                        detail = image.get("detail", detail)
                    image_tokens += 1024 if detail == "low" else 8192
                else:
                    text_chars += len(
                        json.dumps(block, ensure_ascii=False, default=str)
                    )
        elif content is not None:
            text_chars += len(str(content))
        text_chars += len(
            json.dumps(
                {key: value for key, value in message.items() if key != "content"},
                ensure_ascii=False,
                default=str,
            )
        )
    return max(1, text_chars // 3 + image_tokens)
