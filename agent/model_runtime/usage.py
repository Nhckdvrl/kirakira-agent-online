"""Provider-neutral model usage accounting.

Every request is counted, including requests whose provider omits usage data.  This
keeps a numeric total from being mistaken for complete telemetry.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Mapping


class UsageCoverage(StrEnum):
    EXACT = "exact"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class ModelUsage:
    input_tokens: int | None = None
    cached_input_tokens: int | None = None
    output_tokens: int | None = None
    reasoning_output_tokens: int | None = None
    request_count: int = 1
    covered_request_count: int = 0
    coverage: UsageCoverage = UsageCoverage.UNAVAILABLE

    def to_dict(self) -> dict[str, int | str | None]:
        return {
            "input_tokens": self.input_tokens,
            "cached_input_tokens": self.cached_input_tokens,
            "output_tokens": self.output_tokens,
            "reasoning_output_tokens": self.reasoning_output_tokens,
            "request_count": self.request_count,
            "covered_request_count": self.covered_request_count,
            "coverage": self.coverage.value,
        }


def usage_from_mapping(raw: Mapping[str, Any] | None) -> ModelUsage:
    """Normalize Chat Completions/DeepSeek usage without inventing unknown data."""

    value = raw or {}
    input_tokens = _optional_int(value.get("prompt_tokens", value.get("input_tokens")))
    output_tokens = _optional_int(
        value.get("completion_tokens", value.get("output_tokens"))
    )
    prompt_details = _mapping(value.get("prompt_tokens_details"))
    output_details = _mapping(
        value.get("completion_tokens_details", value.get("output_tokens_details"))
    )
    cached = _optional_int(prompt_details.get("cached_tokens"))
    if cached is None:
        cached = _optional_int(
            value.get("prompt_cache_hit_tokens", value.get("cached_input_tokens"))
        )
    reasoning = _optional_int(
        output_details.get("reasoning_tokens", value.get("reasoning_output_tokens"))
    )
    exact = input_tokens is not None and output_tokens is not None
    partial = any(
        item is not None for item in (input_tokens, cached, output_tokens, reasoning)
    )
    return ModelUsage(
        input_tokens=input_tokens,
        cached_input_tokens=cached,
        output_tokens=output_tokens,
        reasoning_output_tokens=reasoning,
        covered_request_count=1 if exact else 0,
        coverage=(
            UsageCoverage.EXACT
            if exact
            else UsageCoverage.PARTIAL
            if partial
            else UsageCoverage.UNAVAILABLE
        ),
    )


def aggregate_usage(items: list[ModelUsage]) -> ModelUsage:
    """Aggregate requests while preserving the difference between zero and unknown."""

    def total(field: str) -> int | None:
        known = [getattr(item, field) for item in items if getattr(item, field) is not None]
        return sum(known) if known else None

    if not items:
        return ModelUsage(request_count=0)
    request_count = sum(item.request_count for item in items)
    covered = sum(item.covered_request_count for item in items)
    coverage = (
        UsageCoverage.UNAVAILABLE
        if not any(item.coverage is not UsageCoverage.UNAVAILABLE for item in items)
        else UsageCoverage.EXACT
        if covered == request_count
        and all(item.coverage is UsageCoverage.EXACT for item in items)
        else UsageCoverage.PARTIAL
    )
    return ModelUsage(
        input_tokens=total("input_tokens"),
        cached_input_tokens=total("cached_input_tokens"),
        output_tokens=total("output_tokens"),
        reasoning_output_tokens=total("reasoning_output_tokens"),
        request_count=request_count,
        covered_request_count=covered,
        coverage=coverage,
    )


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _optional_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return result if result >= 0 else None
