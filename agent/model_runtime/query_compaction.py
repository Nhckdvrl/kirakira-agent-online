"""Query-local ReAct compaction with non-destructive persistence metadata.

The persisted payload and replay pair follow Reference
``agent/model_runtime/query_compaction.py``. Kirakira keeps its provider adapter
small, so the compactor receives an async summary callback from the reasoner.
"""

from __future__ import annotations

import hashlib
import json
import math
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal, cast

from agent.model_runtime.execution_history import active_shell_execution_origins


COMPACTION_SCHEMA_VERSION = 1
COMPACTION_TOOL_NAME = "context_compact"
CompactionTrigger = Literal["soft_limit", "context_overflow"]

SUMMARY_PROMPT = """更新当前长任务的上下文压缩摘要。

摘要会替代已经完成的旧工具步骤，供同一个任务后续继续执行。只记录输入中已经出现的事实，不补充猜测，不把计划写成已完成。

必须使用以下标题：
## Goal
## Constraints
## Progress
## Key facts and references
## Decisions
## Validation
## Unfinished work
## Next steps

保留文件路径、符号、命令、错误、数值和验证结果。省略重复探索、无用日志、tool_call_id 和其他协议细节。只输出摘要正文。"""


class ContextCompactionError(RuntimeError):
    pass


@dataclass(frozen=True)
class ReactCompaction:
    summary: str
    compacted_tool_groups: int
    generation: int
    trigger: CompactionTrigger
    context_window: int
    soft_limit_tokens: int
    estimated_tokens_before: int
    estimated_tokens_after: int

    def to_payload(self) -> dict[str, object]:
        return {
            "schema_version": COMPACTION_SCHEMA_VERSION,
            "summary": self.summary,
            "compacted_tool_groups": self.compacted_tool_groups,
            "generation": self.generation,
            "trigger": self.trigger,
            "context_window": self.context_window,
            "soft_limit_tokens": self.soft_limit_tokens,
            "estimated_tokens_before": self.estimated_tokens_before,
            "estimated_tokens_after": self.estimated_tokens_after,
        }


def parse_react_compaction(value: object, *, source: str) -> ReactCompaction:
    if not isinstance(value, dict):
        raise ValueError(f"react_compaction 必须是 JSON object: {source}")
    raw = cast(dict[str, object], value)
    if raw.get("schema_version") != COMPACTION_SCHEMA_VERSION:
        raise ValueError(f"react_compaction schema_version 无效: {source}")
    summary = raw.get("summary")
    if not isinstance(summary, str) or not summary.strip():
        raise ValueError(f"react_compaction.summary 必须是非空字符串: {source}")
    if len(summary.encode("utf-8")) > 512 * 1024:
        raise ValueError(f"react_compaction.summary 超过 512 KiB: {source}")
    trigger = raw.get("trigger")
    if trigger not in {"soft_limit", "context_overflow"}:
        raise ValueError(f"react_compaction.trigger 无效: {source}")
    integers: dict[str, int] = {}
    for field in (
        "compacted_tool_groups",
        "generation",
        "context_window",
        "soft_limit_tokens",
        "estimated_tokens_before",
        "estimated_tokens_after",
    ):
        item = raw.get(field)
        if not isinstance(item, int) or isinstance(item, bool):
            raise ValueError(f"react_compaction.{field} 必须是整数: {source}")
        integers[field] = item
    if integers["compacted_tool_groups"] <= 0 or integers["generation"] <= 0:
        raise ValueError(f"react_compaction 计数必须大于 0: {source}")
    if (
        integers["context_window"] <= 0
        or integers["soft_limit_tokens"] <= 0
        or integers["soft_limit_tokens"] >= integers["context_window"]
    ):
        raise ValueError(f"react_compaction 模型预算无效: {source}")
    if integers["estimated_tokens_before"] < 0 or integers["estimated_tokens_after"] < 0:
        raise ValueError(f"react_compaction token 估算不能为负数: {source}")
    return ReactCompaction(
        summary=summary.strip(),
        compacted_tool_groups=integers["compacted_tool_groups"],
        generation=integers["generation"],
        trigger=cast(CompactionTrigger, trigger),
        context_window=integers["context_window"],
        soft_limit_tokens=integers["soft_limit_tokens"],
        estimated_tokens_before=integers["estimated_tokens_before"],
        estimated_tokens_after=integers["estimated_tokens_after"],
    )


def build_compaction_messages(
    compaction: ReactCompaction, *, call_id: str
) -> list[dict[str, Any]]:
    return [
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": call_id,
                    "type": "function",
                    "function": {
                        "name": COMPACTION_TOOL_NAME,
                        "arguments": json.dumps(
                            {
                                "scope": "current_user_query",
                                "compacted_tool_groups": compaction.compacted_tool_groups,
                                "generation": compaction.generation,
                            },
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    },
                }
            ],
        },
        {"role": "tool", "tool_call_id": call_id, "content": compaction.summary},
    ]


def build_replay_compaction_messages(
    compaction: ReactCompaction, *, message_id: str
) -> list[dict[str, Any]]:
    return build_compaction_messages(
        compaction,
        call_id=_compaction_call_id(f"persisted:{message_id}", compaction.generation),
    )


class QueryCompactor:
    """Compact only closed tool batches and retain at least the newest batch."""

    def __init__(
        self,
        *,
        base_messages: list[dict[str, Any]],
        context_window: int,
        soft_limit_tokens: int,
        hard_limit_tokens: int | None = None,
        scope_id: str,
        estimate: Callable[[list[dict[str, Any]]], int],
    ) -> None:
        self._base = deepcopy(base_messages)
        self._window = context_window
        self._hard = context_window if hard_limit_tokens is None else hard_limit_tokens
        if self._window < 0 or self._hard < 0 or self._hard > self._window:
            raise ValueError("hard_limit_tokens must be within [0, context_window]")
        self._soft = soft_limit_tokens
        self._scope_id = scope_id
        self._estimate = estimate
        self._batches: list[list[dict[str, Any]]] = []
        self._compaction: ReactCompaction | None = None

    def record_completed_batch(self, batch: list[dict[str, Any]]) -> None:
        if not batch or not any(
            item.get("role") == "assistant" and item.get("tool_calls")
            for item in batch
        ):
            raise ValueError("完整工具批次必须包含 assistant tool_calls")
        self._batches.append(deepcopy(batch))

    def persistence_payload(self) -> dict[str, object] | None:
        return self._compaction.to_payload() if self._compaction else None

    async def prepare(
        self,
        messages: list[dict[str, Any]],
        *,
        summarize: Callable[[str], Awaitable[str]],
        pending_start: int | None = None,
        trigger: CompactionTrigger = "soft_limit",
        force: bool = False,
    ) -> bool:
        before = self._estimate(messages)
        if not force and (self._soft <= 0 or before < self._soft):
            return False
        compact_count = self._select_count()
        if compact_count <= 0:
            if force:
                raise ContextCompactionError("context_compaction_no_closed_prefix")
            return False
        evicted = self._batches[:compact_count]
        retained = self._batches[compact_count:]
        pending = (
            []
            if pending_start is None
            else deepcopy(messages[max(0, min(pending_start, len(messages))) :])
        )
        prompt = self._summary_input(evicted)
        summary = (await summarize(prompt)).strip()
        if not summary:
            raise ContextCompactionError("context_compaction_summary_invalid")
        generation = 1 if self._compaction is None else self._compaction.generation + 1
        compacted = compact_count + (
            self._compaction.compacted_tool_groups if self._compaction else 0
        )
        candidate = ReactCompaction(
            summary=summary,
            compacted_tool_groups=compacted,
            generation=generation,
            trigger=trigger,
            context_window=self._window,
            soft_limit_tokens=self._soft,
            estimated_tokens_before=before,
            estimated_tokens_after=0,
        )
        rebuilt = [
            *deepcopy(self._base),
            *build_compaction_messages(
                candidate,
                call_id=_compaction_call_id(self._scope_id, generation),
            ),
            *[message for batch in deepcopy(retained) for message in batch],
            *pending,
        ]
        after = self._estimate(rebuilt)
        if after >= self._hard:
            raise ContextCompactionError(
                f"context_compaction_insufficient estimated={after} hard_limit={self._hard}"
            )
        self._compaction = ReactCompaction(
            **{**candidate.__dict__, "estimated_tokens_after": after}
        )
        self._batches = retained
        messages[:] = rebuilt
        return True

    def _select_count(self) -> int:
        if len(self._batches) < 2:
            return 0
        keep_budget = max(1, math.floor(self._hard * 0.20))
        kept = 0
        keep_count = 0
        for batch in reversed(self._batches):
            tokens = self._estimate(batch)
            if keep_count and kept + tokens > keep_budget:
                break
            keep_count += 1
            kept += tokens
        selected = min(
            len(self._batches) - 1,
            max(1, len(self._batches) - keep_count),
        )
        active_batch = _active_execution_batch(self._batches)
        return selected if active_batch is None else min(selected, active_batch)

    def _summary_input(self, evicted: list[list[dict[str, Any]]]) -> str:
        sections = [SUMMARY_PROMPT]
        if self._compaction is not None:
            sections.extend(["\n[Previous compaction summary]\n", self._compaction.summary])
        sections.append("\n[New completed steps]\n")
        for batch in evicted:
            for message in batch:
                clean = {k: v for k, v in message.items() if k != "reasoning_content"}
                content = str(clean.get("content") or "")
                if len(content) > 2000:
                    clean["content"] = content[:1000] + "\n…omitted…\n" + content[-1000:]
                sections.append("\n" + json.dumps(clean, ensure_ascii=False, separators=(",", ":")))
        return "".join(sections)


def _compaction_call_id(scope_id: str, generation: int) -> str:
    digest = hashlib.sha256(f"{scope_id}\0{generation}".encode()).hexdigest()
    return f"cmp_{digest[:24]}"


def _active_execution_batch(batches: list[list[dict[str, Any]]]) -> int | None:
    call_batches: dict[str, int] = {}
    messages: list[dict[str, Any]] = []
    for batch_index, batch in enumerate(batches):
        messages.extend(batch)
        for message in batch:
            calls = message.get("tool_calls")
            if message.get("role") != "assistant" or not isinstance(calls, list):
                continue
            for call in calls:
                if isinstance(call, dict) and call.get("id"):
                    call_batches[str(call["id"])] = batch_index
    active = active_shell_execution_origins(messages)
    pinned = [call_batches[call_id] for call_id in active.values() if call_id in call_batches]
    return min(pinned) if pinned else None
