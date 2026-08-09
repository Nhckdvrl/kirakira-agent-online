"""Pure presentation state shared by the full-screen and plain CLIs."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
from typing import Any
from uuid import uuid4

from bus.events_lifecycle import (
    ContextBudgetUpdated,
    ContextPrepared,
    StreamDeltaReady,
    ToolCallCompleted,
    ToolCallStarted,
    TurnFinished,
    TurnStarted,
)


def new_local_session_id() -> str:
    """Return a readable, collision-resistant ID for a fresh local chat."""

    stamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    return "chat-%s-%s" % (stamp, uuid4().hex[:6])


@dataclass
class StepView:
    iteration: int
    content: str = ""
    reasoning: str = ""
    tool_call_ids: list[str] = field(default_factory=list)


@dataclass
class ToolView:
    call_id: str
    name: str
    iteration: int
    arguments: dict[str, Any] = field(default_factory=dict)
    status: str = "running"
    result: str = ""


@dataclass
class TurnViewState:
    """Reduce lifecycle events into one authoritative terminal turn."""

    session_key: str = "cli:local"
    active: bool = False
    status: str = "idle"
    user_content: str = ""
    steps: dict[int, StepView] = field(default_factory=dict)
    tools: dict[str, ToolView] = field(default_factory=dict)
    tool_order: list[str] = field(default_factory=list)
    final_content: str = ""
    final_thinking: str = ""
    duration_seconds: float = 0.0
    error: str = ""
    context_plan: str = ""
    context_estimated_tokens: int = 0
    context_input_budget: int = 0
    context_history_messages: int = 0
    context_sections: list[dict[str, Any]] = field(default_factory=list)
    next_history_tokens: int = 0
    model_usage: dict[str, Any] = field(default_factory=dict)

    def reset(self, content: str = "") -> None:
        self.active = True
        self.status = "running"
        self.user_content = content
        self.steps.clear()
        self.tools.clear()
        self.tool_order.clear()
        self.final_content = ""
        self.final_thinking = ""
        self.duration_seconds = 0.0
        self.error = ""
        self.context_plan = ""
        self.context_estimated_tokens = 0
        self.context_input_budget = 0
        self.context_history_messages = 0
        self.context_sections.clear()
        self.next_history_tokens = 0
        self.model_usage.clear()

    def apply(self, event: object) -> bool:
        if getattr(event, "session_key", self.session_key) != self.session_key:
            return False
        if isinstance(event, TurnStarted):
            self.reset(event.content)
            return True
        if isinstance(event, StreamDeltaReady):
            step = self.steps.setdefault(event.iteration, StepView(event.iteration))
            step.content += event.content_delta
            step.reasoning += event.reasoning_delta
            return True
        if isinstance(event, ContextPrepared):
            self.context_plan = event.plan_name
            self.context_estimated_tokens = event.estimated_tokens
            self.context_input_budget = event.input_budget
            self.context_history_messages = event.history_messages
            self.context_sections = [dict(item) for item in event.sections]
            return True
        if isinstance(event, ContextBudgetUpdated):
            self.next_history_tokens = event.history_tokens_estimate
            self.model_usage = dict(event.model_usage)
            return True
        if isinstance(event, ToolCallStarted):
            iteration = int(event.iteration)
            step = self.steps.setdefault(iteration, StepView(iteration))
            if event.call_id not in step.tool_call_ids:
                step.tool_call_ids.append(event.call_id)
            self.tools[event.call_id] = ToolView(
                call_id=event.call_id,
                name=event.tool_name,
                iteration=iteration,
                arguments=dict(event.arguments),
            )
            if event.call_id not in self.tool_order:
                self.tool_order.append(event.call_id)
            return True
        if isinstance(event, ToolCallCompleted):
            tool = self.tools.get(event.call_id)
            if tool is None:
                tool = ToolView(
                    call_id=event.call_id,
                    name=event.tool_name,
                    iteration=int(event.iteration),
                    arguments=dict(event.arguments),
                )
                self.tools[event.call_id] = tool
                self.tool_order.append(event.call_id)
            tool.arguments = dict(event.arguments)
            tool.status = event.status
            tool.result = event.result
            return True
        if isinstance(event, TurnFinished):
            self.active = False
            self.status = event.status
            self.duration_seconds = event.duration_seconds
            self.error = event.error
            if event.outbound is not None:
                # This replaces the draft. It must never be appended to it.
                self.final_content = event.outbound.content
                self.final_thinking = event.outbound.thinking
            return True
        return False

    @property
    def latest_iteration(self) -> int:
        return max(self.steps, default=0)

    @property
    def draft_answer(self) -> str:
        if self.final_content:
            return self.final_content
        step = self.steps.get(self.latest_iteration)
        if step is None or step.tool_call_ids:
            return ""
        return step.content

    @property
    def latest_streamed_content(self) -> str:
        step = self.steps.get(self.latest_iteration)
        return step.content if step is not None else ""

    def process_text(self, *, result_limit: int = 240) -> str:
        rows: list[str] = []
        if self.context_plan:
            budget = (
                "/%s" % self._compact_number(self.context_input_budget)
                if self.context_input_budget
                else ""
            )
            rows.append(
                "  context   %s · %s%s tokens · %d history"
                % (
                    self.context_plan,
                    self._compact_number(self.context_estimated_tokens),
                    budget,
                    self.context_history_messages,
                )
            )
        latest = self.latest_iteration
        for iteration in sorted(self.steps):
            step = self.steps[iteration]
            if step.reasoning.strip():
                rows.append("  thinking  " + self._one_line(step.reasoning, 360))
            if step.content.strip() and (step.tool_call_ids or iteration != latest):
                rows.append("  note      " + self._one_line(step.content, 360))
            for call_id in step.tool_call_ids:
                tool = self.tools.get(call_id)
                if tool is None:
                    continue
                icon = {
                    "running": "◇",
                    "success": "✓",
                    "error": "✗",
                    "denied": "⊘",
                }.get(tool.status, "•")
                args = self._arguments_summary(tool.arguments)
                rows.append("  %s %-16s %s" % (icon, tool.name, args))
                if tool.status in ("error", "denied") and tool.result.strip():
                    rows.append("    " + self._one_line(tool.result, result_limit))
        if self.status == "error" and self.error:
            rows.append("  ✗ " + self._one_line(self.error, 400))
        return "\n".join(rows)

    @staticmethod
    def _compact_number(value: int) -> str:
        if value >= 1_000_000:
            return "%.1fm" % (value / 1_000_000)
        if value >= 1_000:
            return "%.1fk" % (value / 1_000)
        return str(value)

    @staticmethod
    def _one_line(value: str, limit: int) -> str:
        text = " ".join(value.split())
        return text if len(text) <= limit else text[: limit - 1] + "…"

    @classmethod
    def _arguments_summary(cls, arguments: dict[str, Any]) -> str:
        if not arguments:
            return ""
        for key in ("query", "url", "path", "command", "description"):
            value = arguments.get(key)
            if isinstance(value, str) and value.strip():
                return cls._one_line(value, 120)
        return cls._one_line(
            json.dumps(arguments, ensure_ascii=False, sort_keys=True, default=str),
            120,
        )
