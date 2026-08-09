"""Decorator metadata for concise plugin lifecycle and tool declarations."""

from __future__ import annotations

from dataclasses import dataclass
import inspect
from typing import Any, Callable, Dict, List, Optional


@dataclass(frozen=True)
class PluginBinding:
    kind: str
    phase: str = ""
    priority: int = 0
    tool_name: str = ""
    tool_description: str = ""
    tool_schema: Optional[Dict[str, Any]] = None
    deferred: bool = False
    hook_tool_name: Optional[str] = None


def _bind(function: Callable[..., Any], binding: PluginBinding):
    bindings = list(getattr(function, "_kirakira_bindings", []))
    bindings.append(binding)
    setattr(function, "_kirakira_bindings", bindings)
    return function


def _phase_decorator(phase: str):
    def factory(*, priority: int = 0):
        def decorate(function):
            return _bind(
                function,
                PluginBinding(kind="phase", phase=phase, priority=int(priority)),
            )

        return decorate

    return factory


on_before_turn = _phase_decorator("before_turn")
on_before_reasoning = _phase_decorator("before_reasoning")
on_prompt_render = _phase_decorator("prompt_render")
on_before_step = _phase_decorator("before_step")
on_after_step = _phase_decorator("after_step")
on_after_reasoning = _phase_decorator("after_reasoning")
on_after_turn = _phase_decorator("after_turn")


def on_tool_pre(*, tool_name: Optional[str] = None, priority: int = 0):
    def decorate(function):
        return _bind(
            function,
            PluginBinding(
                kind="tool_hook",
                priority=int(priority),
                hook_tool_name=tool_name,
            ),
        )

    return decorate


def tool(
    name: str,
    *,
    description: str = "",
    parameters: Optional[Dict[str, Any]] = None,
    always_on: bool = False,
):
    def decorate(function):
        schema = parameters or _derive_schema(function)
        return _bind(
            function,
            PluginBinding(
                kind="tool",
                tool_name=name,
                tool_description=description or inspect.getdoc(function) or name,
                tool_schema=schema,
                deferred=not always_on,
            ),
        )

    return decorate


def get_bindings(method: object) -> List[PluginBinding]:
    return list(getattr(method, "_kirakira_bindings", []))


def _derive_schema(function: Callable[..., Any]) -> Dict[str, Any]:
    signature = inspect.signature(function)
    properties: Dict[str, Any] = {}
    required = []
    type_names = {str: "string", int: "integer", float: "number", bool: "boolean", list: "array", dict: "object"}
    for name, parameter in signature.parameters.items():
        if name in ("self", "event"):
            continue
        annotation = parameter.annotation
        json_type = type_names.get(annotation, "string")
        properties[name] = {"type": json_type}
        if parameter.default is inspect.Parameter.empty:
            required.append(name)
    return {"type": "object", "properties": properties, "required": required}
