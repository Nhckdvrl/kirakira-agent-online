"""Kirakira Agent learning harness module."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


JsonDict = Dict[str, Any]


@dataclass
class ToolSpec:
    name: str
    description: str
    input_schema: JsonDict


@dataclass
class ToolCall:
    id: str
    name: str
    arguments: JsonDict = field(default_factory=dict)


@dataclass
class ToolResult:
    tool_call_id: str
    content: str
    is_error: bool = False
    # 工具声明"本轮需要用户明确确认"。唯一合法值是 "confirmation"(照 Reference
    # agent/tools/base.py)。只有成功的工具可以声明,由 reasoner 聚合到 turn 级。
    mobile_attention: Optional[str] = None


@dataclass
class ModelResponse:
    text: str = ""
    reasoning_content: str = ""
    tool_calls: List[ToolCall] = field(default_factory=list)
    stop_reason: str = "end_turn"
    raw: Optional[JsonDict] = None
    usage: JsonDict = field(default_factory=dict)

    @property
    def has_tool_calls(self) -> bool:
        return bool(self.tool_calls)


def assistant_message_from_response(response: ModelResponse) -> JsonDict:
    message: JsonDict = {"role": "assistant", "content": response.text or ""}
    if response.reasoning_content:
        message["reasoning_content"] = response.reasoning_content
    if response.tool_calls:
        message["tool_calls"] = [
            {
                "id": call.id,
                "type": "function",
                "function": {
                    "name": call.name,
                    "arguments": call.arguments,
                },
            }
            for call in response.tool_calls
        ]
    return message


def tool_result_message(result: ToolResult) -> JsonDict:
    return {
        "role": "tool",
        "tool_call_id": result.tool_call_id,
        "content": result.content,
        "is_error": result.is_error,
    }
