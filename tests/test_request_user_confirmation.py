"""request_user_confirmation 标记工具与 mobile_attention 的传播规则。"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from bootstrap.control import build_turn_executor
from agent.control.models import TurnItemKind, TurnRequest
from agent.turns.models import TurnResult
from core.schema import ToolCall, ToolResult
from agent.tool_hooks import ToolExecutionRequest, ToolExecutor
from agent.tools.builtins import build_default_registry


@pytest.fixture
def registry():
    with tempfile.TemporaryDirectory() as raw:
        yield build_default_registry(Path(raw))


@pytest.mark.asyncio
async def test_tool_declares_confirmation_attention(registry):
    result = await registry.execute_async(
        ToolCall("c1", "request_user_confirmation", {"prompt": "删除生产库?"})
    )
    assert result.is_error is False
    assert result.mobile_attention == "confirmation"
    assert "删除生产库?" in result.content


@pytest.mark.asyncio
async def test_blank_and_oversized_prompt_are_rejected_without_attention(registry):
    blank = await registry.execute_async(
        ToolCall("c1", "request_user_confirmation", {"prompt": "   "})
    )
    assert blank.is_error and blank.mobile_attention is None
    huge = await registry.execute_async(
        ToolCall("c2", "request_user_confirmation", {"prompt": "x" * 501})
    )
    assert huge.is_error and huge.mobile_attention is None


@pytest.mark.asyncio
async def test_ordinary_tools_do_not_set_attention(registry):
    result = await registry.execute_async(ToolCall("c1", "list_dir", {"path": "."}))
    assert result.mobile_attention is None


@pytest.mark.asyncio
async def test_executor_carries_attention_only_from_tool_result():
    executor = ToolExecutor()
    request = ToolExecutionRequest(
        session_key="s",
        channel="control",
        chat_id="c",
        tool_name="request_user_confirmation",
        arguments={},
        call_id="1",
        request_text="",
    )

    async def invoke(name, args):
        return ToolResult("1", "ok", mobile_attention="confirmation")

    result = await executor.execute(request, invoke)
    assert result.status == "success" and result.mobile_attention == "confirmation"

    async def plain(name, args):
        return "just text"

    assert (await executor.execute(request, plain)).mobile_attention is None


@pytest.mark.asyncio
async def test_failed_tool_declaring_attention_keeps_error_status():
    """失败的工具即使声明了标记,也不能以 success 身份把它带出去。"""
    executor = ToolExecutor()
    request = ToolExecutionRequest(
        session_key="s",
        channel="control",
        chat_id="c",
        tool_name="request_user_confirmation",
        arguments={},
        call_id="1",
        request_text="",
    )

    async def failing(name, args):
        return ToolResult("1", "Error: nope", is_error=True, mobile_attention="confirmation")

    result = await executor.execute(request, failing)
    assert result.status == "error"
    # runtime 在聚合处对 status != success 且带标记的情况 fail loud;
    # 这里确认 status 确实是 error,那条规则才有意义。
    assert result.mobile_attention == "confirmation"


class _FakePipeline:
    def __init__(self, outbound: TurnResult) -> None:
        self._outbound = outbound
        self.seen_metadata: dict = {}
        self.seen_request = None

    async def execute(self, request):
        self.seen_request = request
        self.seen_metadata = dict(request.metadata)
        return self._outbound


@pytest.mark.asyncio
async def test_binding_projects_attention_and_tool_calls():
    outbound = TurnResult(
        conversation_id="programmatic:x",
        content="done",
        thinking="思考过程",
        metadata={
            "mobile_attention": "confirmation",
            "tool_chain": [
                {
                    "calls": [
                        {
                            "name": "list_dir",
                            "arguments": {"path": "."},
                            "result": "a\nb",
                            "status": "success",
                        }
                    ]
                }
            ],
        },
    )
    pipeline = _FakePipeline(outbound)
    execute = build_turn_executor(pipeline)
    result = await execute(TurnRequest("programmatic:x", "hi", {"_controlItemEvent": object()}))

    assert result.response == "done"
    assert result.assistant_data["mobileAttention"] == "confirmation"
    kinds = [item.kind for item in result.items]
    assert TurnItemKind.TOOL_CALL in kinds and TurnItemKind.REASONING in kinds
    tool_item = next(i for i in result.items if i.kind is TurnItemKind.TOOL_CALL)
    assert tool_item.data["name"] == "list_dir"
    # 下划线开头的内部回调不能混进模型可见的 metadata
    assert "_controlItemEvent" not in pipeline.seen_metadata
    assert "session_key_override" not in pipeline.seen_metadata
    assert pipeline.seen_request.conversation_id == "programmatic:x"
    assert pipeline.seen_request.principal.kind == "service"
    assert pipeline.seen_request.origin.kind == "control"


# --- runtime 端到端:标记从工具一路走到 outbound metadata ---


@pytest.mark.asyncio
async def test_attention_reaches_outbound_metadata_through_pipeline():
    """模型调用确认工具后,outbound.metadata 必须带上 mobile_attention。"""
    import tempfile as _tempfile

    from bus.events import InboundMessage
    from core.schema import ModelResponse, ToolCall as _ToolCall
    from tests.test_runtime import build_test_runtime

    class _Model:
        def __init__(self):
            self.responses = [
                ModelResponse(
                    text="",
                    tool_calls=[
                        _ToolCall("1", "request_user_confirmation", {"prompt": "要删库吗?"})
                    ],
                ),
                ModelResponse(text="请确认后我再动手。"),
            ]

        def complete(self, messages, tools, system, model, max_tokens):
            return self.responses.pop(0)

    with _tempfile.TemporaryDirectory() as raw:
        workdir = Path(raw)
        _bus, loop, sessions, memory = build_test_runtime(workdir, _Model())
        outbound = await loop.pipeline.run(
            InboundMessage(channel="web", sender="u", chat_id="1", content="删库"),
            "web:1",
            dispatch_outbound=False,
        )
        assert outbound.metadata["mobile_attention"] == "confirmation"
        assert "request_user_confirmation" in outbound.metadata["tools_used"]
        await memory.shutdown()
        sessions.close()


@pytest.mark.asyncio
async def test_inbound_cannot_forge_attention():
    """入站 metadata 里伪造的标记必须被丢弃,只认本轮工具声明的。"""
    import tempfile as _tempfile

    from bus.events import InboundMessage
    from core.schema import ModelResponse
    from tests.test_runtime import build_test_runtime

    class _Model:
        def complete(self, messages, tools, system, model, max_tokens):
            return ModelResponse(text="没调任何工具。")

    with _tempfile.TemporaryDirectory() as raw:
        workdir = Path(raw)
        _bus, loop, sessions, memory = build_test_runtime(workdir, _Model())
        outbound = await loop.pipeline.run(
            InboundMessage(
                channel="web",
                sender="u",
                chat_id="1",
                content="hi",
                metadata={"mobile_attention": "confirmation"},
            ),
            "web:1",
            dispatch_outbound=False,
        )
        assert "mobile_attention" not in outbound.metadata
        await memory.shutdown()
        sessions.close()
