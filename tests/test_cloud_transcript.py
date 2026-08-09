"""Cloud transcript parity tests against the original passive Agent pipeline."""

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

import pytest

from agent.pipeline_factory import build_passive_pipeline
from agent.turns.models import (
    AgentPrincipal,
    TurnMemoryScope,
    TurnOrigin,
    TurnRequest,
    TurnResult,
)
from agent.tools import build_default_registry
from bus.event_bus import EventBus
from bus.queue import MessageBus
from cloud.executor import (
    CLOUD_TRANSCRIPT_COMMIT_KEY,
    CLOUD_TRANSCRIPT_KEY,
    CloudPipelineExecutor,
)
from cloud.transcript import RunScopedTranscriptStore
from core.memory.legacy import MemoryRuntime
from core.schema import ModelResponse
from session.manager import SessionManager


class RecordingModel:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def complete(self, messages, tools, system, model, max_tokens):
        self.calls.append({"messages": list(messages), "tools": list(tools)})
        return ModelResponse(text="cloud answer", reasoning_content="kept reasoning")


@pytest.mark.asyncio
async def test_cloud_executor_runs_original_pipeline_with_durable_history(tmp_path):
    transcripts = RunScopedTranscriptStore()
    local_sessions = SessionManager(tmp_path)
    memory = MemoryRuntime(tmp_path, session_manager=local_sessions)
    tools = build_default_registry(
        tmp_path,
        memory=memory,
        session_manager=transcripts,
    )
    model = RecordingModel()
    event_bus = EventBus()
    assembly = build_passive_pipeline(
        workspace=tmp_path,
        app_config={
            "llm": {"main": {"context_window": 128_000}},
            "agent": {
                "max_iterations": 3,
                "max_tokens": 1000,
                "context": {"memory_window": 20},
            },
        },
        model="fake",
        model_client=model,
        transcript_store=transcripts,
        memory=memory,
        memory_services=None,
        tools=tools,
        bus=MessageBus(),
        event_bus=event_bus,
    )
    executor = CloudPipelineExecutor(assembly.pipeline, transcripts)
    conversation_id = str(uuid4())
    request = TurnRequest(
        conversation_id=conversation_id,
        content="second question",
        principal=AgentPrincipal("user-1"),
        origin=TurnOrigin("api", "cloud", "run-2"),
        memory_scope=TurnMemoryScope("user", "user-1"),
        metadata={
            CLOUD_TRANSCRIPT_KEY: {
                "created_at": "2026-08-09T00:00:00+00:00",
                "updated_at": "2026-08-09T00:01:00+00:00",
                "metadata": {"turn_count": 1},
                "last_consolidated": 0,
                "messages": [
                    {
                        "id": str(uuid4()),
                        "seq": 1,
                        "role": "user",
                        "content": "first question",
                        "created_at": "2026-08-09T00:00:00+00:00",
                        "metadata": {},
                    },
                    {
                        "id": str(uuid4()),
                        "seq": 2,
                        "role": "assistant",
                        "content": "first answer",
                        "created_at": "2026-08-09T00:01:00+00:00",
                        "metadata": {"tools_used": [], "tool_chain": []},
                    },
                ],
            }
        },
    )
    try:
        result = await executor.execute(request)
    finally:
        await tools.shutdown()
        local_sessions.close()

    prompt_messages = model.calls[0]["messages"]
    prompt_text = "\n".join(str(item.get("content") or "") for item in prompt_messages)
    assert "first question" in prompt_text
    assert "first answer" in prompt_text
    assert "second question" in prompt_text
    commit = result.metadata[CLOUD_TRANSCRIPT_COMMIT_KEY]
    UUID(commit["assistant_message_id"])
    assert commit["assistant_metadata"]["reasoning_content"] == "kept reasoning"
    assert commit["conversation_metadata"]["turn_count"] == 2
    assert not transcripts.session_exists(conversation_id)


@pytest.mark.asyncio
async def test_transcript_bindings_are_task_local_and_fail_closed():
    transcripts = RunScopedTranscriptStore()

    async def use(conversation_id: str, marker: str):
        payload = {
            "metadata": {},
            "messages": [],
        }
        with transcripts.bind(conversation_id, payload):
            session = transcripts.get_or_create(conversation_id)
            session.add_message("assistant", marker)
            await transcripts.save_async(session)
            await asyncio.sleep(0)
            return transcripts.get_or_create(conversation_id).messages[0]["content"]

    assert await asyncio.gather(use("conversation-a", "a"), use("conversation-b", "b")) == [
        "a",
        "b",
    ]
    with pytest.raises(RuntimeError, match="active Run binding"):
        transcripts.get_or_create("conversation-a")


@pytest.mark.asyncio
async def test_cloud_executor_persists_legitimate_pipeline_early_return():
    transcripts = RunScopedTranscriptStore()

    class EarlyReturnPipeline:
        async def execute(self, request):
            return TurnResult(
                request.conversation_id,
                "built-in command result",
                media=("artifact-ref",),
            )

    executor = CloudPipelineExecutor(EarlyReturnPipeline(), transcripts)
    request = TurnRequest(
        conversation_id="conversation-command",
        content="/help",
        principal=AgentPrincipal("user-1"),
        origin=TurnOrigin("api", "cloud", "run-command"),
        memory_scope=TurnMemoryScope("user", "user-1"),
        metadata={
            CLOUD_TRANSCRIPT_KEY: {
                "metadata": {},
                "messages": [],
            }
        },
    )

    result = await executor.execute(request)

    commit = result.metadata[CLOUD_TRANSCRIPT_COMMIT_KEY]
    UUID(commit["assistant_message_id"])
    assert commit["assistant_metadata"]["media"] == ["artifact-ref"]
